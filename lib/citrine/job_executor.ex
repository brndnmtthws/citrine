defmodule Citrine.JobExecutor do
  @moduledoc false
  use GenServer
  require Logger
  alias Citrine.Job

  # 1h in milliseconds
  @max_delay_ms 3600_000
  @epsilon 300_000

  def start_link(_opts, registry, job) do
    case GenServer.start_link(__MODULE__, {registry, job},
           name: {:via, Citrine.Registry, {registry, job.id}}
         ) do
      {:ok, pid} ->
        Logger.debug(fn -> "new citrine job added, id=#{job.id} node=#{Node.self()}" end)
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        Logger.debug(fn ->
          "job id=#{job.id} already started at #{inspect(pid)}, updating existing job state on node=#{
            Node.self()
          }"
        end)

        # instruct the job executor to update its state, update the job in registry
        send(pid, {:update_job, updated_job: job})

        {:ok, pid}

      other ->
        {:error, other}
    end
  end

  @impl true
  def terminate(reason, state) do
    Logger.debug(fn ->
      "terminating #{inspect(__MODULE__)} with reason: #{inspect(reason)} and state=#{
        inspect(state)
      }"
    end)

    :ok
  end

  @impl true
  def init(init_arg) do
    {registry, %Job{} = job} = init_arg
    Logger.debug(fn -> "initializing citrine job, id=#{job.id} node=#{Node.self()}" end)

    Process.flag(:trap_exit, true)

    cron_expr = Crontab.CronExpression.Parser.parse!(job.schedule, job.extended_syntax)

    timer = schedule_iteration(cron_expr)

    {:ok,
     %{
       job: job,
       cron_expr: cron_expr,
       timer: timer,
       registry: registry
     }}
  end

  def child_spec({registry, %Job{} = job}) do
    %{
      id: job.id,
      start: {__MODULE__, :start_link, [registry, job]},
      # Allow for up to 60 seconds to shut down
      shutdown: 60_000,
      # Restart always
      restart: :permanent,
      type: :worker
    }
  end

  defp run_task({mod, fun, args}) do
    :erlang.apply(mod, fun, args)
  end

  defp run_task({fun, args}) when is_function(fun) do
    :erlang.apply(fun, args)
  end

  defp run_task(task) when is_function(task) do
    task.()
  end

  defp measure_fun(fun) do
    fn -> fun.() end
    |> :timer.tc()
    |> elem(0)
    |> Kernel./(1_000_000)
  end

  @impl true
  def handle_info(
        :run_task,
        %{
          job: %Job{} = job,
          cron_expr: %Crontab.CronExpression{} = cron_expr,
          timer: timer
        } = state
      ) do
    Logger.debug(fn -> "running job id=#{job.id} node=#{Node.self()}" end)

    seconds =
      measure_fun(fn ->
        try do
          run_task(job.task)
        rescue
          err ->
            Logger.error(fn -> "error in job id=#{job.id} on node=#{Node.self()}" end)
            Logger.error(fn -> Exception.format(:error, err, __STACKTRACE__) end)
        end
      end)

    Logger.debug(fn -> "finished job id=#{job.id} in #{seconds}s" end)

    Process.cancel_timer(timer)
    timer = schedule_iteration(cron_expr)

    {:noreply, Map.put(state, :timer, timer)}
  end

  @impl true
  def handle_info({:EXIT, _pid, :shutdown}, state) do
    {:stop, :normal, state}
  end

  @impl true
  def handle_info(
        :reschedule,
        %{
          timer: timer,
          job: %Job{} = job,
          cron_expr: %Crontab.CronExpression{} = cron_expr
        } = state
      ) do
    Logger.debug(fn -> "rescheduling job id=#{job.id} on node=#{Node.self()}" end)

    Process.cancel_timer(timer)
    timer = schedule_iteration(cron_expr)

    {:noreply, Map.put(state, :timer, timer)}
  end

  @impl true
  def handle_info(
        {:update_job, updated_job: %Job{} = updated_job},
        %{
          registry: registry,
          timer: timer,
          job: %Job{} = job
        } = state
      ) do
    Logger.debug(fn -> "updating job state for id=#{job.id} on node=#{Node.self()}" end)

    if updated_job != job do
      # check the job id matches before proceeding
      if updated_job.id == job.id do
        # check if schedule changed, and if so, cancel the existing timer and start a new one
        if updated_job.schedule != job.schedule or
             updated_job.extended_syntax != job.extended_syntax do
          # check if the job schedule is valid before continuing
          case Crontab.CronExpression.Parser.parse(
                 updated_job.schedule,
                 updated_job.extended_syntax
               ) do
            {:ok, updated_cron_expr} ->
              :ok =
                Citrine.Registry.register_job(
                  registry,
                  updated_job
                )

              Logger.debug(fn ->
                "using new schedule='#{updated_job.schedule}' for id=#{job.id} on node=#{
                  Node.self()
                }"
              end)

              Process.cancel_timer(timer)
              timer = schedule_iteration(updated_cron_expr)

              {:noreply,
               Map.merge(
                 state,
                 %{
                   job: updated_job,
                   cron_expr: updated_cron_expr,
                   timer: timer
                 }
               )}

            _ ->
              # invalid cron expression, no change
              Logger.error(fn ->
                "invalid cron expression for job id=#{job.id} schedule='#{job.schedule}', update ignored"
              end)

              {:noreply, state}
          end
        else
          # the schedule hasn't changed, just update job state
          :ok = Citrine.Registry.register_job(registry, updated_job)

          Logger.debug(fn ->
            "job id=#{job.id} updated but schedule unchanged on node=#{Node.self()}"
          end)

          {:noreply, Map.put(state, :job, updated_job)}
        end
      else
        # job IDs didn't match, do nothing
        Logger.error(fn -> "unexpected job id, got id=#{updated_job.id} expected id=#{job.id}" end)

        {:noreply, state}
      end
    else
      # job unchanged
      {:noreply, state}
    end
  end

  defp schedule_iteration(%Crontab.CronExpression{} = cron_expr) do
    diff_ms =
      Crontab.Scheduler.get_next_run_date!(cron_expr)
      |> DateTime.from_naive!("Etc/UTC")
      |> DateTime.diff(DateTime.utc_now(), :millisecond)
      |> max(1)

    # reschedule job every 1h, within 60s, to account for some clock skew
    cond do
      diff_ms <= 0 ->
        Process.send_after(self(), :run_task, 1)

      diff_ms >= 0 and diff_ms <= @max_delay_ms + @epsilon ->
        # if it's less than 1h, next iteration should just run the job
        Process.send_after(self(), :run_task, diff_ms)

      true ->
        # if it's more than @max_delay_ms, next iteration will reschedule the job
        Process.send_after(self(), :reschedule, @max_delay_ms)
    end
  end
end

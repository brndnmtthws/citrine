defmodule Citrine.Scheduler do
  @moduledoc """
  The interface for the core Citrine scheduler. To use the scheduler, define
  a module that uses `Citrine.Scheduler`.

  Citrine uses Erlang's [`:mnesia`](https://erlang.org/doc/man/mnesia.html)
  module for state management. Tables are stored in-memory as RAM copies,
  thus they are not persisted to disk.

  ## Usage

  Define a scheduler based on `Citrine.Scheduler`:

      defmodule Blogmail.Scheduler do
        use Citrine.Scheduler, otp_app: :blogmail
        def initialize_jobs() do
          # Initialize your jobs here
          put_job(%Citrine.Job{
            id: "hourly",
            schedule: "0 * * * *",
            task: fn -> nil end
          })
        end
      end

  Citrine should be added to your application's supervisor tree:

      defmodule MyApp.Application do
        use Application
        def start(_type, _args) do
          children = [
            # Start the Citrine scheduler
            MyApp.Scheduler
          ]
          opts = [strategy: :one_for_one, name: MyApp.Supervisor]
          Supervisor.start_link(children, opts)
        end
      end

  Additionally, it's recommended that you provide an `init_task` to perform
  initialization at startup. You should also specify a sufficient
  `init_task_delay` to allow everything to settle before running the init task.

  For example, you could specify the following in `config.exs`:

      config :myapp, MyApp.Scheduler,
        init_task_delay: 30_000,
        init_task: {MyApp.Scheduler, :initialize_jobs, []}

  ## Example

      iex(1)> defmodule MyApp.Scheduler do
      ...(1)>   use Citrine.Scheduler, otp_app: :myapp
      ...(1)> end
      iex(2)> MyApp.Scheduler.start_link()
      iex(3)> MyApp.Scheduler.put_job(
      ...(3)>   %Citrine.Job{
      ...(3)>    id: "job",
      ...(3)>    schedule: "* * * * *",
      ...(3)>    task: fn -> nil end
      ...(3)>  })
  """

  alias Citrine.Job
  use Supervisor

  @opaque t :: module

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts, moduledoc: @moduledoc] do
      require Logger

      @otp_app Keyword.fetch!(opts, :otp_app)
      @default_opts [
        name: __MODULE__,
        strategy: :one_for_one
      ]

      def start_link(opts \\ []) do
        opts =
          @default_opts
          |> Keyword.merge(Application.get_env(@otp_app, __MODULE__, []))
          |> Keyword.merge(opts)
          |> Keyword.put(:registry_name, registry_name())
          |> Keyword.put(:supervisor_name, supervisor_name())

        init_task = Keyword.get(opts, :init_task)
        init_task_delay = Keyword.get(opts, :init_task_delay, 0)

        children = [
          %{
            id: registry_name(),
            start:
              {Citrine.Registry, :start_link,
               [
                 [
                   name: registry_name()
                 ]
               ]}
          },
          %{
            id: supervisor_name(),
            start: {Citrine.Supervisor, :start_link, [[name: supervisor_name()]]}
          },
          %{
            id: monitor_name(),
            start:
              {Citrine.Monitor, :start_link,
               [[supervisor_name: supervisor_name(), registry_name: registry_name()]]}
          },
          {Task,
           fn ->
             if init_task do
               if init_task_delay > 0 do
                 Logger.debug("waiting #{init_task_delay / 1_000}s before running init task")
                 Process.sleep(init_task_delay)
               end

               try do
                 Logger.debug("starting init task")

                 case init_task do
                   {mod, fun, args} ->
                     :erlang.apply(mod, fun, args)

                   {fun, args} ->
                     :erlang.apply(fun, args)

                   fun when is_function(fun) ->
                     fun.()

                   _ ->
                     Logger.warn(fn ->
                       "unexpect value for init_task, skipping (got init_task=#{
                         inspect(init_task)
                       }"
                     end)
                 end

                 Logger.debug("finished init task")
               rescue
                 err ->
                   Logger.error("error during initialization")
                   Logger.error(Exception.format(:error, err, __STACKTRACE__))
               catch
                 :exit, {:timeout, _value} ->
                   Logger.error("error during initialization: timeout waiting for quorum")
               end
             end
           end}
        ]

        Supervisor.start_link(children, opts)
      end

      defp monitor_name() do
        String.to_atom("#{__MODULE__}.Monitor")
      end

      defp registry_name() do
        String.to_atom("#{__MODULE__}.Registry")
      end

      defp supervisor_name() do
        String.to_atom("#{__MODULE__}.Supervisor")
      end

      def put_job(%Citrine.Job{} = job) do
        Logger.debug(fn ->
          "adding/updating citrine job: #{inspect(job)}"
        end)

        # verify schedule parses
        {:ok, _} = Crontab.CronExpression.Parser.parse(job.schedule, job.extended_syntax)

        Citrine.Registry.register_job(registry_name(), job)

        Citrine.Supervisor.start_child(
          supervisor_name(),
          {Citrine.JobExecutor, {registry_name(), job}}
        )
      end

      def delete_job(%Citrine.Job{} = job) do
        delete_job(job.id)
      end

      def delete_job(job_id) do
        Logger.debug(fn ->
          "deleting citrine job on supervisor id=#{job_id}"
        end)

        case Citrine.Registry.whereis_name(registry_name(), job_id) do
          :undefined ->
            Citrine.Registry.unregister_job(registry_name(), job_id)

          pid ->
            :ok = Citrine.Supervisor.terminate_child(supervisor_name(), pid)
            Citrine.Registry.unregister_job(registry_name(), job_id)
        end
      end

      def get_job(%Citrine.Job{} = job) do
        get_job(job.id)
      end

      def get_job(job_id) do
        case Citrine.Registry.lookup_job(registry_name(), job_id) do
          nil -> nil
          job -> {Citrine.Registry.whereis_name(registry_name(), job_id), job}
        end
      end

      def list_jobs() do
        Citrine.Registry.list_jobs(registry_name())
      end

      def count_local_jobs() do
        Citrine.Supervisor.active_children(supervisor_name())
      end

      spec = [
        id: __MODULE__,
        start: quote(do: {__MODULE__, :start_link, [opts]}),
        type: :supervisor
      ]

      def child_spec(opts) do
        %{unquote_splicing(spec)}
      end

      defoverridable child_spec: 1
    end
  end

  def init(opts) do
    {:ok, opts}
  end

  @typedoc """
  After the scheduler has started, the init task will be executed after
  `:init_task_delay` milliseconds have passed.
  """
  @type init_task() :: Job.task()
  @typedoc """
  Delay in milliseconds before executing the `:init_task`.
  """
  @type init_task_delay() :: non_neg_integer()

  @type options() :: [option()]
  @type option ::
          {:init_task, Job.task()}
          | {:init_task_delay, init_task_delay()}

  @callback start_link(opts :: options()) ::
              {:ok, pid}
              | {:error, term}

  @doc """
  Start or update a job. If a job with the same ID already exists, the job will be updated with the new `Citrine.Job`.
  """
  @callback put_job(job :: %Job{}) :: DynamicSupervisor.on_start_child()

  @doc """
  Delete and terminate an existing job matching the specified `id`.
  """
  @callback delete_job(job_id :: Job.jobid()) :: :ok
  @callback delete_job(job :: %Job{}) :: :ok

  @doc """
  Retrieve a job by its `id`.
  """
  @callback get_job(job_id :: Job.jobid()) :: {pid(), %Job{}} | nil
  @callback get_job(job :: %Job{}) :: {pid(), %Job{}} | nil

  @doc """
  List all known jobs in the cluster.
  """
  @callback list_jobs() :: [{pid(), %Job{}}]
  @doc """
  Returns a count of the number of jobs running on this node in the cluster.
  """
  @callback count_local_jobs() :: non_neg_integer()
end

defmodule Citrine.Initializer do
  @moduledoc false
  use GenServer, restart: :temporary
  require Logger

  @impl true
  def init(opts) do
    Logger.debug(fn -> "starting #{inspect(__MODULE__)} with opts=#{inspect(opts)}" end)

    name = Keyword.get(opts, :name)
    true = Process.register(self(), name)

    {:ok, Enum.into(opts, %{})}
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def handle_info(:initialize, state) do
    run(state)
    {:stop, :normal, state}
  end

  defp run(%{init_task: init_task, init_task_delay: init_task_delay} = _state) do
    if init_task do
      if init_task_delay > 0 do
        Logger.debug(fn ->
          "waiting #{init_task_delay / 1_000}s before running init task"
        end)

        Process.sleep(init_task_delay)
      end

      try do
        Logger.debug(fn -> "starting init task" end)

        case init_task do
          {mod, fun, args} ->
            :erlang.apply(mod, fun, args)

          {fun, args} ->
            :erlang.apply(fun, args)

          fun when is_function(fun) ->
            fun.()

          _ ->
            Logger.warn(fn ->
              "unexpect value for init_task, skipping (got init_task=#{inspect(init_task)}"
            end)
        end

        Logger.debug(fn -> "finished init task" end)
      rescue
        err ->
          Logger.error(fn -> "error during initialization" end)
          Logger.error(fn -> Exception.format(:error, err, __STACKTRACE__) end)
      end
    end
  end
end

defmodule Citrine.Supervisor do
  @moduledoc false
  require Logger
  use DynamicSupervisor

  def start_link(opts) do
    name = Keyword.take(opts, [:name])
    DynamicSupervisor.start_link(__MODULE__, opts, name)
  end

  @impl true
  def init(opts) do
    Logger.debug(fn -> "starting #{inspect(__MODULE__)} with opts=#{inspect(opts)}" end)
    Process.flag(:trap_exit, true)

    DynamicSupervisor.init(
      strategy: :one_for_one,
      extra_arguments: [opts]
    )
  end

  def terminate(reason, state) do
    Logger.debug(fn ->
      "terminating #{inspect(__MODULE__)} with reason: #{inspect(reason)} and state=#{
        inspect(state)
      }"
    end)

    :ok
  end

  def start_child(name, child) do
    DynamicSupervisor.start_child(name, child)
  end

  def active_children(name) do
    DynamicSupervisor.count_children(name).active
  end

  def terminate_child(name, pid) do
    DynamicSupervisor.terminate_child(name, pid)
  end
end

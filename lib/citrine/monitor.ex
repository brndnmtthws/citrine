defmodule Citrine.Monitor do
  @moduledoc false
  use GenServer
  require Logger

  @impl true
  def init(opts) do
    Logger.debug(fn -> "starting #{inspect(__MODULE__)} with opts=#{inspect(opts)}" end)

    :net_kernel.monitor_nodes(true, node_type: :visible)

    {:ok, opts}
  end

  defp reassign(registry_name, supervisor_name, node) do
    # reassign jobs that were on dead nodes
    jobs = Citrine.Registry.get_jobs_by_node(registry_name, node)
    Logger.debug("orphaned citrine jobs on node=#{inspect(node)}: #{inspect(jobs)}")

    # Use consistent hash to decide which node to reassign jobs to
    alive_nodes =
      [Node.self() | Node.list()]
      |> Enum.reject(fn n -> n == node end)
      |> Enum.filter(&(Node.ping(&1) == :pong))
      |> Enum.sort()

    node_count = Enum.count(alive_nodes)

    for job <- jobs do
      Citrine.Registry.unregister_name(registry_name, job.id)
      digest = :crypto.hash(:sha256, "#{job.id}")
      integer = :binary.decode_unsigned(digest)
      idx = rem(integer, node_count)

      if Enum.at(alive_nodes, idx) == Node.self() do
        Logger.debug(
          "restarting orphaned citrine job=#{inspect(job.id)} on #{inspect(Node.self())}"
        )

        # Restart job on *this* node
        Citrine.Supervisor.start_child(
          supervisor_name,
          {Citrine.JobExecutor, {registry_name, job}}
        )
      end
    end
  end

  @impl true
  def handle_info({:nodedown, node, _node_type}, state) do
    registry_name = Keyword.get(state, :registry_name)
    supervisor_name = Keyword.get(state, :supervisor_name)
    reassign(registry_name, supervisor_name, node)
    {:noreply, state}
  end

  @impl true
  def handle_info({:nodeup, _node, _node_type}, state) do
    {:noreply, state}
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end
end

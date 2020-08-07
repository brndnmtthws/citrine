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
    # reassign any jobs that were on the dead node
    jobs = Citrine.Registry.get_jobs_by_node(registry_name, node)

    for job <- jobs do
      # unregister these jobs so they can be reassigned
      Citrine.Registry.unregister_name(registry_name, job.id)
    end

    Logger.debug(fn -> "orphaned citrine jobs on node=#{inspect(node)}: #{inspect(jobs)}" end)

    # get a list of nodes that are probably alive
    alive_nodes =
      Node.list()
      |> Enum.reject(&(&1 == node))
      |> Enum.filter(&(Node.ping(&1) == :pong))
      |> Enum.concat([Node.self()])
      |> Enum.sort()

    node_count = Enum.count(alive_nodes)

    for job <- jobs do
      # Use consistent hash to decide which node to reassign jobs to
      digest = :crypto.hash(:sha256, "#{job.id}")
      integer = :binary.decode_unsigned(digest)
      idx = rem(integer, node_count)

      new_node = Enum.at(alive_nodes, idx)

      Logger.debug(fn ->
        "restarting orphaned citrine job=#{inspect(job.id)} on #{inspect(new_node)}"
      end)

      :rpc.call(new_node, Citrine.Supervisor, :start_child, [
        supervisor_name,
        {Citrine.JobExecutor, {registry_name, job}}
      ])
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

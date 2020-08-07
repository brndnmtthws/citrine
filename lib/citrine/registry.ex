defmodule Citrine.Registry do
  @moduledoc false
  use GenServer
  require Logger

  @impl true
  def init(opts) do
    Logger.debug(fn -> "starting #{inspect(__MODULE__)} with opts=#{inspect(opts)}" end)

    name = Keyword.get(opts, :name)
    initializer_name = Keyword.get(opts, :initializer_name)

    :ok = init_mnesia(name, initializer_name)

    {:ok, opts}
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  defp create_tables(name) do
    # Create table because it isn't available elsewhere
    :mnesia.create_table(name,
      attributes: [:id, :job, :pid, :node],
      ram_copies: [Node.self()]
    )

    # Index on pid and node
    :mnesia.add_table_index(name, :pid)
    :mnesia.add_table_index(name, :node)
  end

  # Logic partly borrowed from https://github.com/danschultzer/pow/blob/master/lib/pow/store/backend/mnesia_cache/unsplit.ex
  defp heal(name, node) do
    case :mnesia.system_info(:running_db_nodes) |> Enum.member?(node) do
      true ->
        :ok

      false ->
        force_reload(name, node)
    end
  end

  defp force_reload(name, node) do
    [master_nodes, nodes] = sorted_cluster_islands(name, node)

    for node <- nodes do
      :stopped = :rpc.call(node, :mnesia, :stop, [])

      :ok = :rpc.call(node, :mnesia, :set_master_nodes, [name, master_nodes])

      :ok = :rpc.block_call(node, :mnesia, :start, [])
      :ok = :rpc.call(node, :mnesia, :wait_for_tables, [[name], :timer.seconds(15)])

      Logger.debug(fn ->
        "[#{inspect(__MODULE__)}] #{inspect(node)} has been healed and joined #{
          inspect(master_nodes)
        }"
      end)
    end

    :ok
  end

  defp sorted_cluster_islands(name, node) do
    island_a = :mnesia.system_info(:running_db_nodes)
    island_b = :rpc.call(node, :mnesia, :system_info, [:running_db_nodes])

    Enum.sort([island_a, island_b], &older?(name, &1, &2))
  end

  defp get_all_nodes_for_table(name) do
    [:ram_copies, :disc_copies, :disc_only_copies]
    |> Enum.map(&:mnesia.table_info(name, &1))
    |> Enum.concat()
  end

  defp older?(name, island_a, island_b) do
    all_nodes = get_all_nodes_for_table(name)
    island_nodes = Enum.concat(island_a, island_b)

    oldest_node = all_nodes |> Enum.reverse() |> Enum.find(&Enum.member?(island_nodes, &1))

    Enum.member?(island_a, oldest_node)
  end

  defp init_mnesia(name, initializer_name) do
    # Wait up to 500 millis before trying to initialize to prevent every node initializing simultaneously
    Process.sleep(:rand.uniform(500))

    # Only allow initializing one instance at a time
    :global.trans({name, self()}, fn ->
      db_nodes =
        Node.list()
        |> Enum.filter(&(:rpc.block_call(&1, :mnesia, :system_info, [:is_running]) == :yes))
        |> Enum.sort()

      unless Enum.empty?(db_nodes) do
        case :mnesia.system_info(:running_db_nodes) do
          [] ->
            :ok

          _nodes ->
            # Stop mnesia
            :mnesia.stop()

            # Delete any local schema
            :mnesia.delete_schema([Node.self()])

            # Set master nodes
            :mnesia.set_master_nodes(name, db_nodes)
        end

        # Start mnesia
        :mnesia.start()

        # Add other running DB nodes
        {:ok, _} = :mnesia.change_config(:extra_db_nodes, db_nodes)

        # Change schema type
        :mnesia.change_table_copy_type(:schema, Node.self(), :ram_copies)

        # Add existing table copy if it exists
        :mnesia.add_table_copy(name, Node.self(), :ram_copies)
      else
        # Start mnesia
        Application.start(:mnesia)

        # Change schema type
        :mnesia.change_table_copy_type(:schema, Node.self(), :ram_copies)
      end

      unless Enum.member?(:mnesia.system_info(:tables), name) do
        create_tables(name)
      end

      :mnesia.subscribe({:table, name, :detailed})
      :mnesia.subscribe(:system)

      # Wait up to 10s for tables
      :ok = :mnesia.wait_for_tables([name], 60_000)
    end)

    # Notify initializer to begin
    send(initializer_name, :initialize)

    :ok
  end

  @impl true
  def handle_info(
        {:mnesia_system_event, {:inconsistent_database, _context, node}},
        %{name: name} = state
      ) do
    :global.trans({name, self()}, fn -> heal(name, node) end)

    {:noreply, state}
  end

  @impl true
  def handle_info(
        {:mnesia_table_event, {:delete, _name, _new_job, old_jobs, _activity_id}},
        state
      ) do
    this_node = Node.self()

    Logger.debug(fn ->
      ":delete event on node=#{inspect(this_node)} old_jobs=#{inspect(old_jobs)}"
    end)

    case old_jobs do
      [{_name, _job_id, %Citrine.Job{} = _job, pid, ^this_node}] ->
        if Process.alive?(pid) do
          Logger.debug(fn -> "terminating pid=#{inspect(pid)}" end)
          Process.send(pid, :terminate, [])
        end

      _ ->
        nil
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:mnesia_table_event, {:write, _name, new_job, old_jobs, _activity_id}}, state) do
    this_node = Node.self()

    Logger.debug(fn ->
      ":write event on node=#{inspect(this_node)} new_job=#{inspect(new_job)} old_jobs=#{
        inspect(old_jobs)
      }"
    end)

    case old_jobs do
      [{_name, _job_id, %Citrine.Job{} = _job, old_pid, ^this_node}] ->
        case new_job do
          {_name, _job_id, %Citrine.Job{} = _job, new_pid, _new_node} ->
            if old_pid != new_pid and Process.alive?(old_pid) do
              Logger.debug(fn -> "terminating old_pid=#{inspect(old_pid)}" end)
              Process.send(old_pid, :terminate, [])
            end

          _ ->
            nil
        end

      _ ->
        nil
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:mnesia_system_event, _event}, state) do
    {:noreply, state}
  end

  def register_job(registry_name, job = %Citrine.Job{}) do
    Logger.debug(fn -> "registering job #{inspect(job)}" end)

    case :mnesia.sync_transaction(fn ->
           record =
             case :mnesia.wread({registry_name, job.id}) do
               [{_table, _id, %Citrine.Job{} = _job, pid, node}] ->
                 # Record already exists in table, keep existing pid/node
                 {registry_name, job.id, job, pid, node}

               _ ->
                 {registry_name, job.id, job, nil, nil}
             end

           Logger.debug(fn -> "register_job writing record=#{inspect(record)}" end)
           :mnesia.write(record)
         end) do
      {:atomic, :ok} -> :ok
      _ -> :error
    end
  end

  def unregister_job(registry_name, job_id) do
    Logger.debug(fn -> "unregistering job_id=#{job_id}" end)

    case :mnesia.sync_transaction(fn ->
           :mnesia.delete({registry_name, job_id})
         end) do
      {:atomic, :ok} ->
        :ok

      _ ->
        nil
    end
  end

  def lookup_job(registry_name, job_id) do
    case :mnesia.sync_transaction(fn ->
           :mnesia.read({registry_name, job_id})
         end) do
      {:atomic, [{_table, _id, %Citrine.Job{} = job, _pid, _node}]} ->
        job

      _ ->
        nil
    end
  end

  def list_jobs(registry_name) do
    case :mnesia.sync_transaction(fn ->
           :mnesia.foldl(
             fn {_registry_name, _id, job, pid, _node}, jobs ->
               jobs ++ [{pid, job}]
             end,
             [],
             registry_name
           )
         end) do
      {:atomic, result} -> result
      _ -> []
    end
  end

  @spec register_name({atom, atom}, any) :: :no | :yes
  def register_name({registry_name, job_id}, pid) do
    Logger.debug(fn -> "registering name job_id=#{inspect(job_id)} with pid=#{inspect(pid)}" end)

    case :mnesia.sync_transaction(fn ->
           case :mnesia.wread({registry_name, job_id}) do
             [{_table, _id, %Citrine.Job{} = job, nil, nil}] ->
               record = {registry_name, job_id, job, pid, node(pid)}
               Logger.debug(fn -> "register_name writing record=#{inspect(record)}" end)
               :mnesia.write(record)
               :yes

             _ ->
               :no
           end
         end) do
      {:atomic, res} -> res
      _ -> :no
    end
  end

  def whereis_name(registry_name, job_id), do: whereis_name({registry_name, job_id})

  @spec whereis_name({atom(), Citrine.Job.jobid()}) :: pid() | :undefined
  def whereis_name({registry_name, job_id}) do
    case :mnesia.sync_transaction(fn ->
           :mnesia.read({registry_name, job_id})
         end) do
      {:atomic, [{_table, _id, %Citrine.Job{} = _job, pid, _node}]} ->
        case pid do
          nil -> :undefined
          pid -> pid
        end

      _ ->
        :undefined
    end
  end

  def unregister_name(registry_name, job_id), do: unregister_name({registry_name, job_id})

  def unregister_name({registry_name, job_id}) do
    Logger.debug(fn -> "unregistering name #{job_id}" end)

    :mnesia.sync_transaction(fn ->
      case :mnesia.wread({registry_name, job_id}) do
        [{_table, _id, %Citrine.Job{} = job, _pid, _node}] ->
          record = {registry_name, job_id, job, nil, nil}
          Logger.debug(fn -> "unregister_name writing record=#{inspect(record)}" end)
          :mnesia.write(record)

        _ ->
          nil
      end
    end)

    job_id
  end

  def get_jobs_by_node(registry_name, node) do
    case :mnesia.sync_transaction(fn ->
           :mnesia.index_read(registry_name, node, :node)
         end) do
      {:atomic, records} ->
        Enum.map(records, fn {_table, _id, %Citrine.Job{} = job, _pid, _node} -> job end)

      _ ->
        nil
    end
  end
end

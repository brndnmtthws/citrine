defmodule Citrine.Registry do
  @moduledoc false
  use GenServer
  require Logger

  @impl true
  def init(opts) do
    Logger.debug(fn -> "starting #{inspect(__MODULE__)} with opts=#{inspect(opts)}" end)

    name = Keyword.get(opts, :name)
    :ok = init_mnesia(name)

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

  defp init_mnesia(name) do
    db_nodes =
      Node.list()
      |> Enum.filter(&(:rpc.block_call(&1, :mnesia, :system_info, [:is_running]) == :yes))

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

      # Add running DB nodes
      {:ok, _} = :mnesia.change_config(:extra_db_nodes, [List.first(db_nodes)])

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

    # Wait up to 10s for tables
    :ok = :mnesia.wait_for_tables([name], 60_000)

    :ok
  end

  def register_job(registry_name, job = %Citrine.Job{}) do
    Logger.debug("registering job #{inspect(job)}")

    case :mnesia.sync_transaction(fn ->
           case :mnesia.wread({registry_name, job.id}) do
             [{_table, _id, %Citrine.Job{} = _job, pid, node}] ->
               # Record already exists in table, keep existing pid/node
               :mnesia.write({registry_name, job.id, job, pid, node})

             _ ->
               :mnesia.write({registry_name, job.id, job, nil, nil})
           end
         end) do
      {:atomic, :ok} -> :ok
      _ -> :error
    end
  end

  def unregister_job(registry_name, job_id) do
    Logger.debug("unregistering job_id=#{job_id}")

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
    Logger.debug("registering name job_id=#{inspect(job_id)} with pid=#{inspect(pid)}")

    case :mnesia.sync_transaction(fn ->
           case :mnesia.wread({registry_name, job_id}) do
             [{_table, _id, %Citrine.Job{} = job, nil, nil}] ->
               :mnesia.write({registry_name, job_id, job, pid, node(pid)})
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
    Logger.debug("unregistering name #{job_id}")

    :mnesia.sync_transaction(fn ->
      case :mnesia.wread({registry_name, job_id}) do
        [{_table, _id, %Citrine.Job{} = job, _pid, _node}] ->
          :mnesia.write({registry_name, job_id, job, nil, nil})
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

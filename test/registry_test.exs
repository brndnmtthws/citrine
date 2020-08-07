defmodule Citrine.RegistryTest do
  use ExUnit.Case, async: true
  alias Citrine.Registry

  def gen_id() do
    :rand.uniform(10000)
    |> Integer.to_string()
  end

  def gen_job_id() do
    gen_id()
  end

  setup do
    registry_name = String.to_atom("#{__MODULE__}.#{gen_id()}")

    initializer = start_supervised!({Citrine.Initializer, [name: :initializer]})

    registry =
      start_supervised!({Registry, [name: registry_name, initializer_name: :initializer]})

    %{registry: registry, registry_name: registry_name}
  end

  describe "single node" do
    test "initializes empty", %{registry_name: registry_name} do
      assert Registry.list_jobs(registry_name) == []
    end

    test "registers and unregisters a job", %{registry_name: registry_name} do
      job_id = gen_job_id()

      job = %Citrine.Job{
        id: job_id,
        schedule: "* * * * *",
        task: fn -> nil end,
        extended_syntax: false
      }

      assert Registry.register_job(registry_name, job) == :ok
      assert Registry.lookup_job(registry_name, job.id) == job
      assert Registry.unregister_job(registry_name, job.id) == :ok
      assert Registry.lookup_job(registry_name, job.id) == nil
    end
  end
end

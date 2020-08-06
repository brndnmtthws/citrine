defmodule Citrine.SchedulerTest do
  use ExUnit.Case, async: true

  defmodule Scheduler do
    use Citrine.Scheduler, otp_app: :scheduler_test
  end

  def gen_id() do
    :rand.uniform(10000)
    |> Integer.to_string()
  end

  def gen_job_id() do
    gen_id()
    |> String.to_atom()
  end

  def while(pred, func) do
    if pred.() do
      func.()
      while(func, pred)
    end
  end

  setup do
    scheduler = start_supervised!({Scheduler, []})

    %{
      scheduler: scheduler
    }
  end

  describe "single node" do
    test "registers and unregisters a job" do
      job_id = gen_job_id()

      job = %Citrine.Job{
        id: job_id,
        schedule: "* * * * *",
        task: fn -> nil end,
        extended_syntax: false
      }

      {:ok, pid} = Scheduler.put_job(job)
      while(fn -> length(Scheduler.list_jobs()) == 0 end, fn -> Process.sleep(50) end)
      assert Scheduler.list_jobs() == [{pid, job}]
      assert Scheduler.count_local_jobs() == 1
      {_gotpid, gotjob} = Scheduler.get_job(job.id)
      assert job.id == gotjob.id
      assert Scheduler.delete_job(job.id) == :ok
    end
  end
end

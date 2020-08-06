defmodule Citrine.JobTest do
  use ExUnit.Case, async: true
  require Logger

  defmodule Scheduler1 do
    use Citrine.Scheduler, otp_app: :citrine_test_1
  end

  defmodule Scheduler2 do
    use Citrine.Scheduler, otp_app: :citrine_test_2
  end

  defp add(a, b), do: a + b

  defp sleep_until_next_window(schedule) do
    # Sleep until the start of next execution window
    Crontab.Scheduler.get_next_run_date!(Crontab.CronExpression.Parser.parse!(schedule, true))
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.diff(DateTime.utc_now(), :millisecond)
    # Add 250 millis to allow jobs to complete, it can take time for everything
    # to synchronize
    |> add(250)
    # Don't allow any less than 5 millis
    |> max(5)
    |> Process.sleep()
  end

  defp read_tmp(tmp_path) do
    Logger.debug("reading from #{tmp_path}")
    {:ok, content} = File.read(tmp_path)

    case Integer.parse(content) do
      :error ->
        Process.sleep(1)
        read_tmp(tmp_path)

      {count, _} ->
        count
    end
  end

  setup do
    {:ok, tmp_path} = Temp.path()
    :ok = File.write(tmp_path, "0")

    job_task = fn ->
      :global.trans(
        {:tmp_lock, self()},
        fn ->
          count = read_tmp(tmp_path) + 1

          Logger.debug("writing #{count} to #{tmp_path}")
          :ok = File.write(tmp_path, Integer.to_string(count))
        end,
        [Node.self() | Node.list()]
      )
    end

    %{tmp_path: tmp_path, job_task: job_task}
  end

  describe "multiple nodes" do
    test "simple", %{tmp_path: tmp_path, job_task: job_task} do
      [node1, node2, node3] =
        nodes = LocalCluster.start_nodes("citrine-test-simple", 3, files: [__ENV__.file])

      IO.inspect(nodes, label: "Test nodes that are now online")

      # Ensure online
      assert Node.ping(node1) == :pong
      assert Node.ping(node2) == :pong
      assert Node.ping(node3) == :pong

      for node <- nodes do
        {:ok, _} = :rpc.block_call(node, Scheduler1, :start_link, [])
      end

      tester = self()

      Node.spawn(node1, fn ->
        job1 = %Citrine.Job{
          id: "job1",
          schedule: "* * * * * *",
          task: job_task,
          extended_syntax: true
        }

        sleep_until_next_window(job1.schedule)

        {:ok, pid1} = Scheduler1.put_job(job1)

        # Should run three times
        sleep_until_next_window(job1.schedule)
        sleep_until_next_window(job1.schedule)
        sleep_until_next_window(job1.schedule)

        {pid, job} = Scheduler1.get_job("job1")
        assert job1 == job
        assert pid1 == pid

        job2 = %Citrine.Job{
          id: "job1",
          schedule: "*/2 * * * * *",
          task: job_task,
          extended_syntax: true
        }

        # One more run
        sleep_until_next_window(job1.schedule)

        {:ok, pid2} = Scheduler1.put_job(job2)

        # Should run twice
        sleep_until_next_window(job2.schedule)
        sleep_until_next_window(job2.schedule)

        [{pid, job}] = Scheduler1.list_jobs()
        assert job == job2
        assert pid == pid2

        {pid, job} = Scheduler1.get_job("job1")
        assert job == job2
        assert pid == pid1

        Scheduler1.delete_job("job1")

        # One more run for job2
        sleep_until_next_window(job2.schedule)

        assert nil == Scheduler1.get_job("job1")

        send(tester, :node1_finished)
      end)

      assert_receive :node1_finished, 20_000

      count = read_tmp(tmp_path)
      assert count == 6

      :ok = LocalCluster.stop_nodes([node1])

      Node.spawn(node2, fn ->
        job1 = %Citrine.Job{
          id: "job1",
          schedule: "* * * * * *",
          task: job_task,
          extended_syntax: true
        }

        sleep_until_next_window(job1.schedule)

        {:ok, _pid} = Scheduler1.put_job(job1)
        {_pid, job} = Scheduler1.get_job(job1)
        assert job1 == job

        sleep_until_next_window(job1.schedule)
        sleep_until_next_window(job1.schedule)

        Scheduler1.delete_job("job1")

        assert nil == Scheduler1.get_job("job1")

        sleep_until_next_window(job1.schedule)

        send(tester, :node2_finished)
      end)

      assert_receive :node2_finished, 20_000

      count = read_tmp(tmp_path)
      assert count == 8
    end

    @tag :shutdown
    test "after shutdown", %{tmp_path: tmp_path, job_task: job_task} do
      [node1, node2, node3, node4, node5] =
        nodes = LocalCluster.start_nodes("citrine-test-shutdown", 5, files: [__ENV__.file])

      IO.inspect(nodes, label: "Test nodes that are now online")

      # Ensure online
      assert Node.ping(node1) == :pong
      assert Node.ping(node2) == :pong
      assert Node.ping(node3) == :pong
      assert Node.ping(node4) == :pong
      assert Node.ping(node5) == :pong

      for node <- nodes do
        {:ok, _} = :rpc.block_call(node, Scheduler2, :start_link, [])
      end

      tester = self()

      job1 = %Citrine.Job{
        id: "job1",
        schedule: "* * * * * *",
        task: job_task,
        extended_syntax: true
      }

      job2 = %Citrine.Job{
        id: "job2",
        schedule: "* * * * * *",
        task: job_task,
        extended_syntax: true
      }

      sleep_until_next_window(job1.schedule)

      for node <- nodes do
        Node.spawn(node, fn ->
          send(tester, String.to_atom("#{node}.start1"))

          {:ok, pid1} = Scheduler2.put_job(job1)

          {pid, job} = Scheduler2.get_job("job1")
          assert job1.id == job.id
          assert pid1 == pid

          send(tester, String.to_atom("#{node}.ok1"))
        end)

        Process.sleep(15)
      end

      for node <- nodes do
        ok = String.to_atom("#{node}.ok1")
        assert_receive ok, 1_500
      end

      # Run twice
      sleep_until_next_window(job1.schedule)
      sleep_until_next_window(job1.schedule)

      count = read_tmp(tmp_path)
      assert count == 2

      for node <- Enum.reverse(nodes) do
        Node.spawn(node, fn ->
          {:ok, pid2} = Scheduler2.put_job(job2)

          {pid, job} = Scheduler2.get_job("job2")
          assert job2.id == job.id
          assert pid2 == pid

          send(tester, String.to_atom("#{node}.ok2"))
        end)

        Process.sleep(15)
      end

      for node <- nodes do
        ok = String.to_atom("#{node}.ok2")
        assert_receive ok, 1_500
      end

      # Should run twice again, for two jobs
      sleep_until_next_window(job2.schedule)
      sleep_until_next_window(job2.schedule)

      count = read_tmp(tmp_path)
      assert count == 6

      :ok = LocalCluster.stop_nodes([node4, node5])

      # Two more runs
      sleep_until_next_window(job1.schedule)
      sleep_until_next_window(job1.schedule)

      Node.spawn(node1, fn ->
        {_pid, job} = Scheduler2.get_job("job1")
        assert job1.id == job.id
        {_pid, job} = Scheduler2.get_job("job2")
        assert job2.id == job.id

        send(tester, :node1_ok)
      end)

      Node.spawn(node2, fn ->
        {_pid, job} = Scheduler2.get_job("job1")
        assert job1.id == job.id
        {_pid, job} = Scheduler2.get_job("job2")
        assert job2.id == job.id

        send(tester, :node2_ok)
      end)

      Node.spawn(node3, fn ->
        {_pid, job} = Scheduler2.get_job("job1")
        assert job1.id == job.id
        {_pid, job} = Scheduler2.get_job("job2")
        assert job2.id == job.id

        send(tester, :node3_ok)
      end)

      assert_receive :node1_ok, 100
      assert_receive :node2_ok, 100
      assert_receive :node3_ok, 100

      count = read_tmp(tmp_path)
      assert count == 10
    end
  end
end

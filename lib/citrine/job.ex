defmodule Citrine.Job do
  @typedoc """
  The job task to run.
  """
  @type task :: {module(), atom, [term()]} | {atom, [term()]} | (() -> any)
  @typedoc """
  A unique ID for this job.
  """
  @type jobid :: String.t()
  @typedoc """
  Cron-based schedule specification.
  """
  @type schedule :: String.t()

  @type t :: %__MODULE__{
          id: jobid(),
          schedule: String.t(),
          extended_syntax: boolean(),
          task: task()
        }

  @enforce_keys [:id, :schedule, :task]

  @doc """
  Struct that describes a job. Must specify a unique `:id`, a `:schedule` using
  cron syntax, and a task. If `:extended_syntax` is set to `true`, the schedule
  will be parsed as extended cron syntax by `Crontab.CronExpression.Parser`.

  ## Example

      iex(1)> job = %Citrine.Job{
      ...(1)>   id: "my-job-id",
      ...(1)>   schedule: "* * * * * *",
      ...(1)>   task: fn -> nil end,
      ...(1)>   extended_syntax: true
      ...(1)> }
  """
  defstruct [:id, :schedule, :task, extended_syntax: false]
end

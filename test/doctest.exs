defmodule Citrine.Doctest do
  use ExUnit.Case, async: true
  doctest Citrine.Scheduler
  doctest Citrine.Job
end

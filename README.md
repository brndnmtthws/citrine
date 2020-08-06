[![Elixir CI](https://github.com/brndnmtthws/citrine/workflows/Elixir%20CI/badge.svg?branch=master)](https://github.com/brndnmtthws/citrine/actions?query=workflow%3A%22Elixir+CI%22+branch%3Amaster) [![Elixir Publish](https://github.com/brndnmtthws/citrine/workflows/Elixir%20Publish/badge.svg?branch=master)](https://github.com/brndnmtthws/citrine/actions?query=workflow%3A%22Elixir+Publish%22) [![Hex pm](http://img.shields.io/hexpm/v/citrine.svg?style=flat)](https://hex.pm/packages/citrine)
# Citrine

_Citrine energizes every level of life. It cleanses the chakras and opens the intuition. It's one of the most powerful energy crystals in the world._

Citrine is a library for running scheduled jobs on an Erlang cluster, similar
to cron. Citrine was created to satisfy a few simple requirements:

* Provide reliable execution of jobs according to a schedule
* Keep jobs running in the event of network splits or other failure scenarios
* Build upon Elixir and Erlang primitives with minimum external dependencies
* Be easy to operate and reason about
* Work well in stateless container-based environments such as Kubernetes
* Distribute jobs globally across a cluster
* Follow a pattern of convention over configuration and minimize the number of knobs required

Internally Citrine uses Erlang's excellent [mnesia
module](https://erlang.org/doc/man/mnesia.html) for state and process
management. It uses in-memory tables, which comes with certain caveats.

If you're using mnesia elsewhere in your application, you should be aware that Citrine will attempt to join existing mnesia clusters.

## What Citrine can and cannot do

Citrine is not a perfect solution for all scheduling needs. There are certain features Citrine does not implement, or even try to implement, and if you require these features you should either look elsewhere or consider different patterns.

* **Best effort**: There are some scenarios in which Citrine *could* potentially miss execution of a job, such as during a rolling cluster update. Citrine will always try its best to execute jobs according to a schedule, but in some cases it may not.
* **Exactly once semantics**: Citrine does not attempt to guarantee a job runs exactly once according to a schedule, it prefers to run jobs multiple times in failure scenarios such as a netsplit.
* **Interruption handling**: In the event a job is interrupted during execution, Citrine makes no attempt to restart or re-run the job.
* **Idempotency**: This is not so much a limitation but rather a warning: your jobs should be designed to be idempotent to prevent strange side effects. Citrine cannot provide any guarantees.
* **Clustering**: Citrine assumes you've already specified the cluster topology, either manually or using a library such as [libcluster](https://hexdocs.pm/libcluster/readme.html).

If you need stronger guarantees of execution, it's recommended that you
use a combination of a stateful queueing service with a worker library, such
as [Exq](https://github.com/akira/exq).

If you're okay with the limitations of the library, Citrine
is a great choice due to minimal dependencies and operational burden.

## Installation

The package can be installed by adding `citrine` to your list of dependencies
in `mix.exs`:

```elixir
def deps do
  [
    {:citrine, "~> 0.1.0"}
  ]
end
```

## Usage

Start by creating a module for the core scheduler:

```elixir
defmodule MyApp.Scheduler do
  use Citrine.Scheduler, otp_app: :myapp
end
```

Add the scheduler to your supervisor tree, for example in `application.ex`:

```elixir
defmodule MyApp.Application do
  use Application
  def start(_type, _args) do
    children = [
      # Start the Citrine scheduler
      MyApp.Scheduler
    ]
    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

Using the scheduler:

```elixir
# Create a job
job = %Citrine.Job{
  id: :my_job_id,
  schedule: "* * * * * *", # Run every second
  task: job_task,
  extended_syntax: true # Use extended cron syntax
}
# Start or update a job
MyApp.Scheduler.put_job(job)
# Terminate and delete a job
MyApp.Scheduler.delete_job(job)
```

For further details, [see HexDocs](http://hexdocs.pm/citrine/).

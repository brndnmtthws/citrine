defmodule Citrine.MixProject do
  use Mix.Project

  def project do
    [
      app: :citrine,
      version: "0.1.12",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),

      # Docs
      name: "Citrine",
      source_url: "https://github.com/brndnmtthws/citrine",
      homepage_url: "http://hexdocs.pm/citrine/readme.html",
      docs: [
        # The main page in the docs
        main: "readme",
        extras: ["README.md", "LICENSE"]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      included_applications: [:mnesia]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:crontab, "~> 1.1"},
      {:local_cluster, "~> 1.1", only: [:test]},
      {:temp, "~> 0.4", only: [:test]},
      {:ex_doc, "~> 0.22", only: :dev, runtime: false}
    ]
  end

  defp description() do
    "Elixir library for running cron-based scheduled jobs on your Erlang cluster."
  end

  defp package() do
    [
      name: "citrine",
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/brndnmtthws/citrine"}
    ]
  end
end

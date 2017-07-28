defmodule Mux.Mixfile do
  use Mix.Project

  def project do
    [app: :mux,
     version: "0.1.0",
     elixir: "~> 1.5",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  def application do
    []
  end

  defp deps do
    [{:acceptor_pool, "1.0.0-rc.0", optional: true},
     {:stream_data, github: "whatyouhide/stream_data", only: :test}]
  end
end

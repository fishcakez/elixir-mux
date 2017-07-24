defmodule Mux.Mixfile do
  use Mix.Project

  def project do
    [app: :mux,
     version: "0.1.0",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  def application do
    []
  end

  defp deps do
    [{:stream_data, github: "whatyouhide/stream_data", only: :test}]
  end
end

defmodule Mux.Client.Dispatcher do
  @moduledoc false

  @spec lookup(Mux.Packet.dest) :: {:ok, pid, {module, any}} | :nack
  def lookup(dest) do
    case Registry.lookup(Mux.Client.Connection, dest) do
      [] ->
        :nack
      [{pid, info}] ->
        {:ok, pid, info}
      sessions ->
        # poor load balancing as could hit slow or expired lease client
        len = length(sessions)
        at = :rand.uniform(len) - 1
        {pid, info} = Enum.at(sessions, at)
        {:ok, pid, info}
    end
  end
end

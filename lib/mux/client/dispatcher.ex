defmodule Mux.Client.Dispatcher do
  @moduledoc false

  # only need to implement whereis_name as using Registry underneath
  @spec whereis_name(Mux.Packet.dest) :: pid | :undefined
  def whereis_name(dest) do
    case Registry.lookup(Mux.ClientSession, dest) do
      [] ->
        :undefined
      [{pid, _}] ->
        pid
      sessions ->
        # very poor load balancing as could hit slow, expired lease or
        # draining client but unlucky to hit disconnected: unsure if any open
        # source pool would do better as client will nack without hitting
        # network on draining or max exchanges
        len = length(sessions)
        at = :rand.uniform(len) - 1
        {pid, _} = Enum.at(sessions, at)
        pid
    end
  end
end

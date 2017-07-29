defmodule Mux.Alarm.Watcher do
  @moduledoc false

  use GenServer

  @spec start_link(:ets.tab) :: GenServer.on_start
  def start_link(table),
    do: GenServer.start_link(__MODULE__, table, [name: table])

  def init(table) do
    :ok = Mux.Alarm.swap_sup_handler(table)
    {:ok, table}
  end

  def handle_info({:gen_event_EXIT, {_, table}, reason}, table),
    do: {:stop, reason, table}
  def handle_info(msg, table) do
    :error_logger.error_msg("#{__MODULE__} received unexpected message: ~p~n",
                            [msg])
    {:noreply, table}
  end
end

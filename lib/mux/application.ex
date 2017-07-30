defmodule Mux.Application do
  @moduledoc false

  use Application

  def start(_, _) do
    alarm_tab = Mux.Alarm.new_table(Mux.Alarm)
    children = [registry(Mux.Server, :unique),
                registry(Mux.Server.Pool, :unique),
                registry(Mux.Server.Socket, :unique),
                registry(Mux.ServerSession, :duplicate),
                registry(Mux.Client, :duplicate),
                registry(Mux.Client.Pool, :duplicate),
                registry(Mux.ClientSession, :duplicate),
                {Mux.Alarm.Supervisor, alarm_tab}]
    case Supervisor.start_link(children, [strategy: :one_for_one]) do
      {:ok, pid} ->
        {:ok, pid, alarm_tab}
      {:error, _} = error ->
        Mux.Alarm.delete_table(alarm_tab)
        error
    end
  end

  def stop(alarm_tab),
    do: Mux.Alarm.delete_table(alarm_tab)

  defp registry(name, keys) do
    child_opts = [id: Module.concat(name, Registry)]
    Supervisor.child_spec({Registry, [name: name, keys: keys]}, child_opts)
  end
end

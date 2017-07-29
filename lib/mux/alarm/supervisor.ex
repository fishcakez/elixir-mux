defmodule Mux.Alarm.Supervisor do
  @moduledoc false

  use Supervisor

  @spec start_link(:ets.tab) :: Supervisor.on_start()
  def start_link(alarm_tab),
    do: Supervisor.start_link(__MODULE__, alarm_tab)

  def init(alarm_tab) do
    alarm_reg =  Module.concat(alarm_tab, Registry)
    children = [{Registry, [name: alarm_reg, keys: :duplicate]},
                {Mux.Alarm.Watcher, alarm_tab}]
    # if watcher goes down the handler will crash and will lose the alarm
    # handler events before we re-sub whether we crash registry or not, so don't
    # crash it and perform best effort so subscribers can continue
    Supervisor.init(children, [strategy: :rest_for_one])
  end
end

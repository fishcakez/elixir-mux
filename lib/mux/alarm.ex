defmodule Mux.Alarm do
  @moduledoc false

  @behaviour :gen_event

  @spec new_table(atom) :: :ets.tab
  def new_table(table),
    do: :ets.new(table, [:named_table, :public, {:read_concurrency, true}])

  @spec delete_table(:ets.tab) :: true
  def delete_table(table),
    do: :ets.delete(table)

  @spec swap_sup_handler(:ets.tab) :: :ok
  def swap_sup_handler(table) do
    handler = {__MODULE__, table}
    remove = {handler, :swap}
    add = {handler, table}
    :ok = :gen_event.swap_sup_handler(:alarm_handler, remove, add)
  end

  @spec register(:ets.tab, any, reference) :: boolean
  def register(table, id, ref) do
    {:ok, _} = Registry.register(Module.concat(table, Registry), id, ref)
    handler = {__MODULE__, table}
    # sync with handler to ensure no missed set/clears
    {:ok, set?} = :gen_event.call(:alarm_handler, handler, {:set?, id})
    set?
  end

  def init({table, _}),
    do: {:ok, {table, Module.concat(table, Registry)}}

  def handle_event({:set_alarm, {id, _} = alarm}, {table, registry} = state) do
    :ets.insert(table, alarm)
    # need to serialize notify to prevent out of order set/clear on same alarm
    Registry.dispatch(registry, id, &notify(&1, :SET, id), [parallel: false])
    {:ok, state}
  end
  def handle_event({:clear_alarm, id}, {table, registry} = state) do
    :ets.delete(table, id)
    Registry.dispatch(registry, id, &notify(&1, :CLEAR, id), [parallel: false])
    {:ok, state}
  end

  def handle_call({:set?, id}, {table, _} = state),
    do: {:ok, {:ok, :ets.member(table, id)}, state}

  def handle_info(_, state),
    do: {:ok, state}

  def code_change(_, state, _),
    do: {:ok, state}

  def terminate(_, _),
    do: :ok

  defp notify(subs, tag, id),
    do: Enum.each(subs, fn {pid, ref} -> send(pid, {tag, ref, id}) end)
end

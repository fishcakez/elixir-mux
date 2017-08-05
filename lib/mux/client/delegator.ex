defmodule Mux.Client.Delegator do
  @moduledoc false
  @behaviour Mux.Client.Connection

  defmodule Data do
    @moduledoc false
    @enforce_keys [:status, :dest, :session, :present]
    defstruct [:status, :dest, :session, :present]
  end

  def init({dest, {session, arg1}, {present, arg2}}) do
    {:ok, headers, state1} = apply(session, :init, [arg1])
    try do
      {:ok, _state2} = apply(present, :init, [arg2])
    catch
      kind, reason ->
        stack = System.stacktrace()
        apply(session, :terminate, [reason, state1])
        :erlang.raise(kind, reason, stack)
    else
      {:ok, state2} ->
        data = %Data{status: :init, dest: dest, session: {session, state1},
                     present: {present, state2}}
        {:ok, headers, data}
    end
  end

  def handshake(headers, %Data{session: {session, state}} = data) do
    {:ok, opts, state} = apply(session, :handshake, [headers, state])
    {:ok, opts, next(:handshake, %Data{data | session: {session, state}})}
  end

  def lease(_, 0, data),
    do: {:ok, next(:expire, data)}
  def lease(_, _, data),
    do: {:ok, next(:lease, data)}

  def drain(data),
    do: {:ok, next(:drain, data)}

  def terminate(reason, data) do
    %Data{dest: dest, session: session_info, present: present_info} = data
    try do
      Registry.unregister(Mux.Client.Connection, dest)
    after
      terminate(reason, session_info, present_info)
    end
  end

  defp terminate(reason, {session, state1}, {present, state2}) do
    # terminate in reverse order of init/1
    apply(present, :terminate, [reason, state2])
  after
    apply(session, :terminate, [reason, state1])
  end

  # no change
  defp next(status, %Data{status: status} = data),
    do: data

  # drain dominates
  defp next(_, %Data{status: :drain} = data),
    do: data
  defp next(:drain, %Data{status: :lease, dest: dest} = data) do
    Registry.unregister(Mux.Client.Connection, dest)
    %Data{data | status: :drain}
  end
  defp next(:drain, data),
    do: %Data{data | status: :drain}

  defp next(:handshake, %Data{status: :init} = data) do
    %Data{dest: dest, present: present_info} = data
    Registry.register(Mux.Client.Connection, dest, present_info)
    %Data{data | status: :lease}
  end
  defp next(:expire, %Data{status: :init} = data),
    do: %Data{data | status: :init_expire}
  defp next(:lease, %Data{status: :init} = data),
    do: data

  defp next(:handshake, %Data{status: :init_expire} = data),
    do: %Data{data | status: :expire}
  defp next(:expire, %Data{status: :init_expire} = data),
    do: data
  defp next(:lease, %Data{status: :init_expire} = data),
    do: %Data{data | status: :init}

  defp next(:lease, %Data{status: :expire} = data) do
    %Data{dest: dest, present: present_info} = data
    Registry.register(Mux.Client.Connection, dest, present_info)
    %Data{data | status: :lease}
  end

  defp next(:expire, %Data{status: :lease, dest: dest} = data) do
    Registry.unregister(Mux.Client.Connection, dest)
    %Data{data | status: :expire}
  end
end

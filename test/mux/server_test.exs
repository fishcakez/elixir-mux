defmodule Mux.ServerTest do
  use ExUnit.Case, async: true

  setup context do
    dest = "#{inspect context[:case]} #{context[:test]}"
    drain_id = {:drain, dest}
    lease_id = {:lease, dest}
    if context[:set_drain], do: :alarm_handler.set_alarm({drain_id, self()})
    if context[:set_lease], do: :alarm_handler.set_alarm({lease_id, self()})

    opts =
      context
      |> Map.to_list()
      |> Keyword.put_new(:debug, [:log])
      |> Keyword.put_new(:drain_alarms, [drain_id])
      |> Keyword.put_new(:lease_alarms, [lease_id])

    {cli, srv, sup} = pair(dest, opts)

    {:ok, [client: cli, server: srv, supervisor: sup, dest: dest,
           drain_id: drain_id, lease_id: lease_id]}
  end

  test "server registers itself with destination", context do
    %{supervisor: sup, dest: dest} = context
    assert Registry.keys(Mux.Server, sup) == [dest]
    assert Registry.lookup(Mux.Server, dest) == [{sup, MuxServerProxy}]
    assert stop(context) == {:normal, :normal}
  end

  test "server handles dispatch", context do
    %{client: cli} = context
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "hi", %{}, "world"}}])
    assert_receive {task, :dispatch, {%{}, "hi", %{}, "world"}}
    send(task, {self(), {:ok, %{}, "hello"}})
    assert_receive {^cli, {:packet, _}, {:receive_dispatch, :ok, %{}, "hello"}}
    assert stop(context) == {:normal, :normal}
  end

  @tag :capture_log
  @tag debug: []
  test "server session exits abnormally if busy on close", context do
    %{client: cli} = context
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "hi", %{}, "world"}}])
    assert_receive {task, :dispatch, {%{}, "hi", %{}, "world"}}
    mon = Process.monitor(task)
    assert stop(context) == {:normal, {:tcp_error, :closed}}
    assert_receive {:DOWN, ^mon, _, _, :killed}
  end

  @tag grace: 10
  test "server session shuts down when after grace period", context do
    %{client: cli, server: srv, supervisor: sup} = context
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "hi", %{}, "world"}}])
    assert_receive {task, :dispatch, {%{}, "hi", %{}, "world"}}
    mon = Process.monitor(task)
    Supervisor.stop(sup)
    assert_receive {^srv, :terminate, :shutdown}
    assert_receive {^cli, :terminate, :normal}
    assert_receive {:DOWN, ^mon, _, _, :killed}
  end

  @tag :set_drain
  @tag :capture_log
  test "server drains if alarm already set", context do
    %{client: cli} = context
    assert_receive {^cli, {:packet, tag}, :transmit_drain}
    MuxProxy.commands(cli, [{:send, tag, :receive_drain}])
    assert stop(context) == {:normal, :normal}
  end

  @tag :capture_log
  test "server drains once alarm is set", context do
    %{client: cli, drain_id: alarm_id} = context
    refute_received {^cli, {:packet, _}, :transmit_drain}
    :alarm_handler.set_alarm({alarm_id, self()})
    assert_receive {^cli, {:packet, tag}, :transmit_drain}
    MuxProxy.commands(cli, [{:send, tag, :receive_drain}])
    context = Map.put(context, :set_drain, true)
    assert stop(context) == {:normal, :normal}
  end

  @tag :set_lease
  @tag lease_interval: 1_000
  @tag :capture_log
  test "server sends 0 lease if alarm already set", context do
    %{client: cli} = context
    assert_receive {^cli, {:packet, 0}, {:transmit_lease, :millisecond, 0}}
    assert stop(context) == {:normal, :normal}
  end

  @tag lease_interval: 10
  @tag :capture_log
  test "server stops leasing when alarm set and leases on clear", context do
    %{client: cli, server: srv, lease_id: alarm_id} = context
    assert_receive {^cli, {:packet, 0}, {:transmit_lease, :millisecond, lease}}
    assert lease >= 5
    assert lease <= 15

    :alarm_handler.set_alarm({alarm_id, self()})
    assert Enum.all?(flush_leases(cli),
                     fn lease -> lease >= 5 and lease <= 15 end)

    # no longer have a lease, dispatches nack
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "hi", %{}, "world"}}])
    assert_receive {^srv, :nack, {%{}, "hi", %{}, "world"}}
    send(srv, {self(), {:nack, %{}}})
    assert_receive {^cli, {:packet, _}, {:receive_dispatch, :nack, %{}, ""}}

    :alarm_handler.clear_alarm(alarm_id)

    assert_receive {^cli, {:packet, 0}, {:transmit_lease, :millisecond, lease}}
    assert lease >= 5
    assert lease <= 15

    # got a lease again, dispatches succeed
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "hi", %{}, "world"}}])
    assert_receive {task, :dispatch, {%{}, "hi", %{}, "world"}}
    send(task, {self(), {:ok, %{}, "hello"}})
    assert_receive {^cli, {:packet, _}, {:receive_dispatch, :ok, %{}, "hello"}}

    assert stop(context) == {:normal, :normal}
  end

  defp pair(dest, opts) do
    srv_opts = [port: 0, handshake: {MuxServerProxy.Handshake, self()}] ++ opts
    {:ok, sup} = Mux.Server.start_link(MuxServerProxy, dest, self(), srv_opts)
    [{_, {ip, port}}] = Registry.lookup(Mux.Server.Socket, dest)

    {:ok, cli_sock} = :gen_tcp.connect(ip, port, [active: false])
    cli = MuxProxy.spawn_link(cli_sock, opts)

    # send ping to server so that when response comes back we know the srv
    # process is alive and then we can discover it
    MuxProxy.commands(cli, [{:send, 1, :transmit_ping}])
    assert_receive {^cli, {:packet, 1}, :receive_ping}
    [{srv, _}] = Registry.lookup(Mux.ServerSession, dest)

    # perform a handshake
    MuxProxy.commands(cli, [{:send, 2, {:transmit_init, 1, %{}}}])
    assert_receive {^srv, :handshake, %{}}
    send(srv, {self(), {:ok, %{}, Keyword.get(opts, :session_opts, []), self()}})
    assert_receive {^cli, {:packet, 2}, {:receive_init, 1, %{}}}

    {cli, srv, sup}
  end

  defp stop(%{client: cli, server: srv, supervisor: sup} = context) do
    # parent of sup (and it traps exits) so it will stop normally asynchronously
    Process.exit(sup, :normal)
    # srv should ask us to drain to initiate clean shutdown
    if context[:set_drain] do
      :alarm_handler.clear_alarm(context.drain_id)
    else
      assert_receive {^cli, {:packet, tag}, :transmit_drain}
      MuxProxy.commands(cli, [{:send, tag, :receive_drain}])
    end
    if context[:set_lease] do
      :alarm_handler.clear_alarm(context.lease_id)
    end
    # close writes on client to trigger half close and server should trigger
    # clean close
    {_, %{sock: sock}} = :sys.get_state(cli)
    # :gen_tcp.shutdown/2 blocks until pending writes finished (and receive
    # drain must be in front as sync'ed with cli using :sys after commands)
    :gen_tcp.shutdown(sock, :write)
    assert_receive {^cli, :terminate, cli_reason}
    assert_receive {^srv, :terminate, srv_reason}
    {cli_reason, srv_reason}
  end

  defp flush_leases(cli, acc \\ []) do
    timeout = ExUnit.configuration[:assert_receive_timeout] || 100
    receive do
      {^cli, {:packet, 0}, {:transmit_lease, :millisecond, lease}} ->
        flush_leases(cli, [lease | acc])
    after
      timeout ->
        Enum.reverse(acc)
    end
  end
end

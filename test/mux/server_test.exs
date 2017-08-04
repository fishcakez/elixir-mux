defmodule Mux.ServerTest do
  use ExUnit.Case, async: true

  setup context do
    dest = "#{inspect context[:case]} #{context[:test]}"
    alarm_id = {:drain, dest}
    if context[:set_alarm], do: :alarm_handler.set_alarm({alarm_id, self()})

    opts =
      context
      |> Map.to_list()
      |> Keyword.put_new(:debug, [:log])
      |> Keyword.put_new(:drain_alarms, [alarm_id])
      |> Keyword.put(:session, {MuxServerProxy.Session, self()})
      |> Keyword.put(:presentation, {MuxTest, nil})

    {cli, srv, sup} = pair(dest, opts)

    {:ok, [client: cli, server: srv, supervisor: sup, dest: dest,
           alarm_id: alarm_id]}
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
    assert_receive {task, :dispatch, {%{}, "hi", %{}, %MuxTest{body: "world"}}}
    send(task, {self(), {:ok, %MuxTest{body: "hello"}}})
    assert_receive {^cli, {:packet, _}, {:receive_dispatch, :ok, %{}, "hello"}}
    assert stop(context) == {:normal, :normal}
  end

  @tag :capture_log
  @tag debug: []
  test "server session exits abnormally if busy on close", context do
    %{client: cli} = context
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "hi", %{}, "world"}}])
    assert_receive {task, :dispatch, {%{}, "hi", %{}, %MuxTest{body: "world"}}}
    mon = Process.monitor(task)
    assert stop(context) == {:normal, {:tcp_error, :closed}}
    assert_receive {:DOWN, ^mon, _, _, :killed}
  end

  @tag grace: 10
  test "server session shuts down when after grace period", context do
    %{client: cli, server: srv, supervisor: sup} = context
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "hi", %{}, "world"}}])
    assert_receive {task, :dispatch, {%{}, "hi", %{}, %MuxTest{body: "world"}}}
    mon = Process.monitor(task)
    Supervisor.stop(sup)
    assert_receive {^srv, :terminate, :shutdown}
    assert_receive {^cli, :terminate, :normal}
    assert_receive {:DOWN, ^mon, _, _, :killed}
  end

  @tag :set_alarm
  @tag :capture_log
  test "server drains if alarm already set", context do
    %{client: cli} = context
    assert_receive {^cli, {:packet, tag}, :transmit_drain}
    MuxProxy.commands(cli, [{:send, tag, :receive_drain}])
    assert stop(context) == {:normal, :normal}
  end

  @tag :capture_log
  test "server drains once alarm is set", context do
    %{client: cli, alarm_id: alarm_id} = context
    refute_received {^cli, {:packet, _}, :transmit_drain}
    :alarm_handler.set_alarm({alarm_id, self()})
    assert_receive {^cli, {:packet, tag}, :transmit_drain}
    MuxProxy.commands(cli, [{:send, tag, :receive_drain}])
    context = Map.put(context, :set_alarm, true)
    assert stop(context) == {:normal, :normal}
  end

  defp pair(dest, opts) do
    srv_arg = {[], self()}
    srv_opts = Keyword.put(opts, :port, 0)
    {:ok, sup} = Mux.Server.start_link(MuxServerProxy, dest, srv_arg, srv_opts)
    [{_, {ip, port}}] = Registry.lookup(Mux.Server.Socket, dest)

    {:ok, cli_sock} = :gen_tcp.connect(ip, port, [active: false])
    cli = MuxProxy.spawn_link(cli_sock, opts)

    # send ping to server so that when response comes back we know the srv
    # process is alive and then we can discover it
    MuxProxy.commands(cli, [{:send, 1, :transmit_ping}])
    assert_receive {^cli, {:packet, 1}, :receive_ping}
    [{srv, _}] = Registry.lookup(Mux.Server.Connection, dest)

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
    if context[:set_alarm] do
      :alarm_handler.clear_alarm(context.alarm_id)
    else
      assert_receive {^cli, {:packet, tag}, :transmit_drain}
      MuxProxy.commands(cli, [{:send, tag, :receive_drain}])
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
end

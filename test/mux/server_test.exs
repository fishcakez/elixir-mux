defmodule Mux.ServerTest do
  use ExUnit.Case, async: true

  setup context do
    dest = "#{inspect context[:case]} #{context[:test]}"
    debug = context[:debug] || [:log]
    session_opts = context[:session_opts] || []
    grace = context[:grace] || 5_000
    opts = [debug: debug, session_opts: session_opts, grace: grace]
    {cli, srv, sup} = pair(dest, [port: 0] ++ opts)
    {:ok, [client: cli, server: srv, supervisor: sup, dest: dest]}
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

  defp pair(dest, opts) do
    {:ok, sup} = Mux.Server.start_link(MuxServerProxy, dest, self(), opts)
    children = Supervisor.which_children(sup)
    {_, pool, _, _} = List.keyfind(children, Mux.Server.Pool, 0)
    [{_, {ip, port}, _, _}] = :acceptor_pool.which_sockets(pool)

    {:ok, cli_sock} = :gen_tcp.connect(ip, port, [active: false])
    cli = MuxProxy.spawn_link(cli_sock, opts)

    # send ping to server so that when response comes back we know the srv
    # process is alive and then we can discover it
    MuxProxy.commands(cli, [{:send, 1, :transmit_ping}])
    assert_receive {^cli, {:packet, 1}, :receive_ping}
    [{_, manager, _, _}] = :acceptor_pool.which_children(pool)
    {srv, _} = :sys.get_state(manager)

    # performa handshake
    MuxProxy.commands(cli, [{:send, 2, {:transmit_init, 1, %{}}}])
    assert_receive {^srv, :handshake, %{}}
    send(srv, {self(), {:ok, %{}, Keyword.get(opts, :session_opts, []), self()}})
    assert_receive {^cli, {:packet, 2}, {:receive_init, 1, %{}}}

    {cli, srv, sup}
  end

  defp stop(%{client: cli, server: srv, supervisor: sup}) do
    # parent of sup (and it traps exits) so it will stop normally asynchronously
    Process.exit(sup, :normal)
    # srv should ask us to drain to initiate clean shutdown
    assert_receive {^cli, {:packet, tag}, :transmit_drain}
    MuxProxy.commands(cli, [{:send, tag, :receive_drain}])
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

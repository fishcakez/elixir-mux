defmodule Mux.IntegrationTest do
  use ExUnit.Case, async: true

  setup context do
    debug = context[:debug] || [:log]
    session_opts = context[:session_opts] || []
    opts = [debug: debug, session_opts: session_opts,
            headers: context[:headers]]
    {cli, srv} = pair(opts)
    {:ok, [client: cli, server: srv]}
  end

  test "client dispatch returns ok response", %{client: cli} do
    ctx = %{"a" => "b"}
    dest = "server"
    dest_tab = %{"c" => "d"}
    body = "hello"

    ref = Mux.Client.async_dispatch(cli, ctx, dest, dest_tab, body)
    assert_receive {task, :handle, {^ctx, ^dest, ^dest_tab, ^body}}
    send(task, {self(), {:ok, %{"e" => "f"}, "hi"}})
    assert_receive {^ref, {:ok, %{"e" => "f"}, "hi"}}
  end

  test "client cancel kills task", %{client: cli} do
    ref = Mux.Client.async_dispatch(cli, %{}, "", %{}, "hi")
    assert_receive {task, :handle, {%{}, "", %{}, "hi"}}
    mon = Process.monitor(task)
    assert Mux.Client.cancel(cli, ref) == :ok
    assert_receive {:DOWN, ^mon, _, _, :killed}
  end

  @tag :capture_log
  @tag debug: []
  test "server drain causes close after tasks handled", context do
    %{client: cli, server: srv} = context

    Process.flag(:trap_exit, true)

    ref1 = Mux.Client.async_dispatch(cli, %{}, "dest", %{}, "1")
    ref2 = Mux.Client.async_dispatch(cli, %{}, "dest", %{}, "2")
    assert_receive {task1, :handle, {%{}, "dest", %{}, "1"}}
    assert_receive {task2, :handle, {%{}, "dest", %{}, "2"}}

    Mux.Server.drain(srv)

    # send first after drain to ensure receive_drain arrives before response
    send(task1, {self(), {:ok, %{}, "one"}})
    # draining must have commenced once this is received
    assert_receive {^ref1, {:ok, %{}, "one"}}
    # client nacks all new requests
    ref3 = Mux.Client.async_dispatch(cli, %{}, "", %{}, "drain")
    assert_receive {^cli, :nack, {%{}, "", %{}, "drain"}}
    send(cli, {self(), {:nack, %{"draining" => "sorry"}}})
    assert_receive {^ref3, {:nack, %{"draining" => "sorry"}}}

    send(task2, {self(), {:ok, %{}, "two"}})
    assert_receive {^ref2, {:ok, %{}, "two"}}

    assert_receive {:EXIT, ^cli, {:tcp_error, :closed}}
    assert_receive {:EXIT, ^srv, {:tcp_error, :closed}}
  end

  ## Helpers

  defp pair(opts) do
    parent = self()
    {:ok, l} = :gen_tcp.listen(0, [active: false])
    {:ok, {ip, port}} = :inet.sockname(l)
    cli_task =
      Task.async(fn ->
        {:ok, c} = :gen_tcp.connect(ip, port, [active: false])
        :ok = :gen_tcp.controlling_process(c, parent)
        c
      end)
    srv_task =
      Task.async(fn ->
        {:ok, s} = :gen_tcp.accept(l)
        :gen_tcp.close(l)
        :ok = :gen_tcp.controlling_process(s, parent)
        s
      end)
    cli_sock = Task.await(cli_task)
    srv_sock = Task.await(srv_task)
    :gen_tcp.close(l)

    headers = Keyword.get(opts, :headers)

    cli = MuxClientProxy.spawn_link(cli_sock, headers || %{}, opts)
    srv = MuxServerProxy.spawn_link(srv_sock, opts)

    unless headers do
      assert_receive {^srv, :handshake, %{}}
      session_opts = opts[:session_opts] || []
      send(srv, {self(), {:ok, %{}, session_opts, self()}})
      assert_receive {^cli, :handshake, %{}}
      send(cli, {self(), {:ok, session_opts, self()}})
    end

    {cli, srv}
  end
end

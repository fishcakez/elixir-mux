defmodule Mux.ClientTest do
  use ExUnit.Case, async: true

  setup context do
    debug = context[:debug] || [:log]
    session_opts = context[:session_opts] || []
    opts = [debug: debug, session_opts: session_opts,
            headers: context[:headers]]
    {cli, srv} = pair(opts)
    {:ok, [client: cli, server: srv]}
  end

  test "server ping is ponged", %{server: srv} do
    MuxProxy.commands(srv, [{:send, 1, :transmit_ping}])
    assert_receive {^srv, {:packet, 1}, :receive_ping}
  end

  test "client dispatch returns ok response", %{client: cli, server: srv} do
    task = Task.async(Mux.Client, :sync_dispatch, [cli, %{}, "", %{}, "hello"])

    assert_receive {^srv, {:packet, tag},
      {:transmit_dispatch, %{}, "", %{}, "hello"}}

    ctx = %{"a" => <<>>}
    body = "hello"
    MuxProxy.commands(srv, [{:send, tag, {:receive_dispatch, :ok, ctx, body}}])

    assert Task.await(task) == {:ok, ctx, body}
  end

  test "client dispatch returns app error", %{client: cli, server: srv} do
    task = Task.async(Mux.Client, :sync_dispatch, [cli, %{}, "", %{}, "hello"])

    assert_receive {^srv, {:packet, tag},
      {:transmit_dispatch, %{}, "", %{}, "hello"}}

    msg = "bad!"
    MuxProxy.commands(srv, [{:send, tag, {:receive_dispatch, :error, %{}, msg}}])

    assert Task.await(task) == {:error, %{}, %Mux.ApplicationError{message: msg}}
  end

  test "client dispatch returns server error", %{client: cli, server: srv} do
    task = Task.async(Mux.Client, :sync_dispatch, [cli, %{}, "", %{}, "hello"])

    assert_receive {^srv, {:packet, tag},
      {:transmit_dispatch, %{}, "", %{}, "hello"}}

    msg = "oops!"
    MuxProxy.commands(srv, [{:send, tag, {:receive_error, msg}}])

    assert Task.await(task) == {:error, %Mux.ServerError{message: msg}}
  end

  test "client returns error on unexpected discarded", context do
    %{client: cli, server: srv} = context
    task = Task.async(Mux.Client, :sync_dispatch, [cli, %{}, "", %{}, "hello"])

    assert_receive {^srv, {:packet, tag},
      {:transmit_dispatch, %{}, "", %{}, "hello"}}

    MuxProxy.commands(srv, [{:send, tag, :receive_discarded}])

    assert Task.await(task) == {:error, %Mux.ServerError{message: "server discarded"}}
  end

  test "client dispatch returns nack response", %{client: cli, server: srv} do
    task = Task.async(Mux.Client, :sync_dispatch, [cli, %{}, "", %{}, "hello"])

    assert_receive {^srv, {:packet, tag},
      {:transmit_dispatch, %{}, "", %{}, "hello"}}

    MuxProxy.commands(srv, [{:send, tag, {:receive_dispatch, :nack, %{}, ""}}])

    assert Task.await(task) == {:nack, %{}}
  end

  test "client cancels on dispatch timeout", %{client: cli, server: srv} do
    catch_exit(Mux.Client.sync_dispatch(cli, %{}, "", %{}, "hello", 1))

    assert_receive {^srv, {:packet, tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}

    assert_receive {^srv, {:packet, 0},
      {:transmit_discarded, ^tag, "process discarded"}}
  end

  test "client cancels and ignores ok result", %{client: cli, server: srv} do
    ref = Mux.Client.dispatch(cli, %{}, "", %{}, "hello")
    assert_receive {^srv, {:packet, tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}
    assert Mux.Client.cancel(cli, ref, "test cancels") == :ok
    assert_receive {^srv, {:packet, 0},
      {:transmit_discarded, ^tag, "test cancels"}}

    MuxProxy.commands(srv, [{:send, tag, {:receive_dispatch, :ok, %{}, ""}},
                            {:send, 1, :transmit_ping}])

    assert_receive {^srv, {:packet, 1}, :receive_ping}

    refute_received _
  end

  test "client cancels and ignores nack", %{client: cli, server: srv} do
    ref = Mux.Client.dispatch(cli, %{}, "", %{}, "hello")
    assert_receive {^srv, {:packet, tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}
    assert Mux.Client.cancel(cli, ref, "test cancels") == :ok
    assert_receive {^srv, {:packet, 0},
      {:transmit_discarded, ^tag, "test cancels"}}

    MuxProxy.commands(srv, [{:send, tag, {:receive_dispatch, :nack, %{}, ""}},
                            {:send, 1, :transmit_ping}])

    assert_receive {^srv, {:packet, 1}, :receive_ping}

    refute_received _
  end

  test "client cancels and ignores app error", %{client: cli, server: srv} do
    ref = Mux.Client.dispatch(cli, %{}, "", %{}, "hello")
    assert_receive {^srv, {:packet, tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}
    assert Mux.Client.cancel(cli, ref, "test cancels") == :ok
    assert_receive {^srv, {:packet, 0},
      {:transmit_discarded, ^tag, "test cancels"}}

    MuxProxy.commands(srv, [{:send, tag, {:receive_dispatch, :error, %{}, "bad"}},
                            {:send, 1, :transmit_ping}])

    assert_receive {^srv, {:packet, 1}, :receive_ping}

    refute_received _
  end

  test "client cancels and ignores server error", %{client: cli, server: srv} do
    ref = Mux.Client.dispatch(cli, %{}, "", %{}, "hello")
    assert_receive {^srv, {:packet, tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}
    assert Mux.Client.cancel(cli, ref, "test cancels") == :ok
    assert_receive {^srv, {:packet, 0},
      {:transmit_discarded, ^tag, "test cancels"}}

    MuxProxy.commands(srv, [{:send, tag, {:receive_error, "oops"}},
                            {:send, 1, :transmit_ping}])

    assert_receive {^srv, {:packet, 1}, :receive_ping}

    refute_received _
  end

  test "client cancels on explicit cancel and reuses tag", context do
    %{client: cli, server: srv} = context

    ref = Mux.Client.dispatch(cli, %{}, "", %{}, "hello")
    assert_receive {^srv, {:packet, tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}

    assert Mux.Client.cancel(cli, ref, "test cancels") == :ok
    assert_receive {^srv, {:packet, 0},
      {:transmit_discarded, ^tag, "test cancels"}}

    MuxProxy.commands(srv, [{:send, tag, :receive_discarded},
                            {:send, 1, :transmit_ping}])

    # wait for ping to ensure client has handled discarding
    assert_receive {^srv, {:packet, 1}, :receive_ping}

    ref2 = Mux.Client.dispatch(cli, %{}, "", %{}, "hello")
    assert_receive {^srv, {:packet, ^tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}

    assert Mux.Client.cancel(cli, ref, "test cancels") == {:error, :not_found}
    assert Mux.Client.cancel(cli, ref2, "test cancels again") == :ok
    assert_receive {^srv, {:packet, 0},
      {:transmit_discarded, ^tag, "test cancels again"}}
  end

  @tag session_opts: [session_size: 1]
  test "client nacks when at max sessions", context do
    %{client: cli, server: srv} = context

    ref1 = Mux.Client.dispatch(cli, %{}, "", %{}, "hello")
    assert_receive {^srv, {:packet, tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}

    ref2 = Mux.Client.dispatch(cli, %{}, "", %{}, "nacked")
    assert_receive {^cli, :nack, {%{}, "", %{}, "nacked"}}
    send(cli, {self(), {:nack, %{"busy" => "sorry"}}})
    assert_receive {^ref2, {:nack, %{"busy" => "sorry"}}}

    ref3 = Mux.Client.dispatch(cli, %{}, "", %{}, "nacked again")
    assert_receive {^cli, :nack, {%{}, "", %{}, "nacked again"}}
    send(cli, {self(), {:nack, %{"busy" => "not sorry"}}})
    assert_receive {^ref3, {:nack, %{"busy" => "not sorry"}}}

    MuxProxy.commands(srv, [{:send, tag, {:receive_dispatch, :ok, %{}, "hi"}}])

    assert_receive {^ref1, {:ok, %{}, "hi"}}

    Mux.Client.dispatch(cli, %{}, "", %{}, "hello")
  end

  @tag session_opts: [session_size: 1]
  test "client waits for server discarded before reusing tag", context do
    %{client: cli, server: srv} = context

    ref1 = Mux.Client.dispatch(cli, %{}, "", %{}, "hello")
    assert_receive {^srv, {:packet, tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}

    assert Mux.Client.cancel(cli, ref1, "test cancels") == :ok
    assert_receive {^srv, {:packet, 0},
      {:transmit_discarded, ^tag, "test cancels"}}

    ref2 = Mux.Client.dispatch(cli, %{}, "", %{}, "nacked")
    assert_receive {^cli, :nack, {%{}, "", %{}, "nacked"}}
    send(cli, {self(), {:nack, %{"busy" => "sorry"}}})
    assert_receive {^ref2, {:nack, %{"busy" => "sorry"}}}

    MuxProxy.commands(srv, [{:send, tag, :receive_discarded},
                            {:send, 1, :transmit_ping}])

    # wait for ping to ensure client has handled discarding
    assert_receive {^srv, {:packet, 1}, :receive_ping}
    refute_received {^srv, _, _}

    Mux.Client.dispatch(cli, %{}, "", %{}, "hello")
    assert_receive {^srv, {:packet, ^tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}
  end

  @tag session_opts: [ping_interval: 50]
  test "client pings multiple times", %{server: srv} do
    assert_receive {^srv, {:packet, tag}, :transmit_ping}
    MuxProxy.commands(srv, [{:send, tag, :receive_ping}])
    assert_receive {^srv, {:packet, tag}, :transmit_ping}
    MuxProxy.commands(srv, [{:send, tag, :receive_ping}])
  end

  @tag session_opts: [ping_interval: 10]
  @tag :capture_log
  @tag debug: []
  test "client closes connection if no ping response", context do
    %{client: cli, server: srv} = context
    Process.flag(:trap_exit, true)

    assert_receive {^srv, {:packet, _}, :transmit_ping}

    assert_receive {:EXIT, ^srv, :normal}
    assert_received {^srv, :terminate, :normal}
    assert_receive {:EXIT, ^cli, {:tcp_error, :timeout}}
    assert_received {^cli, :terminate, {:tcp_error, :timeout}}
  end

  test "client updates handler on server lease", %{server: srv, client: cli} do
    MuxProxy.commands(srv, [{:send, 0, {:transmit_lease, :second, 1}}])
    assert_receive {^cli, :lease, {:millisecond, 1000}}
  end

  test "client shutdowns on drain if no exchanges", context do
    %{client: cli, server: srv} = context
    Process.flag(:trap_exit, true)

    MuxProxy.commands(srv, [{:send, 1, :transmit_drain}])
    assert_receive {^cli, :drain, nil}
    send(cli, {self(), {:ok, self()}})
    assert_receive {^srv, {:packet, 1}, :receive_drain}

    assert_receive {:EXIT, ^srv, :normal}
    assert_received {^srv, :terminate, :normal}
    assert_receive {:EXIT, ^cli, :normal}
    assert_received {^cli, :terminate, :normal}
  end

  test "client shutdowns on drain once last exchange responds", context do
    %{client: cli, server: srv} = context
    Process.flag(:trap_exit, true)
    ref1 = Mux.Client.dispatch(cli, %{}, "", %{}, "hello")
    assert_receive {^srv, {:packet, tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}

    MuxProxy.commands(srv, [{:send, 1, :transmit_drain}])
    assert_receive {^cli, :drain, nil}
    send(cli, {self(), {:ok, self()}})
    assert_receive {^srv, {:packet, 1}, :receive_drain}

    # client promised not to send more requests
    ref2 = Mux.Client.dispatch(cli, %{}, "", %{}, "nacked")
    assert_receive {^cli, :nack, {%{}, "", %{}, "nacked"}}
    send(cli, {self(), {:nack, %{"draining" => "sorry"}}})
    assert_receive {^ref2, {:nack, %{"draining" => "sorry"}}}

    MuxProxy.commands(srv, [{:send, tag, {:receive_dispatch, :ok, %{}, "ok"}}])

    assert_receive {^ref1, {:ok, %{}, "ok"}}

    assert_receive {:EXIT, ^srv, :normal}
    assert_received {^srv, :terminate, :normal}
    assert_receive {:EXIT, ^cli, :normal}
    assert_received {^cli, :terminate, :normal}
  end

  test "client sends back error on init", %{server: srv} do
    MuxProxy.commands(srv, [{:send, 1, {:transmit_init, 1, %{}}}])

    assert_receive {^srv, {:packet, 1},
      {:receive_error, "can not handle init"}}
  end

  @tag headers: %{"hello" => "world"}
  test "client completes handshake", %{client: cli, server: srv} do
    assert_receive {^srv, {:packet, tag}, {:transmit_init, 1, %{"hello" => "world"}}}

    # client won't handle dispatches until handshake completes
    ref1 = Mux.Client.dispatch(cli, %{}, "", %{}, "nacked")
    assert_receive {^cli, :nack, {%{}, "", %{}, "nacked"}}
    send(cli, {self(), {:nack, %{"handshake" => "sorry"}}})
    assert_receive {^ref1, {:nack, %{"handshake" => "sorry"}}}

    MuxProxy.commands(srv, [{:send, tag, {:receive_init, 1, %{"hi" => "back"}}}])

    assert_receive {^cli, :handshake, %{"hi" => "back"}}
    send(cli, {self(), {:ok, [], self()}})

    _ = Mux.Client.dispatch(cli, %{}, "", %{}, "hello")
    assert_receive {^srv, {:packet, _}, {:transmit_dispatch, %{}, "", %{}, "hello"}}
  end

  @tag headers: %{"hello" => "world"}
  @tag :capture_log
  @tag debug: []
  test "client exits on error on handshake", %{client: cli, server: srv} do
    Process.flag(:trap_exit, true)
    assert_receive {^srv, {:packet, tag}, {:transmit_init, 1, %{"hello" => "world"}}}

    MuxProxy.commands(srv, [{:send, tag, {:receive_error, "oops"}}])
    assert_receive {^cli, :terminate, %Mux.ServerError{message: "oops"}}
    assert_receive {^srv, :terminate, :normal}
  end

  @tag headers: %{"hello" => "world"}
  test "client handles drain during handshake", context do
    %{client: cli, server: srv} = context

    assert_receive {^srv, {:packet, tag}, {:transmit_init, 1, %{"hello" => "world"}}}
    MuxProxy.commands(srv, [{:send, 2, :transmit_drain},
                            {:send, tag, {:receive_init, 1, %{"hi" => "drain"}}}])

    assert_receive {^cli, :drain, nil}
    send(cli, {self(), {:ok, self()}})
    assert_receive {^srv, {:packet, 2}, :receive_drain}

    assert_receive {^cli, :handshake, %{"hi" => "drain"}}
    send(cli, {self(), {:ok, [], self()}})

    assert_receive {^cli, :terminate, :normal}
    assert_receive {^srv, :terminate, :normal}

    refute_received {^cli, _, _}
    refute_received {^srv, _, _}
  end

  test "client sends back error on transmit dispatch/request", %{server: srv} do
    MuxProxy.commands(srv, [{:send, 1, {:transmit_request, %{}, ""}},
                            {:send, 2, {:transmit_dispatch, %{}, "", %{}, ""}}])

    assert_receive {^srv, {:packet, 1},
      {:receive_error, "can not handle request"}}

    assert_receive {^srv, {:packet, 2},
      {:receive_error, "can not handle dispatch"}}
  end

  @tag :capture_log
  @tag debug: []
  test "client exits abnormally if awaiting dispatch on close", context do
    %{client: cli, server: srv} = context
    Process.flag(:trap_exit, true)

    _ = Mux.Client.dispatch(cli, %{}, "hello", %{}, "world")
    assert_receive {^srv, {:packet, _}, {:transmit_dispatch, %{}, "hello", %{}, "world"}}

    # close srv while cli is awaiting a response
    GenServer.stop(srv)

    assert_receive {:EXIT, ^cli, {:tcp_error, :closed}}
    assert_received {^cli, :terminate, {:tcp_error, :closed}}

    assert_receive {:EXIT, ^srv, :normal}
    assert_received {^srv, :terminate, :normal}
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
    srv = MuxProxy.spawn_link(srv_sock, opts)

    unless headers do
      assert_receive {^srv, {:packet, tag}, {:transmit_init, 1, %{}}}
      MuxProxy.commands(srv, [{:send, tag, {:receive_init, 1, %{}}}])

      assert_receive {^cli, :handshake, %{}}
      send(cli, {self(), {:ok, Keyword.get(opts, :session_opts, []), self()}})
    end

    {cli, srv}
  end
end

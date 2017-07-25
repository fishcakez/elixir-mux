defmodule Mux.ClientTest do
  use ExUnit.Case, async: true

  setup context do
    session_size = context[:session_size] || 32
    debug = context[:debug] || [:log]
    {cli, srv} = pair([debug: debug, session_size: session_size])
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

    assert Task.await(task) == :nack
  end

  test "client cancels on dispatch timeout", %{client: cli, server: srv} do
    catch_exit(Mux.Client.sync_dispatch(cli, %{}, "", %{}, "hello", 1))

    assert_receive {^srv, {:packet, tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}

    assert_receive {^srv, {:packet, 0},
      {:transmit_discarded, ^tag, "process discarded"}}
  end

  test "client cancels and ignores ok result", %{client: cli, server: srv} do
    ref = Mux.Client.async_dispatch(cli, %{}, "", %{}, "hello")
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
    ref = Mux.Client.async_dispatch(cli, %{}, "", %{}, "hello")
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
    ref = Mux.Client.async_dispatch(cli, %{}, "", %{}, "hello")
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
    ref = Mux.Client.async_dispatch(cli, %{}, "", %{}, "hello")
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

    ref = Mux.Client.async_dispatch(cli, %{}, "", %{}, "hello")
    assert_receive {^srv, {:packet, tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}

    assert Mux.Client.cancel(cli, ref, "test cancels") == :ok
    assert_receive {^srv, {:packet, 0},
      {:transmit_discarded, ^tag, "test cancels"}}

    MuxProxy.commands(srv, [{:send, tag, :receive_discarded},
                            {:send, 1, :transmit_ping}])

    # wait for ping to ensure client has handled discarding
    assert_receive {^srv, {:packet, 1}, :receive_ping}

    ref2 = Mux.Client.async_dispatch(cli, %{}, "", %{}, "hello")
    assert_receive {^srv, {:packet, ^tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}

    assert Mux.Client.cancel(cli, ref, "test cancels") == {:error, :not_found}
    assert Mux.Client.cancel(cli, ref2, "test cancels again") == :ok
    assert_receive {^srv, {:packet, 0},
      {:transmit_discarded, ^tag, "test cancels again"}}
  end

  @tag session_size: 1
  test "client nacks when at max sessions", context do
    %{client: cli, server: srv} = context

    ref1 = Mux.Client.async_dispatch(cli, %{}, "", %{}, "hello")
    assert_receive {^srv, {:packet, tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}

    assert Mux.Client.sync_dispatch(cli, %{}, "", %{}, "nacked") == :nack
    ref2 = Mux.Client.async_dispatch(cli, %{}, "", %{}, "nacked")
    assert_receive {^ref2, :nack}

    MuxProxy.commands(srv, [{:send, tag, {:receive_dispatch, :ok, %{}, "hi"}}])

    assert_receive {^ref1, {:ok, %{}, "hi"}}

    Mux.Client.async_dispatch(cli, %{}, "", %{}, "hello")
  end

  @tag session_size: 1
  test "client waits for server discarded before reusing tag", context do
    %{client: cli, server: srv} = context

    ref = Mux.Client.async_dispatch(cli, %{}, "", %{}, "hello")
    assert_receive {^srv, {:packet, tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}

    assert Mux.Client.cancel(cli, ref, "test cancels") == :ok
    assert_receive {^srv, {:packet, 0},
      {:transmit_discarded, ^tag, "test cancels"}}

    assert Mux.Client.sync_dispatch(cli, %{}, "", %{}, "nacked") == :nack

    MuxProxy.commands(srv, [{:send, tag, :receive_discarded},
                            {:send, 1, :transmit_ping}])

    # wait for ping to ensure client has handled discarding
    assert_receive {^srv, {:packet, 1}, :receive_ping}
    refute_received {^srv, _, _}

    Mux.Client.async_dispatch(cli, %{}, "", %{}, "hello")
    assert_receive {^srv, {:packet, ^tag}, {:transmit_dispatch, %{}, "", %{}, "hello"}}
  end

  test "client sends back error on drain/init", %{server: srv} do
    MuxProxy.commands(srv, [{:send, 1, :transmit_drain},
                            {:send, 2, {:transmit_init, 1, %{}}}])

    assert_receive {^srv, {:packet, 1},
      {:receive_error, "can not handle drain"}}

    assert_receive {^srv, {:packet, 2},
      {:receive_error, "can not handle init"}}
  end

  test "client sends back error on transmit dispatch/request", %{server: srv} do
    MuxProxy.commands(srv, [{:send, 1, {:transmit_request, %{}, ""}},
                            {:send, 2, {:transmit_dispatch, %{}, "", %{}, ""}}])

    assert_receive {^srv, {:packet, 1},
      {:receive_error, "can not handle request"}}

    assert_receive {^srv, {:packet, 2},
      {:receive_error, "can not handle dispatch"}}
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

    cli = client_spawn_link(cli_sock, opts)
    srv = MuxProxy.spawn_link(srv_sock, opts)

    {cli, srv}
  end

  defp client_spawn_link(sock, opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :client_init_it, [self(), opts])
    :ok = :gen_tcp.controlling_process(sock, pid)
    send(pid, {self(), sock})
    pid
  end

  def client_init_it(parent, opts) do
    receive do
      {^parent, sock} ->
        Mux.Client.enter_loop(sock, opts)
    end
  end
end

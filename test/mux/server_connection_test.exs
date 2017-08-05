defmodule Mux.Server.ConnectionTest do
  use ExUnit.Case, async: true

  setup context do
    debug = context[:debug] || [:log]
    session_opts = context[:session_opts] || []
    opts = [debug: debug, session_opts: session_opts,
            headers: context[:headers], contexts: context[:contexts] || []]
    {cli, srv} = pair(opts)
    {:ok, [client: cli, server: srv]}
  end

  test "client ping is ponged", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, :transmit_ping}])
    assert_receive {^cli, {:packet, 1}, :receive_ping}
  end

  @tag contexts: [Mux.Deadline]
  test "server dispatch returns ok response", %{client: cli} do
    ctx = %{Mux.Deadline => Mux.Deadline.new(100)}
    wire = Mux.Context.to_wire(ctx)
    dest = "server"
    dest_tab = %{"c" => "d"}
    body = "hello"

    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, wire, dest, dest_tab, body}}])

    assert_receive {task, :dispatch, {^ctx, ^dest, ^dest_tab, ^body}}

    send(task, {self(), {:ok, "hi"}})

    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :ok, %{}, "hi"}}
  end

  test "server dispatch returns nack response", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {task, :dispatch, {%{}, "", %{}, "hi"}}
    send(task, {self(), :nack})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{}, ""}}
  end

  test "server dispatch returns app error response", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {task, :dispatch, {%{}, "", %{}, "hi"}}
    send(task, {self(), {:error, Mux.ApplicationError.exception("oops")}})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :error, %{}, "oops"}}
  end

  test "server dispatch returns server error response", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {task, :dispatch, {%{}, "", %{}, "hi"}}
    send(task, {self(), {:error, Mux.ServerError.exception("oops")}})
    assert_receive {^cli, {:packet, 1},
      {:receive_error, "oops"}}
  end

  @tag :capture_log
  @tag debug: []
  test "server dispatch returns server error on bad return", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {task, :dispatch, {%{}, "", %{}, "hi"}}
    send(task, {self(), :bad_return})
    assert_receive {^cli, {:packet, 1},
      {:receive_error, "process exited"}}
  end

  test "server dispatch returns server error on task exit", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {task, :dispatch, {%{}, "", %{}, "hi"}}
    Process.exit(task, :die)
    assert_receive {^cli, {:packet, 1},
      {:receive_error, "process exited"}}
  end

  test "server handles discarding tag by killing task", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {task, :dispatch, {%{}, "", %{}, "hi"}}

    MuxProxy.commands(cli, [{:send, 0, {:transmit_discarded, 1, "bye!"}}])
    assert_receive {^cli, {:packet, 1}, :receive_discarded}
    refute Process.alive?(task)
  end

  test "server handles discarding tag after response sent", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {task, :dispatch, {%{}, "", %{}, "hi"}}

    send(task, {self(), :nack})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{}, ""}}

    MuxProxy.commands(cli, [{:send, 0, {:transmit_discarded, 1, "bye!"}},
                            {:send, 1, :transmit_ping}])
    assert_receive {^cli, {:packet, 1}, :receive_ping}
  end

  test "server handles client reusing tag", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {task1, :dispatch, {%{}, "", %{}, "hi"}}
    send(task1, {self(), :nack})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{}, ""}}

    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi again"}}])
    assert_receive {task2, :dispatch, {%{}, "", %{}, "hi again"}}
    send(task2, {self(), {:ok, "success"}})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :ok, %{}, "success"}}
  end

  test "server sends response when duplicate tag task responds first", context do
    %{client: cli} = context
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "1"}},
                            {:send, 1, {:transmit_dispatch, %{}, "", %{}, "2"}}])

    assert_receive {task2, :dispatch, {%{}, "", %{}, "2"}}
    send(task2, {self(), :nack})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{}, ""}}

    assert_receive {task1, :dispatch, {%{}, "", %{}, "1"}}
    send(task1, {self(), :nack})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{}, ""}}

    MuxProxy.commands(cli, [{:send, 1, :transmit_ping}])
    assert_receive {^cli, {:packet, 1}, :receive_ping}
  end

  test "server sends response when duplicate tag tasks responds last", context do
    %{client: cli} = context
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "1"}},
                            {:send, 1, {:transmit_dispatch, %{}, "", %{}, "2"}}])

    assert_receive {task1, :dispatch, {%{}, "", %{}, "1"}}
    send(task1, {self(), :nack})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{}, ""}}

    assert_receive {task2, :dispatch, {%{}, "", %{}, "2"}}
    send(task2, {self(), :nack})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{}, ""}}

    MuxProxy.commands(cli, [{:send, 1, :transmit_ping}])
    assert_receive {^cli, {:packet, 1}, :receive_ping}
  end

  test "server handles discarding duplicate tags before a response", context do
    %{client: cli} = context
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "1"}},
                            {:send, 1, {:transmit_dispatch, %{}, "", %{}, "2"}}])

    assert_receive {task1, :dispatch, {%{}, "", %{}, "1"}}
    assert_receive {task2, :dispatch, {%{}, "", %{}, "2"}}

    MuxProxy.commands(cli, [{:send, 0, {:transmit_discarded, 1, "oops"}}])

    assert_receive {^cli, {:packet, 1}, :receive_discarded}
    refute Process.alive?(task1)

    send(task2, {self(), :nack})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{}, ""}}

    MuxProxy.commands(cli, [{:send, 1, :transmit_ping}])
    assert_receive {^cli, {:packet, 1}, :receive_ping}
  end

  test "server responds to drain from client", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, :transmit_drain}])
    assert_receive {^cli, {:packet, 1}, :receive_drain}
  end

  test "server nacks once lease expires", %{client: cli, server: srv} do
    Mux.Server.Connection.lease(srv, 1000, 1)
    assert_receive {^cli, {:packet, 0}, {:transmit_lease, :millisecond, 1}}

    :timer.sleep(20)

    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {^cli, {:packet, 1}, {:receive_dispatch, :nack, %{}, ""}}
  end

  test "server responds with error on request", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_request, %{}, "hi"}}])
    assert_receive {^cli, {:packet, 1}, {:receive_error, "can not handle request"}}
  end

  test "server reponds with error on init", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_init, 1, %{}}}])
    assert_receive {^cli, {:packet, 1}, {:receive_error, "reinitialization not supported"}}
  end

  @tag session_opts: [session_size: 1]
  test "server nacks on max active tasks and allows once below", context do
    %{client: cli} = context
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "1"}},
                            {:send, 2, {:transmit_dispatch, %{}, "", %{}, "2"}}])

    assert_receive {task1, :dispatch, {%{}, "", %{}, "1"}}

    assert_receive {^cli, {:packet, 2},
      {:receive_dispatch, :nack, %{}, ""}}

    send(task1, {self(), {:ok, "hi"}})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :ok, %{}, "hi"}}

    MuxProxy.commands(cli, [{:send, 3, {:transmit_dispatch, %{}, "", %{}, "3"}}])

    assert_receive {task3, :dispatch, {%{}, "", %{}, "3"}}
    send(task3, {self(), {:ok, "hello"}})
    assert_receive {^cli, {:packet, 3},
      {:receive_dispatch, :ok, %{}, "hello"}}
  end

  @tag session_opts: [ping_interval: 50]
  test "server pings multiple times", %{client: cli} do
    assert_receive {^cli, {:packet, tag}, :transmit_ping}
    MuxProxy.commands(cli, [{:send, tag, :receive_ping}])
    assert_receive {^cli, {:packet, tag}, :transmit_ping}
    MuxProxy.commands(cli, [{:send, tag, :receive_ping}])
  end

  @tag session_opts: [ping_interval: 10]
  @tag :capture_log
  @tag debug: []
  test "server closes connection if no ping response", context do
    %{client: cli, server: srv} = context
    Process.flag(:trap_exit, true)

    assert_receive {^cli, {:packet, _}, :transmit_ping}

    assert_receive {:EXIT, ^cli, :normal}
    assert_received {^cli, :terminate, :normal}
    assert_receive {:EXIT, ^srv, {:tcp_error, :timeout}}
    assert_received {^srv, :terminate, {:tcp_error, :timeout}}
  end

  @tag :headers
  test "server handles tinit check", context do
    %{client: cli, server: srv} = context

    init_check = "tinit check"
    MuxProxy.commands(cli, [{:send, 1, {:receive_error, init_check}}])
    assert_receive {^cli, {:packet, _}, {:receive_error, ^init_check}}

    MuxProxy.commands(cli, [{:send, 1, {:transmit_init, 1, %{"hello" => "world"}}}])

    assert_receive {^srv, :handshake, %{"hello" => "world"}}
    send(srv, {self(), {:ok, %{"hi" => "back"}, [], self()}})

    assert_receive {^cli, {:packet, 1}, {:receive_init, 1, %{"hi" => "back"}}}
  end

  @tag :headers
  test "server sends receive error on server error in handshake", context do
    %{client: cli, server: srv} = context

    MuxProxy.commands(cli, [{:send, 1, {:transmit_init, 1, %{"hello" => "world"}}}])

    assert_receive {^srv, :handshake, %{"hello" => "world"}}
    send(srv, {self(), {:error, %Mux.ServerError{message: "nope"}, self()}})

    assert_receive {^cli, {:packet, 1}, {:receive_error, "nope"}}

    MuxProxy.commands(cli, [{:send, 1, {:transmit_init, 1, %{"hello" => "again"}}}])

    assert_receive {^srv, :handshake, %{"hello" => "again"}}
    send(srv, {self(), {:ok, %{"hi" => "back"}, [], self()}})

    assert_receive {^cli, {:packet, 1}, {:receive_init, 1, %{"hi" => "back"}}}
  end

  @tag :headers
  test "server nacks before handshake", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "hello", %{}, "world"}}])
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{}, ""}}
  end

  @tag :capture_log
  @tag debug: []
  test "server exits abnormally if handling dispatch on close", context do
    %{client: cli, server: srv} = context
    Process.flag(:trap_exit, true)

    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "hello", %{}, "world"}}])
    assert_receive {_task, :dispatch, {%{}, "hello", %{}, "world"}}

    # close cli while srv is handling a response
    GenServer.stop(cli)

    assert_receive {:EXIT, ^srv, {:tcp_error, :closed}}
    assert_received {^srv, :terminate, {:tcp_error, :closed}}

    assert_receive {:EXIT, ^cli, :normal}
    assert_received {^cli, :terminate, :normal}
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

    cli = MuxProxy.spawn_link(cli_sock, opts)
    contexts = Keyword.get(opts, :contexts, [])
    srv = MuxServerProxy.spawn_link(srv_sock, contexts, opts)

    unless Keyword.get(opts, :headers) do
      MuxProxy.commands(cli, [{:send, 1, {:transmit_init, 1, %{}}}])

      assert_receive {^srv, :handshake, %{}}
      send(srv, {self(), {:ok, %{}, Keyword.get(opts, :session_opts, []), self()}})

      assert_receive {^cli, {:packet, 1}, {:receive_init, 1, %{}}}
    end

    {cli, srv}
  end
end

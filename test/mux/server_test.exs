defmodule Mux.ServerTest do
  use ExUnit.Case, async: true

  setup context do
    session_size = context[:session_size] || 32
    debug = context[:debug] || [:log]
    {cli, srv} = pair([debug: debug, session_size: session_size])
    {:ok, [client: cli, server: srv]}
  end

  test "client ping is ponged", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, :transmit_ping}])
    assert_receive {^cli, {:packet, 1}, :receive_ping}
  end

  test "server dispatch returns ok response", %{client: cli} do
    ctx = %{"a" => "b"}
    dest = "server"
    dest_tab = %{"c" => "d"}
    body = "hello"

    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, ctx, dest, dest_tab, body}}])

    assert_receive {task, :handle, {^ctx, ^dest, ^dest_tab, ^body}}

    send(task, {self(), {:ok, %{"e" => "f"}, "hi"}})

    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :ok, %{"e" => "f"}, "hi"}}
  end

  test "server dispatch returns nack response", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {task, :handle, {%{}, "", %{}, "hi"}}
    send(task, {self(), {:nack, %{"a" => "b"}}})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{"a" => "b"}, ""}}
  end

  test "server dispatch returns app error response", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {task, :handle, {%{}, "", %{}, "hi"}}
    send(task, {self(), {:error, %{}, Mux.ApplicationError.exception("oops")}})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :error, %{}, "oops"}}
  end

  test "server dispatch returns server error response", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {task, :handle, {%{}, "", %{}, "hi"}}
    send(task, {self(), {:error, Mux.ServerError.exception("oops")}})
    assert_receive {^cli, {:packet, 1},
      {:receive_error, "oops"}}
  end

  @tag :capture_log
  test "server dispatch returns server error on bad return", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {task, :handle, {%{}, "", %{}, "hi"}}
    send(task, {self(), :bad_return})
    assert_receive {^cli, {:packet, 1},
      {:receive_error, "process exited"}}
  end

  test "server dispatch returns server error on task exit", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {task, :handle, {%{}, "", %{}, "hi"}}
    Process.exit(task, :die)
    assert_receive {^cli, {:packet, 1},
      {:receive_error, "process exited"}}
  end

  test "server handles discarding tag by killing task", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {task, :handle, {%{}, "", %{}, "hi"}}

    MuxProxy.commands(cli, [{:send, 0, {:transmit_discarded, 1, "bye!"}}])
    assert_receive {^cli, {:packet, 1}, :receive_discarded}
    refute Process.alive?(task)
  end

  test "server handles discarding tag after response sent", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {task, :handle, {%{}, "", %{}, "hi"}}

    send(task, {self(), {:nack, %{}}})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{}, ""}}

    MuxProxy.commands(cli, [{:send, 0, {:transmit_discarded, 1, "bye!"}},
                            {:send, 1, :transmit_ping}])
    assert_receive {^cli, {:packet, 1}, :receive_ping}
  end

  test "server handles client reusing tag", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi"}}])
    assert_receive {task1, :handle, {%{}, "", %{}, "hi"}}
    send(task1, {self(), {:nack, %{}}})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{}, ""}}

    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "hi again"}}])
    assert_receive {task2, :handle, {%{}, "", %{}, "hi again"}}
    send(task2, {self(), {:ok, %{}, "success"}})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :ok, %{}, "success"}}
  end

  test "server sends response when duplicate tag task responds first", context do
    %{client: cli} = context
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "1"}},
                            {:send, 1, {:transmit_dispatch, %{}, "", %{}, "2"}}])

    assert_receive {task2, :handle, {%{}, "", %{}, "2"}}
    send(task2, {self(), {:nack, %{}}})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{}, ""}}

    assert_receive {task1, :handle, {%{}, "", %{}, "1"}}
    send(task1, {self(), {:nack, %{}}})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{}, ""}}

    MuxProxy.commands(cli, [{:send, 1, :transmit_ping}])
    assert_receive {^cli, {:packet, 1}, :receive_ping}
  end

  test "server sends response when duplicate tag tasks responds last", context do
    %{client: cli} = context
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "1"}},
                            {:send, 1, {:transmit_dispatch, %{}, "", %{}, "2"}}])

    assert_receive {task1, :handle, {%{}, "", %{}, "1"}}
    send(task1, {self(), {:nack, %{}}})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{}, ""}}

    assert_receive {task2, :handle, {%{}, "", %{}, "2"}}
    send(task2, {self(), {:nack, %{}}})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{}, ""}}

    MuxProxy.commands(cli, [{:send, 1, :transmit_ping}])
    assert_receive {^cli, {:packet, 1}, :receive_ping}
  end

  test "server handles discarding duplicate tags before a response", context do
    %{client: cli} = context
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "1"}},
                            {:send, 1, {:transmit_dispatch, %{}, "", %{}, "2"}}])

    assert_receive {task1, :handle, {%{}, "", %{}, "1"}}
    assert_receive {task2, :handle, {%{}, "", %{}, "2"}}

    MuxProxy.commands(cli, [{:send, 0, {:transmit_discarded, 1, "oops"}}])

    assert_receive {^cli, {:packet, 1}, :receive_discarded}
    refute Process.alive?(task1)

    send(task2, {self(), {:nack, %{}}})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :nack, %{}, ""}}

    MuxProxy.commands(cli, [{:send, 1, :transmit_ping}])
    assert_receive {^cli, {:packet, 1}, :receive_ping}
  end

  test "server responds to drain from client", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, :transmit_drain}])
    assert_receive {^cli, {:packet, 1}, :receive_drain}
  end

  test "server responds with error on request", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_request, %{}, "hi"}}])
    assert_receive {^cli, {:packet, 1}, {:receive_error, "can not handle request"}}
  end

  test "server reponds with error on init", %{client: cli} do
    MuxProxy.commands(cli, [{:send, 1, {:transmit_init, 1, %{}}}])
    assert_receive {^cli, {:packet, 1}, {:receive_error, "can not handle init"}}
  end

  @tag session_size: 1
  test "server nacks on max active tasks and allows once below", context do
    %{client: cli} = context
    MuxProxy.commands(cli, [{:send, 1, {:transmit_dispatch, %{}, "", %{}, "1"}},
                            {:send, 2, {:transmit_dispatch, %{}, "", %{}, "2"}}])

    assert_receive {task1, :handle, {%{}, "", %{}, "1"}}
    assert_receive {^cli, {:packet, 2},
      {:receive_dispatch, :nack, %{}, ""}}
    refute_received {_, :handle, _}

    send(task1, {self(), {:ok, %{}, "hi"}})
    assert_receive {^cli, {:packet, 1},
      {:receive_dispatch, :ok, %{}, "hi"}}

    MuxProxy.commands(cli, [{:send, 3, {:transmit_dispatch, %{}, "", %{}, "3"}}])

    assert_receive {task3, :handle, {%{}, "", %{}, "3"}}
    send(task3, {self(), {:ok, %{}, "hello"}})
    assert_receive {^cli, {:packet, 3},
      {:receive_dispatch, :ok, %{}, "hello"}}
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
    srv = MuxServerProxy.spawn_link(srv_sock, opts)

    {cli, srv}
  end
end

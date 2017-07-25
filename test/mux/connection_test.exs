defmodule Mux.ConnectionTest do
  use ExUnit.Case, async: true

  setup context do
    debug = context[:debug] || [:log]
    {cli, srv} = pair([debug: debug])
    {:ok, [client: cli, server: srv]}
  end

  test "ping is received", %{client: cli, server: srv} do
    MuxProxy.commands(cli, [{:send, 1, :transmit_ping}])
    assert_receive {^srv, {:packet, 1}, :transmit_ping}

    MuxProxy.commands(srv, [{:send, 1, :receive_ping}])
    assert_receive {^cli, {:packet, 1}, :receive_ping}
  end

  test "simple batch is received", %{client: cli, server: srv} do
    MuxProxy.commands(cli, [{:send, 1, :transmit_drain},
                         {:send, 2, :receive_drain}])
    assert_receive {^srv, {:packet, 2}, :receive_drain}
    assert_received {^srv, {:packet, 1}, :transmit_drain}
  end

  test "frame size splits and interleaves transmit_dispatch", context do
    %{client: cli, server: srv} = context

    dispatch = {:transmit_dispatch, %{}, "gets", %{}, "split"}

    MuxProxy.commands(cli, [{:frame_size, 2},
                            {:send, 1, dispatch},
                            {:send, 2, dispatch},
                            {:send, 3, {:transmit_lease, :millisecond, 100}}])

    assert_receive {^srv, {:packet, tag1}, packet1}
    assert_receive {^srv, {:packet, tag2}, packet2}
    assert_receive {^srv, {:packet, tag3}, packet3}
    refute_received {^srv, {:packet, _}, _}

    assert packet1 == {:transmit_lease, :millisecond, 100}
    assert tag1 == 3
    assert packet2 == dispatch
    assert tag2 == 1
    assert packet3 == dispatch
    assert tag3 == 2
  end

  test "interleave transmit/receive_dispatch with same tag", context do
    %{client: cli, server: srv} = context

    # transmit_dispatch is longer (more fragments) so finishes later
    transmit_dispatch = {:transmit_dispatch, %{}, "splits", %{}, "and is long"}
    receive_dispatch = {:receive_dispatch, :ok, %{}, "short"}

    MuxProxy.commands(cli, [{:frame_size, 2},
                            {:send, 1, transmit_dispatch},
                            {:send, 1, receive_dispatch},
                            {:send, 2, :transmit_drain}])

    assert_receive {^srv, {:packet, tag1}, packet1}
    assert_receive {^srv, {:packet, tag2}, packet2}
    assert_receive {^srv, {:packet, tag3}, packet3}
    refute_received {^srv, {:packet, _}, _}

    assert packet1 == :transmit_drain
    assert tag1 == 2
    assert packet2 == receive_dispatch
    assert tag2 == 1
    assert packet3 == transmit_dispatch
    assert tag3 == 1
  end

  test "transmit_discarded discards transmit_dispatch", context do
    %{client: cli, server: srv} = context

    # max frame size small, and then split a dispatch greater than it and a
    # discarded so that first fragment is sent, discarded is sent and remainder
    # is discarded then send a similar dispatch. The client should only receive
    # the last dispatch as rest is discarded

    last_dispatch = {:transmit_dispatch, %{}, "will", %{}, "arrive"}
    MuxProxy.commands(cli, [{:frame_size, 2},
                            {:send, 1, {:transmit_dispatch, %{}, "bad", %{}, "b"}},
                            {:send, 0, {:transmit_discarded, 1, "bye!"}},
                            {:send, 2, last_dispatch}])

    assert_receive {^srv, {:packet, 2}, ^last_dispatch}
    refute_received {^srv, {:packet, _}, _}
    assert_receive {^cli, {:packet, 1}, :receive_discarded}
  end

  test "receive_discarded discards receive_dispatch", context do
    %{client: cli, server: srv} = context

    # max frame size small, and then split a dispatch greater than it and a
    # discarded so that first fragment is sent, discarded is sent and remainder
    # is discarded then send a similar dispatch. The client should receive the
    # discarded and last dispatch as first dispatch is discarded

    last_dispatch = {:receive_dispatch, :ok, %{}, "arrives!"}
    MuxProxy.commands(cli, [{:frame_size, 2},
                            {:send, 1, {:receive_dispatch, :ok, %{}, "bad"}},
                            {:send, 1, :receive_discarded},
                            {:send, 2, last_dispatch}])

    assert_receive {^srv, {:packet, 2}, ^last_dispatch}
    assert_received {^srv, {:packet, 1}, :receive_discarded}
    refute_received {^srv, {:packet, _}, _}
  end

  test "receive_error discards receive_dispatch", context do
    %{client: cli, server: srv} = context

    # as previous test but with error instead

    last_dispatch = {:receive_dispatch, :ok, %{}, "arrives!"}
    MuxProxy.commands(cli, [{:frame_size, 2},
                            {:send, 1, {:receive_dispatch, :ok, %{}, "bad"}},
                            {:send, 1, {:receive_error, "oops"}},
                            {:send, 2, last_dispatch}])

    assert_receive {^srv, {:packet, 2}, ^last_dispatch}
    assert_received {^srv, {:packet, 1}, {:receive_error, "oops"}}
    refute_received {^srv, {:packet, _}, _}
  end

  test "transmit_discarded does not discard receive_dispatch", context do
    %{client: cli, server: srv} = context

    MuxProxy.commands(cli, [{:frame_size, 2},
                            {:send, 1, {:receive_dispatch, :ok, %{}, "arrive"}},
                            {:send, 0, {:transmit_discarded, 1, "bye!"}}])

    assert_receive {^srv, {:packet, 1}, {:receive_dispatch, :ok, %{}, "arrive"}}
    assert_receive {^srv, {:packet, 0}, {:transmit_discarded, 1, "bye!"}}
    refute_received {^srv, {:packet, _}, _}
  end

  test "receive_discarded/error does not discard transmit_dispatch", context do
    %{client: cli, server: srv} = context

    transmit_dispatch = {:transmit_dispatch, %{}, "will", %{}, "arrive"}

    MuxProxy.commands(cli, [{:frame_size, 2},
                            {:send, 1, transmit_dispatch},
                            {:send, 1, :receive_discarded},
                            {:send, 1, {:receive_error, "oops"}}])

    assert_receive {^srv, {:packet, 1}, ^transmit_dispatch}
    assert_receive {^srv, {:packet, 1}, :receive_discarded}
    assert_receive {^srv, {:packet, 1}, {:receive_error, "oops"}}
    refute_received {^srv, {:packet, _}, _}
  end

  test "double transmit_discarded discards 2 of 3 transmit_dispatch", context do
    %{client: cli, server: srv} = context

    # ensure discarded ack does not remove another discarded flag for same tag.
    # If it did the second dispatch could send data. The second discarded needs
    # to remove itself before third dispatch tries to send (2nd+) fragments.

    transmit_dispatch = {:transmit_dispatch, %{}, "will", %{}, "arrive"}
    transmit_discarded = {:transmit_discarded, 1, "bye!"}

    MuxProxy.commands(cli, [{:frame_size, 2},
                            {:send, 1, transmit_dispatch},
                            {:send, 0, transmit_discarded},
                            {:send, 1, transmit_dispatch},
                            {:send, 0, transmit_discarded},
                            {:send, 1, transmit_dispatch},
                            {:send, 3, {:transmit_init, 1, %{}}}])

    assert_receive {^srv, {:packet, 3}, {:transmit_init, 1, %{}}}
    assert_receive {^srv, {:packet, 1}, ^transmit_dispatch}
    refute_received {^srv, {:packet, _}, _}
    assert_receive {^cli, {:packet, 1}, :receive_discarded}
    assert_receive {^cli, {:packet, 1}, :receive_discarded}
  end

  test "connection stays activate after many packets", context do
    %{client: cli, server: srv} = context
    n = 1000

    MuxProxy.commands(cli, (for tag <- 1..n, do: {:send, tag, :transmit_ping}))

    for tag <- 1..n do
      assert_receive {^srv, {:packet, ^tag}, :transmit_ping}
    end
  end

  @tag :capture_log
  @tag debug: []
  test "connection detects close", %{client: cli, server: srv} do
    Process.flag(:trap_exit, true)

    GenServer.stop(cli)
    assert_receive {:EXIT, ^cli, :normal}
    assert_received {^cli, :terminate, :normal}

    assert_receive {:EXIT, ^srv, {:tcp_error, :closed}}
    assert_received {^srv, :terminate, {:tcp_error, :closed}}
  end

  @tag :capture_log
  @tag debug: []
  test "connection detects big packet", %{client: cli, server: srv} do
    Process.flag(:trap_exit, true)

    MuxProxy.commands(cli, [{:frame_size, 2}])
    big = :binary.copy("big", 0xFF_FF_FF)
    MuxProxy.commands(srv, [{:send, 0, {:transmit_discarded, 1, big}}])

    assert_receive {:EXIT, ^cli, {:tcp_error, :emsgsize}}
    assert_received {^cli, :terminate, {:tcp_error, :emsgsize}}

    assert_receive {:EXIT, ^srv, {:tcp_error, :closed}}
    assert_received {^srv, :terminate, {:tcp_error, :closed}}
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
    srv = MuxProxy.spawn_link(srv_sock, opts)

    {cli, srv}
  end
end

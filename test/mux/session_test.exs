defmodule Mux.SessionTest do
  use ExUnit.Case, async: true

  setup context do
    debug = context[:debug] || [:log]
    session_opts = context[:session_opts] || []
    opts = [debug: debug, session_opts: session_opts,
            headers: context[:headers], contexts: context[:contexts] || []]
    {cli, srv} = pair(opts)
    {:ok, [client: cli, server: srv]}
  end

  @tag contexts: [Mux.Deadline, Mux.Trace]
  test "client dispatch returns ok response", %{client: cli} do
    dest = "server"
    dest_tab = %{"c" => "d"}
    body = "hello"

    Mux.Deadline.bind(100, fn ->
      Mux.Trace.span([:debug, :sampling_known], fn ->
        %{Mux.Deadline => deadline, Mux.Trace => parent} = Mux.Context.get()
        ref = Mux.Client.Connection.dispatch(cli, dest, dest_tab, body)
        assert_receive {task, :dispatch, {ctx, ^dest, ^dest_tab, ^body}}
        assert %{Mux.Deadline => ^deadline, Mux.Trace => child} = ctx
        assert child.trace_id === parent.trace_id
        assert child.parent_id === parent.span_id
        assert child.span_id !== parent.span_id
        assert Enum.sort(child.flags) === Enum.sort(parent.flags -- [:root])
        send(task, {self(), {:ok, "hi"}})
        assert_receive {^ref, {:ok, "hi"}}
      end)
    end)
  end

  test "client cancel kills task", %{client: cli} do
    ref = Mux.Client.Connection.dispatch(cli, "", %{}, "hi")
    assert_receive {task, :dispatch, {%{}, "", %{}, "hi"}}
    mon = Process.monitor(task)
    assert Mux.Client.Connection.cancel(cli, ref) == :ok
    assert_receive {:DOWN, ^mon, _, _, :killed}
  end

  test "server drain causes close after tasks handled", context do
    %{client: cli, server: srv} = context

    ref1 = Mux.Client.Connection.dispatch(cli, "dest", %{}, "1")
    ref2 = Mux.Client.Connection.dispatch(cli, "dest", %{}, "2")
    assert_receive {task1, :dispatch, {%{}, "dest", %{}, "1"}}
    assert_receive {task2, :dispatch, {%{}, "dest", %{}, "2"}}

    Mux.Server.Connection.drain(srv)
    assert_receive {^cli, :drain, nil}
    send(cli, {self(), {:ok, self()}})

    # send first after drain to ensure receive_drain arrives before response
    send(task1, {self(), {:ok, "one"}})
    # draining must have commenced once this is received
    assert_receive {^ref1, {:ok, "one"}}
    # client nacks all new requests
    ref3 = Mux.Client.Connection.dispatch(cli, "", %{}, "drain")
    assert_receive {^ref3, :nack}

    send(task2, {self(), {:ok, "two"}})
    assert_receive {^ref2, {:ok, "two"}}

    assert_receive {^cli, :terminate, :normal}
    assert_receive {^srv, :terminate, :normal}
  end

  test "server lease causes nacks when expired", context do
    %{client: cli, server: srv} = context

    Mux.Server.Connection.lease(srv, :millisecond, 1)
    assert_receive {^cli, :lease, {:millisecond, 1}}
    send(cli, {self(), {:ok, self()}})

    :timer.sleep(20)

    ref1 = Mux.Client.Connection.dispatch(cli, "dest", %{}, "1")
    assert_receive {^ref1, :nack}

    Mux.Server.Connection.lease(srv, :second, 1)
    assert_receive {^cli, :lease, {:millisecond, 1000}}
    send(cli, {self(), {:ok, self()}})

    ref2 = Mux.Client.Connection.dispatch(cli, "dest", %{}, "2")
    assert_receive {task2, :dispatch, {%{}, "dest", %{}, "2"}}
    send(task2, {self(), {:ok, "lease"}})
    assert_receive {^ref2, {:ok, "lease"}}

    Mux.Server.Connection.lease(srv, :millisecond, 0)
    assert_receive {^cli, :lease, {:millisecond, 0}}
    send(cli, {self(), {:ok, self()}})

    ref3 = Mux.Client.Connection.dispatch(cli, "dest", %{}, "3")
    assert_receive {^ref3, :nack}
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
    contexts = Keyword.get(opts, :contexts)

    cli = MuxClientProxy.spawn_link(cli_sock, headers || %{}, opts)
    srv = MuxServerProxy.spawn_link(srv_sock, contexts, opts)

    unless headers do
      assert_receive {^srv, :handshake, %{}}
      session_opts = opts[:session_opts]
      send(srv, {self(), {:ok, %{}, session_opts, self()}})
      assert_receive {^cli, :handshake, %{}}
      send(cli, {self(), {:ok, session_opts, self()}})
    end

    {cli, srv}
  end
end

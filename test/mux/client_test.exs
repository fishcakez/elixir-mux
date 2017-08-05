defmodule Mux.ClientTest do
  use ExUnit.Case, async: true

  setup context do
    dest = "#{inspect context[:case]} #{context[:test]}"
    alarm_id = {:drain, dest}
    if context[:set_alarm], do: :alarm_handler.set_alarm({alarm_id, self()})

    opts =
      context
      |> Map.to_list()
      |> Keyword.put_new(:debug, [:log])
      |> Keyword.put_new(:clients, 1)
      |> Keyword.put_new(:drain_alarms, [alarm_id])

    {cli_info, srv_info} = pairs(dest, opts)

    {sups, clis} = Enum.unzip(cli_info)
    {lsocks, srvs} = Enum.unzip(srv_info)

    {:ok, [clients: clis, servers: srvs, supervisors: sups, listen: lsocks,
           dest: dest, alarm_id: alarm_id, opts: opts]}
  end

  test "client registers itself with destination", context do
    %{clients: [cli], dest: dest} = context
    assert Registry.keys(Mux.Client.Connection, cli) == [dest]
    assert Registry.lookup(Mux.Client.Connection, dest) == [{cli, {MuxTest, nil}}]
    assert stop(context) == [{:normal, :normal}]
  end

  @tag clients: 2
  test "whereis randomly (eventually) shows multiple clients", context do
    %{clients: clients, dest: dest} = context

    unique_clients =
      dest
      |> Stream.unfold(fn acc -> {Mux.Client.whereis(acc, %{}), acc} end)
      |> Stream.uniq()
      |> Enum.take(2)

    assert Enum.sort(unique_clients) == Enum.sort(clients)
    assert stop(context) == [{:normal, :normal}, {:normal, :normal}]
  end

  test "client handles dispatch", context do
    %{servers: [srv], dest: dest} = context
    req = %MuxTest{body: "hello"}
    task = Task.async(Mux.Client, :sync_dispatch, [dest, %{"a" => "b"}, req])
    assert_receive {^srv, {:packet, tag},
      {:transmit_dispatch, %{}, ^dest, %{"a" => "b"}, "hello"}}
    ctx = %{"hi" => "world"}
    body = "success!"
    MuxProxy.commands(srv, [{:send, tag, {:receive_dispatch, :ok, ctx, body}}])
    assert Task.await(task) == {:ok, %MuxTest{body: body}}
    assert stop(context) == [{:normal, :normal}]
  end

  @tag clients: 0
  test "whereis returns nil if no clients", %{dest: dest} = context do
    assert Mux.Client.whereis(dest, %{}) == nil
    assert stop(context) == []
  end

  @tag clients: 1
  test "whereis returns nil if no clients have lease", context do
    %{servers: [srv], dest: dest} = context
    MuxProxy.commands(srv, [{:send, 0, {:transmit_lease, 1000, 0}},
                            {:send, 1, :transmit_ping}])
    assert_receive {^srv, {:packet, 1}, :receive_ping}
    assert Mux.Client.whereis(dest, %{}) == nil
    assert stop(context) == [{:normal, :normal}]
  end

  @tag clients: 0
  test "dispatch returns nack if no clients", %{dest: dest} = context do
    assert Mux.Client.sync_dispatch(dest, %{}, %MuxTest{body: "hi"}) == :nack
    assert stop(context) == []
  end

  @tag :capture_log
  test "client reconnects", context do
    %{clients: [cli1], servers: [srv1], listen: [lsock], opts: opts} = context

    MuxProxy.commands(srv1, [{:send, 1, :transmit_drain}])
    assert_receive {^srv1, {:packet, 1}, :receive_drain}

    assert_receive {^cli1, :terminate, :normal}
    assert_receive {^srv1, :terminate, :normal}

    assert {:ok, srv_sock2} = :gen_tcp.accept(lsock)
    srv2 = MuxProxy.spawn_link(srv_sock2, opts)

    assert_receive {^srv2, {:packet, tag}, {:transmit_init, 1, %{}}}
    MuxProxy.commands(srv2, [{:send, tag, {:receive_init, 1, %{}}}])
    assert_receive {cli2, :handshake, %{}}
    send(cli2, {self(), {:ok, Keyword.get(opts, :session_opts, []), self()}})

    assert stop(%{context | clients: [cli2], servers: [srv2]}) ==
      [{:normal, :normal}]
  end

  test "client shuts down once last exchange responds", context do
    %{clients: [cli], servers: [srv], dest: dest, supervisors: [sup]} = context
    task1 = Task.async(Mux.Client, :sync_dispatch, [dest, %{}, %MuxTest{body: "hello"}])
    assert_receive {^srv, {:packet, tag}, {:transmit_dispatch, %{}, ^dest, %{}, "hello"}}

    # parent of supervisor and it traps so will exit normally
    Process.exit(sup, :normal)

    # check socket still works
    MuxProxy.commands(srv, [{:send, 1, :transmit_ping}])
    assert_receive {^srv, {:packet, 1}, :receive_ping}

    # client session not going to send more requests
    task2 = Task.async(Mux.Client.Connection, :sync_dispatch, [cli, dest, %{}, "nacked"])
    assert Task.await(task2) == :nack

    # client session not registered anymore!
    assert Registry.lookup(Mux.Client.Connection, dest) == []

    MuxProxy.commands(srv, [{:send, tag, {:receive_dispatch, :ok, %{}, "ok"}}])
    assert Task.await(task1) == {:ok, %MuxTest{body: "ok"}}

    assert_receive {^cli, :terminate, :normal}
    assert_receive {^srv, :terminate, :normal}
  end

  @tag :set_alarm
  @tag :capture_log
  @tag :skip_accept
  @tag connect_interval: 50
  test "client doesn't connect if alarm already set", context do
    %{servers: [srv_task], alarm_id: alarm_id, opts: opts} = context
    refute Task.yield(srv_task, 200)
    :alarm_handler.clear_alarm(alarm_id)
    srv_sock = Task.await(srv_task, 100)
    srv = MuxProxy.spawn_link(srv_sock, opts)
    assert_receive {^srv, {:packet, tag}, {:transmit_init, 1, %{}}}
    MuxProxy.commands(srv, [{:send, tag, {:receive_init, 1, %{}}}])
    assert_receive {cli, :handshake, %{}}
    send(cli, {self(), {:ok, [], self()}})

    assert stop(%{context | servers: [srv], clients: [cli]}) == [{:normal, :normal}]
  end

  @tag :capture_log
  @tag connect_interval: 50
  test "client drains and reconnects once alarm clears", context do
    %{clients: [cli1], servers: [srv1], listen: [lsock],
      alarm_id: alarm_id, opts: opts} = context

    :alarm_handler.set_alarm({alarm_id, self()})

    assert_receive {^cli1, :terminate, :normal}
    assert_receive {^srv1, :terminate, :normal}

    assert :gen_tcp.accept(lsock, 100) == {:error, :timeout}

    :alarm_handler.clear_alarm(alarm_id)
    assert {:ok, srv_sock2} = :gen_tcp.accept(lsock, 100)
    srv2 = MuxProxy.spawn_link(srv_sock2, opts)

    assert_receive {^srv2, {:packet, tag}, {:transmit_init, 1, %{}}}
    MuxProxy.commands(srv2, [{:send, tag, {:receive_init, 1, %{}}}])
    assert_receive {cli2, :handshake, %{}}
    send(cli2, {self(), {:ok, Keyword.get(opts, :session_opts, []), self()}})

    assert stop(%{context | clients: [cli2], servers: [srv2]}) ==
      [{:normal, :normal}]
  end

  defp pairs(dest, opts) do
    case opts[:clients] do
      0 ->
        {[], []}
      n ->
        1..n
        |> Enum.reduce([], fn _, pairs -> pair(dest, pairs, opts) end)
        |> Enum.unzip()
    end
  end

  defp pair(dest, pairs, opts) do
    parent = self()
    {:ok, l} = :gen_tcp.listen(0, [active: false])
    {:ok, {ip, port}} = :inet.sockname(l)
    srv_task =
      Task.async(fn ->
        {:ok, s} = :gen_tcp.accept(l)
        :ok = :gen_tcp.controlling_process(s, parent)
        s
      end)

    cli_opts =
      opts
      |> Keyword.put(:address, ip)
      |> Keyword.put(:port, port)
      |> Keyword.put(:session, {MuxClientProxy.Session, {%{}, self()}})
      |> Keyword.put(:presentation, {MuxTest, nil})

    {:ok, sup} = Mux.Client.start_link(dest, cli_opts)

    if opts[:skip_accept] do
      [{{sup, nil}, {l, srv_task}} | pairs]
    else
      srv_sock = Task.await(srv_task)

      srv = MuxProxy.spawn_link(srv_sock, opts)
      assert_receive {^srv, {:packet, tag}, {:transmit_init, 1, %{}}}
      MuxProxy.commands(srv, [{:send, tag, {:receive_init, 1, %{}}}])
      assert_receive {cli, :handshake, %{}}
      send(cli, {self(), {:ok, Keyword.get(opts, :session_opts, []), self()}})
      # sync with client session to make sure its finished handshake calls
      :sys.get_state(cli)

      [{{sup, cli}, {l, srv}} | pairs]
    end
  end

  defp stop(%{clients: clis, servers: srvs, listen: lsocks}) do
    for lsock <- lsocks, do: :gen_tcp.close(lsock)
    for {cli, srv} <- Enum.zip(clis, srvs) do
      MuxProxy.commands(srv, [{:send, 1, :transmit_drain}])
      assert_receive {^srv, {:packet, 1}, :receive_drain}

      assert_receive {^cli, :terminate, cli_reason}
      assert_receive {^srv, :terminate, srv_reason}
      {cli_reason, srv_reason}
    end
  end
end

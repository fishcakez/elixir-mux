defmodule Mux.ClientTest do
  use ExUnit.Case, async: true

  setup context do
    dest = "#{inspect context[:case]} #{context[:test]}"

    opts =
      context
      |> Map.to_list()
      |> Keyword.put_new(:debug, [:log])
      |> Keyword.put_new(:clients, 1)

    {cli_info, srv_info} = pairs(dest, opts)

    {sups, clis} = Enum.unzip(cli_info)
    {lsocks, srvs} = Enum.unzip(srv_info)

    {:ok, [clients: clis, servers: srvs, supervisors: sups, listen: lsocks,
           dest: dest, opts: opts]}
  end

  test "client registers itself with destination", context do
    %{clients: [cli], dest: dest} = context
    assert Registry.keys(Mux.ClientSession, cli) == [dest]
    assert Registry.lookup(Mux.ClientSession, dest) == [{cli, MuxClientProxy}]
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
    task = Task.async(Mux.Client, :sync_dispatch, [dest, %{"a" => "b"}, "hello"])
    assert_receive {^srv, {:packet, tag},
      {:transmit_dispatch, %{}, ^dest, %{"a" => "b"}, "hello"}}
    ctx = %{"hi" => "world"}
    body = "success!"
    MuxProxy.commands(srv, [{:send, tag, {:receive_dispatch, :ok, ctx, body}}])
    assert Task.await(task) == {:ok, body}
    assert stop(context) == [{:normal, :normal}]
  end

  @tag clients: 0
  test "whereis returns nil if no clients", %{dest: dest} = context do
    assert Mux.Client.whereis(dest, %{}) == nil
    assert stop(context) == []
  end

  @tag clients: 0
  test "dispatch exits with noproc if no clients", %{dest: dest} = context do
    assert {:noproc, _} = catch_exit(Mux.Client.sync_dispatch(dest, %{}, "hi"))
    assert stop(context) == []
  end

  test "dispatch honours deadline", %{dest: dest, servers: [srv]} = context do
    Mux.Deadline.bind(0, fn ->
      assert {:timeout, _} = catch_exit(Mux.Client.sync_dispatch(dest, %{}, "hi"))
    end)
    assert_receive {^srv, {:packet, tag},
      {:transmit_dispatch, %{"com.twitter.finagle.Deadline" => _}, ^dest, %{}, "hi"}}
    assert_receive {^srv, {:packet, 0}, {:transmit_discarded, ^tag, _}}
    MuxProxy.commands(srv, [{:send, tag, :receive_discarded}])
    assert stop(context) == [{:normal, :normal}]
  end

  @tag :reconnect
  @tag :capture_log
  test "client reconnects", context do
    %{clients: [cli1], servers: [srv1], listen: [lsock], opts: opts} = context

    MuxProxy.commands(srv1, [{:send, 1, :transmit_drain}])
    assert_receive {^cli1, :drain, nil}
    send(cli1, {self(), {:ok, self()}})
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
    task1 = Task.async(Mux.Client, :sync_dispatch, [dest, %{}, "hello"])
    assert_receive {^srv, {:packet, tag}, {:transmit_dispatch, %{}, ^dest, %{}, "hello"}}

    # parent of supervisor and it traps so will exit normally
    Process.exit(sup, :normal)

    assert_receive {^cli, :drain, nil}
    send(cli, {self(), {:ok, self()}})

    # check socket still works
    MuxProxy.commands(srv, [{:send, 1, :transmit_ping}])
    assert_receive {^srv, {:packet, 1}, :receive_ping}

    # client not going to send more requests
    task2 = Task.async(Mux.Client, :sync_dispatch, [dest, %{}, "nacked"])
    assert Task.await(task2) == :nack

    MuxProxy.commands(srv, [{:send, tag, {:receive_dispatch, :ok, %{}, "ok"}}])
    assert Task.await(task1) == {:ok, "ok"}

    assert_receive {^cli, :terminate, :normal}
    assert_receive {^srv, :terminate, :normal}
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
      |> Keyword.put(:handshake, {MuxClientProxy.Handshake, {%{}, self()}})

    {:ok, sup} = Mux.Client.start_link(MuxClientProxy, dest, self(), cli_opts)
    srv_sock = Task.await(srv_task)

    srv = MuxProxy.spawn_link(srv_sock, opts)
    assert_receive {^srv, {:packet, tag}, {:transmit_init, 1, %{}}}
    MuxProxy.commands(srv, [{:send, tag, {:receive_init, 1, %{}}}])
    assert_receive {cli, :handshake, %{}}
    send(cli, {self(), {:ok, Keyword.get(opts, :session_opts, []), self()}})

    [{{sup, cli}, {l, srv}} | pairs]
  end

  defp stop(%{clients: clis, servers: srvs, listen: lsocks}) do
    for lsock <- lsocks, do: :gen_tcp.close(lsock)
    for {cli, srv} <- Enum.zip(clis, srvs) do
      MuxProxy.commands(srv, [{:send, 1, :transmit_drain}])
      assert_receive {^cli, :drain, nil}
      send(cli, {self(), {:ok, self()}})
      assert_receive {^srv, {:packet, 1}, :receive_drain}

      assert_receive {^cli, :terminate, cli_reason}
      assert_receive {^srv, :terminate, srv_reason}
      {cli_reason, srv_reason}
    end
  end
end

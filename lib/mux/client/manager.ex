defmodule Mux.Client.Manager do
  @moduledoc false

  @behaviour :gen_statem

  @connect_timeout 5_000
  @connect_interval 5_000
  @connect_options [active: false]

  defmodule Data do
    @moduledoc false

    @enforce_keys [:dest, :address, :port, :socket_opt, :timeout, :interval,
                   :supervisor, :monitor]
    defstruct [:dest, :address, :port, :socket_opt, :timeout, :interval,
               :supervisor, :monitor]
  end

  @type address :: :inet.sock_address() | :inet.hostname()
  @type option ::
    {:socket_opt, [:gen_tcp.connect_option]} |
    {:connect_timeout, timeout} |
    {:connect_interval, timeout}

  @spec child_spec({Mux.Packet.dest, address, :inet.port_number, option}) ::
    Supervisor.child_spec
  def child_spec({dest, addr, port, opts}) do
    %{id: {dest, __MODULE__},
      start: {__MODULE__, :start_link, [dest, addr, port, opts]},
      type: :worker}
  end

  def start_link(dest, addr, port, opts) do
    parent = self()
    :gen_statem.start_link(__MODULE__, {dest, parent, addr, port, opts}, [])
  end

  def init({dest, sup, addr, port, opts}) do
    sock_opt = Keyword.get(opts, :socket_opt, [])
    timeout = Keyword.get(opts, :connect_timeout, @connect_timeout)
    interval = Keyword.get(opts, :connect_interval, @connect_interval)
    data = %Data{dest: dest, address: addr, port: port, socket_opt: sock_opt,
                 timeout: timeout, interval: interval, supervisor: sup,
                 monitor: nil}
    {:ok, :init, data, [{:next_event, :internal, :init}]}
  end

  def callback_mode(),
    do: :state_functions

  def init(:internal, :init, %Data{dest: dest, supervisor: sup} = data) do
    # get pool process to start sessions under
    children = Supervisor.which_children(sup)
    {_, pool, _, _} = List.keyfind(children, {dest, Mux.Client.Pool}, 0)
    {:next_state, :closed, %Data{data | supervisor: pool}, connect()}
  end

  def open(:info, {:DOWN, mon, _, _, reason}, %Data{monitor: mon} = data) do
    case reason do
      :normal ->
        {:next_state, :closed, %Data{data | monitor: nil}, connect()}
      {:tcp_error, _} ->
        {:next_state, :closed, %Data{data | monitor: nil}, connect()}
      reason ->
        {:stop, {:shutdown, reason}}
    end
  end
  def open(:info, msg, data),
    do: info(msg, data)

  def closed(_, {:connect, _}, data),
    do: handle_connect(data)

  def closed(:info, msg, data),
    do: info(msg, data)

  def terminate(_, _, _),
    do: :ok

  ## Helpers

  defp info(msg, _) do
    :error_logger.error_msg("#{__MODULE__} received unexpected message: ~p~n",
                            [msg])
    :keep_state_and_data
  end

  def connect(),
    do: [{:next_event, :internal, {:connect, 0}}]

  defp handle_connect(data) do
    %Data{address: addr, port: port, socket_opt: opts, timeout: timeout,
          supervisor: sup, interval: interval} = data
    case :gen_tcp.connect(addr, port, @connect_options ++ opts, timeout) do
      {:ok, sock} ->
        mon = Mux.Client.Pool.start_session(sup, sock)
        {:next_state, :open, %Data{data | monitor: mon}}
      {:error, _} ->
        backoff = rand_interval(interval)
        {:keep_state_and_data, [{:state_timeout, backoff, {:connect, backoff}}]}
    end
  end

  defp rand_interval(interval) do
    # randomise uniform around [0.5 interval, 1.5 interval]
    div(interval, 2) + :rand.uniform(interval + 1) - 1
  end
end

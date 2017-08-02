defmodule Mux.Context do
  @moduledoc """
  Context for Mux dispatches.

  This module defines binding and reading operations for a context. Access to
  the values exists in the scope of the anonymous function for the calling
  process. The context values can be automatically sent downstream if a dispatch
  is made inside `bind/2`, `bind/3` or `bind_wire/3`. The keys must be modules
  that implement the `Mux.Context` behaviour.
  """

  @type t :: %{optional(module) => term}
  @type wire :: %{optional(String.t) => binary}

  @callback put_wire(wire, term) :: wire
  @callback fetch_wire(wire) :: {:ok, term} | :error

  @doc """
  Bind a module's value inside the scope of a fun and run the fun.

  ## Examples

      iex> Mux.Context.bind(MyMod, "hi", fn -> Mux.Context.get(MyMod) end)
      "hi"
  """
  @spec bind(module, term, (() -> result)) :: result when result: var
  def bind(module, term, fun) do
    case :erlang.get(__MODULE__) do
      %{} = ctx ->
        ctx
        |> Map.put(module, term)
        |> bind_put(fun, ctx)
      _ ->
        bind_delete(%{module => term}, fun)
    end
  end

  @doc """
  Bind the modules' values inside the scope of a fun and run the fun.

  ## Examples

      iex> Mux.Context.bind(%{MyMod => 1}, fn -> Mux.Context.get(MyMod) end)
      1
  """
  @spec bind(t, (() -> result)) :: result when result: var
  def bind(ctx, fun) when ctx == %{},
    do: fun.()
  def bind(ctx, fun) do
    case :erlang.get(__MODULE__) do
      %{} = old_ctx ->
        old_ctx
        |> Map.merge(ctx)
        |> bind_put(fun, old_ctx)
      _ ->
        bind_delete(ctx, fun)
    end
  end

  @doc """
  Get the current context.

  ## Examples

      iex> Mux.Context.bind(MyMod, nil, fn -> Mux.Context.get() end)
      %{MyMod => nil}
  """
  @spec get() :: t
  def get() do
    case :erlang.get(__MODULE__) do
      %{} = ctx ->
        ctx
      _ ->
        %{}
    end
  end

  @doc """
  Get the module's value in the current context.

  ## Examples

      iex> Mux.Context.bind(MyMod, "hi", fn -> Mux.Context.get(MyMod) end)
      "hi"
  """
  @spec get(module, term) :: term
  def get(module, default \\ nil) do
    case :erlang.get(__MODULE__) do
      %{^module => value} ->
        value
      _ ->
        default
    end
  end

  @doc """
  Fetch the module's value in the current context.

  ## Examples

      iex> Mux.Context.bind(MyMod, "hi", fn -> Mux.Context.fetch(MyMod) end)
      {:ok, "hi"}
  """
  @spec fetch(module) :: {:ok, term} | :error
  def fetch(module) do
    case :erlang.get(__MODULE__) do
      %{^module => value} ->
        {:ok, value}
      _ ->
        :error
    end
  end

  @doc """
  Fetch the module's value in the current context.

  ## Examples

      iex> Mux.Context.bind(MyMod, "hi", fn -> Mux.Context.fetch!(MyMod) end)
      "hi"
  """
  @spec fetch!(module) :: term
  def fetch!(module) do
    case :erlang.get(__MODULE__) do
      %{^module => value} ->
        value
      _ ->
        raise KeyError, key: module
    end
  end

  @doc """
  Check whether a module has a value in the current context.

  ## Examples

      iex> Mux.Context.bind(MyMod, "hi", fn -> Mux.Context.has_key?(MyMod) end)
      true
  """
  @spec has_key?(module) :: boolean
  def has_key?(module) do
    case :erlang.get(__MODULE__) do
       %{^module => _} ->
        true
      _ ->
        false
    end
  end

  @doc """
  Get a list of modules in the current context.

  ## Examples

      iex> Mux.Context.bind(MyMod, "hi", fn -> Mux.Context.keys() end)
      [MyMod]
  """
  @spec keys() :: [module]
  def keys() do
    case :erlang.get(__MODULE__) do
      %{} = ctx ->
        Map.keys(ctx)
      _ ->
        []
    end
  end

  @doc """
  Unbind the module or modules' values in the scope of a fun and run the fun.

  ## Examples

      iex> Mux.Context.unbind(MyMod, fn -> Mux.Context.fetch(MyMod) end)
      :error
  """
  @spec unbind([module] | module, (() -> result)) :: result when result: var
  def unbind([], fun),
    do: fun.()
  def unbind([_|_] = modules, fun) do
    case :erlang.get(__MODULE__) do
      %{} = old_ctx ->
        old_ctx
        |> Map.drop(modules)
        |> bind_put(fun, old_ctx)
      _ ->
        fun.()
    end
  end
  def unbind(module, fun) do
    case :erlang.get(__MODULE__) do
      %{} = old_ctx ->
        old_ctx
        |> Map.delete(module)
        |> bind_put(fun, old_ctx)
      _ ->
        fun.()
    end
  end

  @doc """
  Unbind all modules' values in the scope of a fun and run the fun.

  ## Examples

      iex> Mux.Context.unbind(fn -> Mux.Context.get() end)
      %{}
  """
  @spec unbind((() -> result)) :: result when result: var
  def unbind(fun) do
    case :erlang.erase(__MODULE__) do
      %{} = old_ctx ->
        bind_put(fun, old_ctx)
      _ ->
        fun.()
    end
  end

  @doc """
  Convert the context to wire context.

  ## Examples

      Mux.Context.bind(MyMod, "hi", fn -> Mux.Context.to_wire() end)
      %{"com.example.my_mod" => "hello"}
  """
  @spec to_wire(t) :: wire
  def to_wire(ctx \\ get()),
    do: Enum.reduce(ctx, %{}, &put_wire/2)

  @doc """
  Bind module's values from wire context in the scope of a fun and run then fun.

  ## Examples

      wire = %{"com.example.my_mod" => "hello"}
      Mux.Context.bind_wire(wire, fn -> Mux.Context.fetch!(MyMod) end)
      "hi"
  """
  @spec bind_wire(wire, [module], (() -> result)) :: result when result: var
  def bind_wire(wire, modules, fun) do
    modules
    |> Enum.reduce(%{}, &fetch_wire(&1, wire, &2))
    |> bind(fun)
  end

  ## Helpers

  defp bind_put(ctx, fun, old_ctx) do
    _ = :erlang.put(__MODULE__, ctx)
    try do
      fun.()
    after
      _ = :erlang.put(__MODULE__, old_ctx)
    end
  end

  defp bind_put(fun, old_ctx) do
    fun.()
  after
    _ = :erlang.put(__MODULE__, old_ctx)
  end

  defp bind_delete(ctx, fun) do
    _ = :erlang.put(__MODULE__, ctx)
    try do
      fun.()
    after
      _ = :erlang.erase(__MODULE__)
    end
  end

  defp put_wire({mod, val}, wire),
    do: apply(mod, :put_wire, [wire, val])

  defp fetch_wire(mod, wire, ctx) do
    case apply(mod, :fetch_wire, [wire]) do
      {:ok, val} ->
        Map.put(ctx, mod, val)
      :error ->
        ctx
    end
  end
end

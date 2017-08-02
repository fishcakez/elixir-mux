defmodule Mux.ContextTest do
  use ExUnit.Case, async: true

  doctest Mux.Context

  @behaviour Mux.Context

  def put_wire(wire, int),
    do: Map.put(wire, "com.example.int", <<int::64>>)

  def fetch_wire(%{"com.example.int" => data}),
    do: {:ok, :binary.decode_unsigned(data)}
  def fetch_wire(_),
    do: :error

  test "bind value sets value" do
    Mux.Context.bind(Mux.ContextTest, 1, fn ->
      assert Mux.Context.get() == %{Mux.ContextTest => 1}
    end)

    assert Mux.Context.get() == %{}
  end

  test "merge bindings in nested bind value" do
    Mux.Context.bind(Mux.ContextTest, 1, fn ->
      Mux.Context.bind(MyMod, 2, fn ->
        assert Mux.Context.get() == %{Mux.ContextTest => 1, MyMod => 2}
      end)

      assert Mux.Context.get() == %{Mux.ContextTest => 1}
    end)
  end

  test "bind context sets context" do
    Mux.Context.bind(%{Mux.ContextTest => 1}, fn ->
      assert Mux.Context.get() == %{Mux.ContextTest => 1}
    end)

    assert Mux.Context.get(Mux.ContextTest, :nope) == :nope
  end

  test "merge bindings in nested bind context" do
    Mux.Context.bind(%{Mux.ContextTest => 1}, fn ->
      Mux.Context.bind(%{MyMod => 2}, fn ->
        assert Mux.Context.get() == %{Mux.ContextTest => 1, MyMod => 2}
      end)

      assert Mux.Context.fetch!(Mux.ContextTest) == 1
    end)
  end

  test "unbind module is unbound in nested context" do
    Mux.Context.bind(%{Mux.ContextTest => 1}, fn ->
      Mux.Context.unbind(Mux.ContextTest, fn ->
        refute Mux.Context.has_key?(Mux.ContextTest)
      end)

      assert Mux.Context.fetch(Mux.ContextTest) == {:ok, 1}
    end)
  end

  test "unbind modules are unbound in nested context" do
    Mux.Context.bind(%{Mux.ContextTest => 1, MyMod => 2}, fn ->
      Mux.Context.unbind([Mux.ContextTest], fn ->
        assert Mux.Context.keys() == [MyMod]
      end)

      assert Mux.Context.get() == %{Mux.ContextTest => 1, MyMod => 2}
    end)
  end

  test "convert current context to wire context" do
    Mux.Context.bind(%{Mux.ContextTest => 1}, fn ->
      assert Mux.Context.to_wire() == %{"com.example.int" => <<1::64>>}
    end)
  end

  test "decode and bind wire context to context" do
    wire = %{"com.example.int" => <<1::64>>}
    Mux.Context.bind_wire(wire, [Mux.ContextTest], fn ->
      assert Mux.Context.get() == %{Mux.ContextTest => 1}
      assert Mux.Context.to_wire() == wire
    end)
  end
end

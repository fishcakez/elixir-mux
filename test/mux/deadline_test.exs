defmodule Mux.DeadlineTest do
  use ExUnit.Case, async: true

  test "timeout returns a timeout value at or before the deadline" do
    Mux.Deadline.bind(1000, fn ->
      assert Mux.Deadline.timeout() <= 1000
    end)
  end

  test "start timer starts a timer that expires at or before the deadline" do
    Mux.Deadline.bind(1000, fn ->
      timer = Mux.Deadline.start_timer(self(), :hello)
      assert Process.read_timer(timer) <= 1000
    end)
  end

  test "nested deadline merged with prior deadline" do
    Mux.Deadline.bind(1000, fn ->
      Mux.Deadline.bind(60000, fn ->
        assert Mux.Deadline.timeout() <= 1000
      end)
    end)

    Mux.Deadline.bind(60000, fn ->
      Mux.Deadline.bind(1000, fn ->
        assert Mux.Deadline.timeout() <= 1000
      end)
    end)
  end

  test "convert deadline to wire context" do
    pre = System.system_time(:nanosecond)

    Mux.Deadline.bind(1000, fn ->
      assert %{"com.twitter.finagle.Deadline" => <<start::64, finish::64>>} =
        Mux.Context.to_wire()

      post = System.system_time(:nanosecond)

      assert start >= pre
      assert finish <= post + System.convert_time_unit(1000, 1000, :nanosecond)
    end)
  end

  test "decode and bind wire deadline to context" do
    now = System.system_time(:nanosecond)
    wire = %{"com.twitter.finagle.Deadline" => <<now::64, now::64>>}
    Mux.Context.bind_wire(wire, [Mux.Deadline], fn ->
      assert Mux.Deadline.timeout() == 0
      timer = Mux.Deadline.start_timer(self(), :BOOM)
      assert_receive {:timeout, ^timer, :BOOM}
      assert Mux.Context.to_wire() == wire
    end)
  end
end

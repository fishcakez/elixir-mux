defmodule Mux.TraceTest do
  use ExUnit.Case, async: true
  use Bitwise

  test "span with flags starts a root trace" do
    Mux.Trace.span([:debug], fn ->
      trace = Mux.Context.fetch!(Mux.Trace)
      assert %Mux.Trace{parent_id: span_id, span_id: span_id} = trace
      assert (trace.trace_id &&& 0xFFFF_FFFF_FFFF_FFFF) == span_id
      assert Enum.member?(trace.flags, :root)
      assert Enum.member?(trace.flags, :debug)
    end)
  end

  test "bind raises if trace already bound" do
    Mux.Trace.span([], fn ->
      assert_raise RuntimeError, "Mux.Trace already bound in Mux.Context",
        fn -> Mux.Trace.bind(Mux.Trace.start([]), fn -> flunk "ran" end) end
    end)
  end

  test "nested span inherits trace_id and the parent_id is parent's span_id" do
    Mux.Trace.bind(1, 2, 3, [], fn ->
      Mux.Trace.span(fn ->
        trace = Mux.Context.fetch!(Mux.Trace)
        assert %Mux.Trace{trace_id: 1, parent_id: 3, span_id: span_id, flags: []} = trace
        assert span_id !== 3
      end)
    end)
  end

  test "convert trace to wire context" do
    Mux.Trace.span([:sampling_known], fn ->
      assert %{"com.twitter.finagle.tracing.TraceContext" => data} = Mux.Context.to_wire()
      # span id, parent id and lower 64 bits of trace id are the same
      assert <<span_id::64, span_id::64, span_id::64, _::64>> = data
      # 2nd (:sampling_known) and 4th (:root) least sig bits set
      assert <<_::3-binary-unit(64), 0::60, 1::1, 0::1, 1::1, 0::1>> = data
    end)
  end

  test "decode and bind wire deadline to context" do
    span_id = 31394793287310
    parent_id = 390730483098
    trace_id = 39473294703430
    data = <<span_id::64, parent_id::64, trace_id::64, 0::60, 0::1, 1::1, 1::1, 0::1>>
    wire = %{"com.twitter.finagle.tracing.TraceContext" => data}
    Mux.Context.bind_wire(wire, [Mux.Trace], fn ->
      assert %Mux.Trace{trace_id: ^trace_id, parent_id: ^parent_id, span_id: ^span_id, flags: flags} =
        Mux.Context.fetch!(Mux.Trace)
      assert Enum.sort(flags) == [:sampled, :sampling_known]

      assert Mux.Context.to_wire() == wire
    end)
  end
end

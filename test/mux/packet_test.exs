defmodule Mux.PacketTest do
  use ExUnit.Case, async: true

  import PropertyTest
  import StreamData
  import MuxData

  property "encode/decode identity" do
    check all packet <- packet() do
      assert {type, iodata} = Mux.Packet.encode(packet)
      assert Mux.Packet.decode(type, iodata) === packet
    end
  end
end

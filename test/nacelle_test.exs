defmodule NacelleTest do
  use ExUnit.Case

  setup do
    Nacelle.Shard.clear_all
  end

  test "simple put / get txns" do
    assert {:atomic, :ok} == Nacelle.put(:foo, :bar)
    assert {:atomic, :bar} == Nacelle.get(:foo)
    assert Process.info(self)[:message_queue_len] == 0
  end

  test "put transaction spanning shards" do
    res = Nacelle.transaction [:foo, :zab], fn (txn) ->
      Nacelle.put(txn, :foo, :bar)
      Nacelle.put(txn, :zab, :qux)
    end

    # these are not performed in a txn
    assert {:atomic, :bar} == Nacelle.get(:foo)
    assert {:atomic, :qux} == Nacelle.get(:zab)

    assert Process.info(self)[:message_queue_len] == 0
  end

  test "get transaction spanning shards" do
    res = Nacelle.transaction [:foo, :zab], fn (txn) ->
      Nacelle.put(txn, :foo, :bar)
      Nacelle.put(txn, :zab, :qux)
    end

    assert {:atomic, [:bar, :qux]} == Nacelle.transaction [:foo, :zab], fn (txn) ->
      a = Nacelle.get(txn, :foo)
      b = Nacelle.get(txn, :zab)
      [a, b]
    end

    assert Process.info(self)[:message_queue_len] == 0
  end

  test "larger get transaction spanning shards" do
    key_set = [:doo, :ack, :abb, :fre, :zak, :zab]

    res = Nacelle.transaction key_set, fn (txn) ->
      Nacelle.put(txn, :doo, :doo_val)
      Nacelle.put(txn, :ack, :ack_val)
      Nacelle.put(txn, :abb, :abb_val)
      Nacelle.put(txn, :fre, :fre_val)
      Nacelle.put(txn, :zak, :zak_val)
      Nacelle.put(txn, :zab, :zab_val)
    end

    vals = Enum.map(key_set, fn (k) -> :"#{k}_val" end)

    assert {:atomic, vals} == Nacelle.transaction key_set, fn (txn) ->
      [ Nacelle.get(txn, :doo),
        Nacelle.get(txn, :ack),
        Nacelle.get(txn, :abb),
        Nacelle.get(txn, :fre),
        Nacelle.get(txn, :zak),
        Nacelle.get(txn, :zab), ]
    end

    assert Process.info(self)[:message_queue_len] == 0
  end

  test "force race condition that requires locks" do
    Nacelle.put(:bar, :bar_val)
    Nacelle.put(:zoo, :zoo_val)

    outer = self

    pid = spawn fn ->
      Nacelle.transaction [:zoo], fn (txn) ->
        send(outer, :inner_started)

        :timer.sleep(100)
        Nacelle.put(txn, :zoo, :zoo_val2)
      end

      send(outer, :inner_done)
    end

    receive do
      :inner_started -> :ok
    end

    assert {:atomic, [:bar_val, :zoo_val2]} == Nacelle.transaction [:bar, :zoo], fn (txn) ->
      [Nacelle.get(txn, :bar), Nacelle.get(txn, :zoo)]
    end

    receive do
      :inner_done -> :ok
    end
  end
end

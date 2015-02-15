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

  test "a scheduler should be able to run 2 txns in parallel if keys don't collide" do
    Nacelle.put(:bar, :bar_val)
    Nacelle.put(:baz, :baz_val)

    outer = self

    # Warning, there is a race condition here which is that the
    # first txn simply sleeps, but if the other doesn't actually
    # run in time, the test will be non-determistic (also applies)
    # to the return messages to teh main process.

    pid1 = spawn fn ->
      send(outer, :first_started)
      Nacelle.transaction [:baz], fn (txn) ->
        :timer.sleep(100)
        Nacelle.get(txn, :baz)
      end

      send(outer, :pid1)
    end

    receive do :first_started -> :ok end

    pid2 = spawn fn ->
      send(outer, :second_started)
      res = Nacelle.transaction [:bar], fn (txn) ->
        :timer.sleep(10)
        Nacelle.get(txn, :bar)
      end

      send(outer, :pid2)
    end

    receive do :second_started -> :ok end

    pid3 = spawn fn ->
      Nacelle.get(:baz)
      send(outer, :pid3)
    end

    out = [pid1, pid2, pid3] |> Enum.map(fn (_) ->
      receive do
        result -> result
      end
    end)

    assert out == [:pid2, :pid1, :pid3]
  end

  test "a scheduler should be able to run 2 txns in parallel if keys don't collide (other ordering)" do
    Nacelle.put(:bar, :bar_val)
    Nacelle.put(:baz, :baz_val)

    outer = self

    # Warning, there is a race condition here which is that the
    # first txn simply sleeps, but if the other doesn't actually
    # run in time, the test will be non-determistic (also applies)
    # to the return messages to teh main process.

    pid1 = spawn fn ->
      send(outer, :first_started)

      Nacelle.transaction [:baz], fn (txn) ->
        :timer.sleep(100)
        Nacelle.get(txn, :baz)
      end

      send(outer, :pid1)
    end

    receive do :first_started -> :ok end

    pid2 = spawn fn ->
      send(outer, :second_started)

      Nacelle.get(:baz)
      send(outer, :pid2)
    end

    receive do :second_started -> :ok end

    pid3 = spawn fn ->
      Nacelle.get(:bar)
      send(outer, :pid3)
    end

    out = [pid1, pid2, pid3] |> Enum.map(fn (_) ->
      receive do
        result -> result
      end
    end)

    assert out == [:pid3, :pid1, :pid2]
  end

  test "aborting a transaction" do
    Nacelle.put(:bar, :bar_val)
    
    assert :abort == Nacelle.transaction [:bar], fn (txn) ->
      Nacelle.put(txn, :bar, :bar_val2)
      Nacelle.abort(txn)
    end

    assert {:atomic, :bar_val} == Nacelle.get(:bar)
  end

  test "aborting a multi-shard transaction" do
    Nacelle.put(:bar, :bar_val)
    Nacelle.put(:zoo, :zoo_val)
    
    assert :abort == Nacelle.transaction [:bar, :zoo], fn (txn) ->
      Nacelle.put(txn, :bar, :bar_val2)
      Nacelle.put(txn, :zoo, :zoo_val2)
      Nacelle.abort(txn)
    end

    assert {:atomic, :bar_val} == Nacelle.get(:bar)
    assert {:atomic, :zoo_val} == Nacelle.get(:zoo)
  end
end

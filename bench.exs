require Logger

Logger.configure(level: :warn)

# Nacelle
ops = 10_000

{time, val} = :timer.tc fn ->
  Enum.each 1..ops, fn (n) ->
    # key = :random.uniform 10_000
    # Nacelle.put(key, n)

    # Nacelle.transaction [key], fn (txn) ->
    #   Nacelle.put(txn, key, n)
    # end

    kvs = Enum.map 1..10, fn (_) ->
      {:random.uniform(10_000), n}
    end

    # Nacelle.transaction keys, fn (txn) ->
    #   Enum.each keys, fn (k) ->
    #     Nacelle.put(txn, k, n)
    #   end
    # end

    Nacelle.put(kvs)
  end
end

random_gen_time = 110 * ops

real_time = time - random_gen_time

# Nacelle.put(:foo, :bar)
seconds = real_time / 1_000_000.0
ops_s = ops / seconds

IO.puts "Took: #{seconds}s (#{ops_s} ops/s)"

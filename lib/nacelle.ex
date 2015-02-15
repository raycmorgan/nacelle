defmodule Nacelle do
  use Application

  # See http://elixir-lang.org/docs/stable/elixir/Application.html
  # for more information on OTP Applications
  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = [
      # Define workers and child supervisors to be supervised
      # worker(Nacelle.Worker, [arg1, arg2, arg3])
    ]

    {:ok, shard1} = GenServer.start_link(Nacelle.Shard, [], name: :shard1)
    {:ok, shard2} = GenServer.start_link(Nacelle.Shard, [], name: :shard2)

    :pg2.create(:shards)
    :ok = :pg2.join(:shards, shard1)
    :ok = :pg2.join(:shards, shard2)

    {:ok, scheduler1} = GenServer.start_link(Nacelle.Scheduler, [:shard1], name: :scheduler1)
    {:ok, scheduler2} = GenServer.start_link(Nacelle.Scheduler, [:shard2], name: :scheduler2)

    :pg2.create(:schedulers)
    :ok = :pg2.join(:schedulers, scheduler1)
    :ok = :pg2.join(:schedulers, scheduler2)

    GenServer.start_link(Nacelle.Sequencer, [], name: :sequencer)

    # See http://elixir-lang.org/docs/stable/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Nacelle.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def transaction(f) when is_function(f) do
    GenServer.call(:sequencer, {:txn, nil, f})
  end

  def transaction(name, args) when is_atom(name) do
  end

  def transaction(key_set, f) when is_list(key_set) and
                                   length(key_set) > 0 and
                                   is_function(f) do
    GenServer.call(:sequencer, {:txn, key_set, f})
  end

  def create_procedure(name, proc) do
  end

  def put(key, value) do
    Nacelle.transaction [key], fn (txn) ->
      Nacelle.Transaction.put(txn, key, value)
    end
  end

  def get(key) do
    Nacelle.transaction [key], fn (txn) ->
      Nacelle.Transaction.get(txn, key)
    end
  end

  def put(txn, key, value) do
    Nacelle.Transaction.put(txn, key, value)
  end

  def get(txn, key) do
    Nacelle.Transaction.get(txn, key)
  end

  def abort(txn, key) do
    Nacelle.Transaction.abort(txn, key)
  end

  # def get_range(txn, {s, e}, opts) do
  # end
end

# # OOC
# Nacelle.transaction fn (txn) ->
#   foo = Nacelle.get(txn, :foo)
#   Nacelle.put(txn, :foo, foo + 1)
# end
# 
# # Pessimistic
# Nacelle.transcation([:foo], fn (txn) ->
#   foo = Nacelle.get(txn, :foo)
#   Nacelle.put(txn, :foo, foo + 1)
# end)
# 
# # Pessimistic stored proc
# Nacelle.create_procedure(:inc,
#   # if a key is used that is not in the keys_used set the txn is aborted
#   keys_used: fn (keys) -> keys end, # keys can be scalar or a 2 tuple for ranges {:a, {:b}} = [:a, :b)
#   txn: fn (txn, keys) ->
#     Enum.each keys, fn (key) ->
#       val = Nacelle.get(txn, key)
#       Nacelle.put(txn, key, val)
#     end
#   end)
# 
# Nacelle.transaction(:inc, [:foo])

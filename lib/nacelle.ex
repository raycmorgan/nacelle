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

  def transaction(name, args) when is_atom(name) do
  end

  def transaction(f) when is_function(f) do
    GenServer.call(:sequencer, {:txn, nil, f})
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
      Nacelle.Scheduler.put(txn, key, value)
    end
  end

  def get(key) do
    Nacelle.transaction [key], fn (txn) ->
      Nacelle.Scheduler.get(txn, key)
    end
  end

  def put(txn, key, value) do
    Nacelle.Scheduler.put(txn, key, value)
  end

  def get(txn, key) do
    Nacelle.Scheduler.get(txn, key)
  end

  # def get_range(txn, {s, e}, opts) do
  # end
end

defmodule Nacelle.Sequencer do
  use GenServer

  def init([]) do
    {:ok, 1}
  end

  def handle_call({:txn, nil, f}, from, seq) do
    {:reply, {:error, :recon_not_implemented}, seq + 1}
  end

  def handle_call({:txn, key_set, f}, from, seq) do
    # Pass to processors
    # schedulers = :pg2.get_members(:schedulers)

    schedulers = Enum.map(key_set, &scheduler_for_key(&1)) |> Enum.uniq
    indexes = 0..(length(schedulers)-1)

    Enum.zip(schedulers, indexes)
    |> Enum.map(fn ({pid, i}) ->
      leader = rem(seq, length(schedulers)) == i
      GenServer.cast(pid, {:txn, key_set, f, seq, from, leader})
    end)

    {:noreply, seq + 1}
  end

  defp scheduler_for_key(key) do
    if to_string(key) < "m", do: :scheduler1, else: :scheduler2
  end
end

defmodule Nacelle.Transaction do
  defstruct name: nil, key_set: nil, shard: nil, seq: 0, leader: false,
            remote_read_buffer: nil
end

defmodule Nacelle.Scheduler do
  use GenServer

  def init([shard]) do
    ets_options = [:public, :bag]
    tid = :ets.new(:remote_read_buffer, ets_options)

    {:ok, {shard, tid}}
  end

  def handle_cast({:txn, key_set, f, seq, from, leader}, {shard, tid}) do
    txn = %Nacelle.Transaction{
      name: :"txn.#{seq}.#{shard}",
      key_set: key_set,
      shard: shard,
      seq: seq,
      leader: leader,
      remote_read_buffer: tid,
    }

    # monitor this thing!
    spawn_link fn ->
      acquire_locks(txn)

      Process.register(self, :"txn.#{seq}.#{tid}")
      Process.put(:insertions, [])

      # Run the user provided transaction fun
      res = f.(txn)

      # Atomically write all keys
      case Process.get(:insertions) do
        [] -> :ok
        insertions -> GenServer.call(txn.shard, {:put, insertions})
      end

      case other_shards_involved(txn) do
        [] -> :ok
        shards ->
          IO.puts "[#{txn.name}] Sending :txn_fin to other schedulers"

          Enum.each(shards_to_schedulers(shards), fn (s) ->
            GenServer.cast(s, {:txn_fin, seq, txn.shard})
          end)

          IO.puts "[#{txn.name}] Awaiting :txn_fin from other schedulers"

          Enum.each(shards, fn (s) ->
            # We need to prevent a race condition where the remote node has
            # confirmed success, but we haven't even registered ourselves yet.
            # To do so, the sequencer will store all :txn_fin messages into 
            # its ets table.
            case :ets.lookup(txn.remote_read_buffer, {:txn_fin, txn.seq, s}) do
              [] ->
                receive do
                  {:txn_fin, ^seq, ^s} -> :ok
                  # Timeout, handle: shardX possibly down
                end
              _  -> :ok
            end
          end)
      end

      IO.puts "[#{txn.name}] Transaction committed."

      # release locks

      if txn.leader, do: GenServer.reply(from, {:atomic, res})
    end

    {:noreply, {shard, tid}}
  end

  def handle_cast({:txn_fin, seq, rem_shard}, {shard, tid}) do
    :ets.insert(tid, {{:txn_fin, seq, rem_shard}})

    case Process.whereis(:"txn.#{seq}.#{tid}") do
      nil -> nil
      pid -> send(pid, {:txn_fin, seq, rem_shard})
    end

    {:noreply, {shard, tid}}
  end

  def handle_cast({:txn_val, seq, key, value}, {shard, tid}) do
    :ets.insert(tid, {{seq, key}, value})

    case Process.whereis(:"txn.#{seq}.#{tid}") do
      nil -> nil
      pid -> send(pid, {seq, key, value})
    end

    {:noreply, {shard, tid}}
  end

  def get(txn, key) do
    if Enum.member?(txn.key_set, key) do
      if in_shard?(txn, key) do
        val = GenServer.call(txn.shard, {:get, key})

        case other_shards_involved(txn) do
          [] -> nil
          shards ->
            IO.puts "[#{txn.name}] Sending key to other schedulers: #{key}"

            Enum.each(shards_to_schedulers(shards), fn (scheduler) ->
              GenServer.cast(scheduler, {:txn_val, txn.seq, key, val})
            end)
        end

        val
      else
        IO.puts "[#{txn.name}] Need remove key: #{key}"

        case :ets.lookup(txn.remote_read_buffer, {txn.seq, key}) do
          [] -> # not here yet :(
            IO.puts "[#{txn.name}] Remote key hasn't arrived yet: #{key}"

            seq = txn.seq
            receive do
              {^seq, ^key, val} ->
                IO.puts "[#{txn.name}] Remote key arrived: #{key}"
                val # timeout, go get the val from someone?
            end
          [{_, val}] ->
            IO.puts "[#{txn.name}] Remote key already arrived: #{key}"
            val # then clear?
        end
      end
    else
      raise "[#{txn.name}] Attempting to get key not in key_set: #{key}"
    end
  end

  def put(txn, key, value) do
    if Enum.member?(txn.key_set, key) do
      if in_shard?(txn, key) do
        insertions = Process.get(:insertions)
        Process.put(:insertions, [{key, value} | insertions])
        :ok
      else
        IO.puts("[#{txn.name}] Ignoring put as it belongs to another shard")
      end
    else
      raise "[#{txn.name}] Attempting to put key not in key_set: #{key}"
    end
  end

  def acquire_locks(txn) do
    Nacelle.LockManager.acquire_locks(self, txn.key_set)
  end

  defp in_shard?(txn, key) do
    txn.shard == shard_for_key(key)
  end

  defp shard_for_key(key) do
    if to_string(key) < "m", do: :shard1, else: :shard2
  end

  defp other_shards_involved(txn) do
    Enum.map(txn.key_set, &shard_for_key(&1))
    |> Enum.uniq
    |> Enum.reject(fn (shard) -> shard == txn.shard end)
  end

  defp shards_to_schedulers(shards) do
    Enum.map(shards, fn
      (:shard1) -> :scheduler1
      (:shard2) -> :scheduler2
    end)
  end
end

defmodule Nacelle.Shard do
  use GenServer

  def init([]) do
    {:ok, HashDict.new}
  end

  def handle_call({:get, key}, _from, store) do
    {:reply, Dict.get(store, key), store}
  end

  def handle_call({:put, key, value}, _from, store) do
    {:reply, :ok, Dict.put(store, key, value)}
  end

  def handle_call({:put, key_value_list}, _from, store) do
    {:reply, :ok, Dict.merge(store, key_value_list)}
  end

  def handle_call(:clear, _from, store) do
    {:reply, :ok, HashDict.new}
  end

  def clear_all do
    :pg2.get_members(:shards) |> Enum.each fn (shard) ->
      GenServer.call(shard, :clear)
    end
  end
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

# [
#   {1, %{foo: :bar}},
#   {2, %{qux: :baz}}
# ]

# [
#   {1, :foo, :bar},
#   {2, :qux, :baz}
# ]





[
  {1, [:foo, :bar], pid},
  {2, [:baz], pid}
  {3, [:bar, :buz], pid}
]


defmodule Nacelle.Transaction do
  require Logger

  defstruct name: nil, key_set: nil, shard: nil, seq: 0, leader: false,
            remote_read_buffer: nil, lock_manager: nil, f: nil, from: nil

  def start_link(txn) do
    spawn_link fn ->
      Nacelle.LockManager.acquire_locks(txn.lock_manager, txn.name)
      Logger.debug "[#{txn.name}] Locks acquired"

      Process.register(self, :"txn.#{txn.seq}.#{txn.remote_read_buffer}")
      Process.put(:insertions, [])

      # Run the user provided transaction fun
      res = txn.f.(txn)

      # Atomically write all keys
      case Process.get(:insertions) do
        [] -> :ok
        insertions -> GenServer.call(txn.shard, {:put, insertions})
      end

      case other_shards_involved(txn) do
        [] -> :ok
        shards ->
          Logger.debug "[#{txn.name}] Sending :txn_fin to other schedulers"

          Enum.each(shards_to_schedulers(shards), fn (s) ->
            GenServer.cast(s, {:txn_fin, txn.seq, txn.shard})
          end)

          Logger.debug "[#{txn.name}] Awaiting :txn_fin from other schedulers"

          Enum.each(shards, fn (s) ->
            # We need to prevent a race condition where the remote node has
            # confirmed success, but we haven't even registered ourselves yet.
            # To do so, the sequencer will store all :txn_fin messages into 
            # its ets table.
            seq = txn.seq

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

      Logger.debug "[#{txn.name}] Transaction committed."

      Nacelle.LockManager.release_locks(txn.lock_manager, txn.name)

      if txn.leader, do: GenServer.reply(txn.from, {:atomic, res})
    end
  end

  def get(txn, key) do
    if Enum.member?(txn.key_set, key) do
      if in_shard?(txn, key) do
        val = GenServer.call(txn.shard, {:get, key})

        case other_shards_involved(txn) do
          [] -> nil
          shards ->
            Logger.debug "[#{txn.name}] Sending key to other schedulers: #{key}"

            Enum.each(shards_to_schedulers(shards), fn (scheduler) ->
              GenServer.cast(scheduler, {:txn_val, txn.seq, key, val})
            end)
        end

        val
      else
        Logger.debug "[#{txn.name}] Need remote key: #{key}"

        case :ets.lookup(txn.remote_read_buffer, {txn.seq, key}) do
          [] -> # not here yet :(
            Logger.debug "[#{txn.name}] Remote key hasn't arrived yet: #{key}"

            seq = txn.seq
            receive do
              {^seq, ^key, val} ->
                Logger.debug "[#{txn.name}] Remote key arrived: #{key}"
                val # timeout, go get the val from someone?
            end
          [{_, val}] ->
            Logger.debug "[#{txn.name}] Remote key already arrived: #{key}"
            val # then clear?
        end
      end
    else
      # Todo: this is not what I should do..
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
        Logger.debug "[#{txn.name}] Ignoring put as it belongs to another shard"
      end
    else
      # Todo: this is not what I should do..
      raise "[#{txn.name}] Attempting to put key not in key_set: #{key}"
    end
  end

  # --- Private ---

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

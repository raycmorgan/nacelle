defmodule Nacelle.Transaction do
  require Logger

  defstruct name: nil, key_set: nil, shard: nil, seq: 0, leader: false,
            remote_read_buffer: nil, lock_manager: nil, f: nil, from: nil,
            is_recon: false, was_recon: false

  def start_link(txn) do
    spawn_link fn ->
      Nacelle.LockManager.acquire_locks(txn.lock_manager, txn.name)
      Logger.debug "[#{txn.name}] Locks acquired"

      Process.register(self, :"txn.#{txn.seq}.#{txn.remote_read_buffer}")
      Process.put(:insertions, [])

      abort_ref = make_ref

      # Run the user provided transaction fun
      res = try do
        case txn.f do
          {m, f, a} -> apply(m, f, [txn | a]) 
          f -> f.(txn)
        end
      rescue
        e -> abort_ref
      end

      if res != abort_ref && commit_all_shards(txn), do: do_commit(txn, res), else: do_abort(txn)
      
      Nacelle.LockManager.release_locks(txn.lock_manager, txn.name)
    end
  end

  def get(txn, key) do
    if Enum.member?(txn.key_set, key) do
      if in_shard?(txn, key) do
        insertions = Process.get(:insertions)

        val = if Dict.has_key?(insertions, key) do
          insertions[key]
        else
          GenServer.call(txn.shard, {:get, key})
        end

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

  def abort(txn, key) do
    raise "[#{txn.name}] Was aborted by user."
  end

  # --- Private ---

  defp commit_all_shards(txn) do
    broadcast_commit(txn)

    case other_shards_involved(txn) do
      [] -> true
      shards ->
        # Wait for reply from other schedulers
        Logger.debug "[#{txn.name}] Awaiting :txn_result from other schedulers"

        Enum.all?(shards, fn (s) ->
          # We need to prevent a race condition where the remote node has
          # confirmed success, but we haven't even registered ourselves yet.
          # To do so, the sequencer will store all :txn_result messages into 
          # its ets table.
          seq = txn.seq

          case :ets.lookup(txn.remote_read_buffer, {:txn_result, txn.seq, s}) do
            [] ->
              receive do
                {:txn_result, :atomic, ^seq, ^s} -> true
                {:txn_result, :abort, ^seq, ^s} -> false
                # Timeout, handle: shardX possibly down
              end
            [{_, :atomic}] -> true
            [{_, :abort}] -> false
          end
        end)
    end
  end

  defp broadcast_commit(txn) do
    Logger.debug "[#{txn.name}] Sending :txn_result to other schedulers"

    case other_shards_involved(txn) do
      [] -> :ok
      shards ->
        Enum.each(shards_to_schedulers(shards), fn (s) ->
          GenServer.cast(s, {:txn_result, :atomic, txn.seq, txn.shard})
        end)
    end
  end

  defp broadcast_abort(txn, shards) do
    Logger.debug "[#{txn.name}] Sending :txn_result to other schedulers"

    case other_shards_involved(txn) do
      [] -> :ok
      shards ->
        Enum.each(shards_to_schedulers(shards), fn (s) ->
          GenServer.cast(s, {:txn_result, :abort, txn.seq, txn.shard})
        end)
    end
  end

  defp do_commit(txn, res) do
    Logger.debug "[#{txn.name}] Transaction committed."

    # Atomically write all keys
    case Process.get(:insertions) do
      [] -> :ok
      insertions -> GenServer.call(txn.shard, {:put, insertions})
    end

    if txn.leader, do: GenServer.reply(txn.from, {:atomic, res})
  end

  defp do_abort(txn) do
    Logger.debug "[#{txn.name}] Transaction aborted."

    if txn.leader do
      if txn.was_recon do
        # TODO: remove hardcode
        GenServer.cast(:sequencer, {:txn_retry, txn.f, txn.from})
      else
        GenServer.reply(txn.from, :abort)
      end
    end
  end

  defp in_shard?(txn, key) do
    txn.shard == Nacelle.Shard.shard_for_key(key)
  end

  defp other_shards_involved(txn) do
    Enum.map(txn.key_set, &Nacelle.Shard.shard_for_key(&1))
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

defmodule Nacelle.Proc do
  def get(txn, keys) when is_list(keys) do
    Enum.map keys, &Nacelle.Transaction.get(txn, &1)
  end

  def get(txn, key) do
    Nacelle.Transaction.get(txn, key)
  end

  def put(txn, kvs) do
    Enum.map kvs, fn ({k, v}) ->
      Nacelle.Transaction.put(txn, k, v)
    end
  end

  def put(txn, key, value) do
    Nacelle.Transaction.put(txn, key, value)
  end
end

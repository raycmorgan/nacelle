defmodule Nacelle.Scheduler do
  defmodule State do
    defstruct shard: nil, tid: nil, lock_manager: nil
  end

  use GenServer

  def init([shard]) do
    {:ok, lock_manager} = GenServer.start_link(Nacelle.LockManager, [])

    state = %State{
      shard: shard,
      tid: :ets.new(:remote_read_buffer, [:public, :bag]),
      lock_manager: lock_manager
    }

    {:ok, state}
  end

  def handle_cast({:txn, key_set, f, seq, from, leader}, state) do
    %State{shard: shard, tid: tid, lock_manager: lock_manager} = state

    txn = %Nacelle.Transaction{
      name: :"txn.#{seq}.#{shard}",
      key_set: key_set,
      shard: shard,
      seq: seq,
      leader: leader,
      remote_read_buffer: tid,
      lock_manager: lock_manager,
      f: f,
      from: from
    }

    # Sending this from the scheduler process is important since
    # the LockManager needs to get the requests *in order*
    Nacelle.LockManager.prepare_locks(lock_manager, txn.name, txn.key_set)

    # monitor this thing!
    Nacelle.Transaction.start_link(txn)

    {:noreply, state}
  end

  def handle_cast({:txn_fin, seq, rem_shard}, %State{shard: shard, tid: tid} = state) do
    :ets.insert(tid, {{:txn_fin, seq, rem_shard}})

    case Process.whereis(:"txn.#{seq}.#{tid}") do
      nil -> nil
      pid -> send(pid, {:txn_fin, seq, rem_shard})
    end

    {:noreply, state}
  end

  def handle_cast({:txn_val, seq, key, value}, %State{shard: shard, tid: tid} = state) do
    :ets.insert(tid, {{seq, key}, value})

    case Process.whereis(:"txn.#{seq}.#{tid}") do
      nil -> nil
      pid -> send(pid, {seq, key, value})
    end

    {:noreply, state}
  end
end

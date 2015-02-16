defmodule Nacelle.ReconTransaction do
  require Logger

  defstruct f: nil, from: nil, sequencer: nil, is_recon: true

  def start_link(txn) do
    spawn_link fn ->
      Process.put(:key_set, [])

      case txn.f do
        {m, f, a} -> apply(m, f, [txn | a]) 
        f -> f.(txn)
      end

      key_set = Process.get(:key_set)

      GenServer.cast(txn.sequencer, {:txn, key_set, txn.f, txn.from})
    end
  end

  def get(_txn, key) do
    Logger.debug("Recon get for key #{key}")
    append_to_key_set(key)

    shard = Nacelle.Shard.shard_for_key(key)
    GenServer.call(shard, {:get, key})
  end

  def put(_txn, key, _value) do
    Logger.debug("Recon put for key #{key}")
    append_to_key_set(key)
  end

  defp append_to_key_set(key) do
    key_set = Process.get(:key_set)
    Process.put(:key_set, [key | key_set])
  end
end

defmodule Nacelle.Sequencer do
  use GenServer

  def init([]) do
    {:ok, 1}
  end

  def handle_call({:txn, nil, f}, from, seq) do
    txn = %Nacelle.ReconTransaction{f: f, from: from, sequencer: self}
    Nacelle.ReconTransaction.start_link(txn)
    {:noreply, seq}
  end

  def handle_call({:txn, key_set, f}, from, seq) do
    run_transaction(seq, from, key_set, f, false)
    {:noreply, seq + 1}
  end

  def handle_cast({:txn, key_set, f, from}, seq) do
    run_transaction(seq, from, key_set, f, true)
    {:noreply, seq + 1}
  end

  def handle_cast({:txn_retry, f, from}, seq) do
    txn = %Nacelle.ReconTransaction{f: f, from: from, sequencer: self}
    Nacelle.ReconTransaction.start_link(txn)
    {:noreply, seq}
  end

  # --- Private ---

  defp run_transaction(seq, from, key_set, f, was_recon) do
    schedulers = Enum.map(key_set, &Nacelle.Scheduler.scheduler_for_key(&1)) |> Enum.uniq
    indexes = 0..(length(schedulers)-1)

    Enum.zip(schedulers, indexes)
    |> Enum.map(fn ({pid, i}) ->
      leader = rem(seq, length(schedulers)) == i
      GenServer.cast(pid, {:txn, key_set, f, seq, from, leader, was_recon})
    end)
  end
end

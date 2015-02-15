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

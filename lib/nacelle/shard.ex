defmodule Nacelle.Shard do
  use GenServer

  def init([shard_name]) do
    tid = :ets.new(shard_name, [
      :set,
      :public,
      :named_table,
      {:write_concurrency, true},
      {:read_concurrency, true}
    ])

    {:ok, tid}
  end

  def handle_call({:get, key}, _from, store) do
    val = case :ets.lookup(store, key) do
      [{_, val}] -> val
      [] -> nil
    end

    {:reply, val, store}
  end

  def handle_call({:put, key, value}, _from, store) do
    :ets.insert(store, {key, value})
    {:reply, :ok, store}
  end

  def handle_call({:put, key_value_list}, _from, store) do
    :ets.insert(store, key_value_list)
    {:reply, :ok, store}
  end

  def handle_call(:clear, _from, store) do
    :ets.delete_all_objects(store)
    {:reply, :ok, store}
  end

  def clear_all do
    :pg2.get_members(:shards) |> Enum.each fn (shard) ->
      GenServer.call(shard, :clear)
    end
  end

  def shard_for_key(key) when is_number(key) do
    if to_string(key) < "5", do: :shard1, else: :shard2
  end

  def shard_for_key(key) do
    if to_string(key) < "m", do: :shard1, else: :shard2
  end
end

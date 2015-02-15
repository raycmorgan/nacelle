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

defmodule Nacelle.LockManager do
  require Logger
  
  use GenServer

  def prepare_locks(lock_manager, name, key_set) do
    GenServer.call(lock_manager, {:prepare_locks, name, key_set})
  end

  def acquire_locks(lock_manager, name) do
    GenServer.call(lock_manager, {:acquire_locks, name})
  end

  def release_locks(lock_manager, name) do
    GenServer.call(lock_manager, {:release_locks, name})
  end

  defmodule State do
    @derive [Access]
    defstruct pending_txns: HashDict.new,
              running_txns: HashDict.new,
              locks_held: HashSet.new
  end

  def init([]) do
    {:ok, %State{}}
  end

  def handle_call({:prepare_locks, name, key_set}, _from, state) do
    record = %{name: name, locks: Enum.into(key_set, HashSet.new), from: nil}
    state = %State{state | pending_txns: Dict.put(state.pending_txns, name, record)}

    {:reply, :ok, state}
  end

  def handle_call({:acquire_locks, name}, from, state) do
    Logger.debug "[#{name}] -> Acquiring locks #{inspect(state.pending_txns[name].locks)}"

    state = put_in(state[:pending_txns][name][:from], from)
    state = _acquire_locks(state)
    {:noreply, state}
  end

  def handle_call({:release_locks, name}, from, state) do
    GenServer.reply(from, :ok)

    state = _release_locks(state, name)
    state = _acquire_locks(state)

    {:noreply, state}
  end

  def _release_locks(state, name) do
    txn_locks = state.running_txns[name][:locks]

    %State{state | locks_held: Set.difference(state.locks_held, txn_locks),
                   running_txns: Dict.delete(state.running_txns, name)}
  end

  def _acquire_locks(state) do
    # TODO: make this acquire locks for ALL pending txns that are able to run
    # TODO: only acquire locks on THIS shard

    case (Dict.keys(state.pending_txns) |> Enum.sort) do
      [] -> state
      pending_txn_keys ->
        Enum.reduce(pending_txn_keys, state, fn (txn_name, state) ->
          record = Dict.get(state.pending_txns, txn_name)

          if record.from && Set.disjoint?(state.locks_held, record.locks) do
            GenServer.reply(record.from, :locks_held)

            %State{state | locks_held: Set.union(state.locks_held, record.locks),
                           pending_txns: Dict.delete(state.pending_txns, txn_name),
                           running_txns: Dict.put(state.running_txns, txn_name, record)}
          else
            if record.from do
              Logger.debug "[#{record.name}] Attempted to acquire #{inspect(record.locks)} but already held #{inspect(state.locks_held)}"
            else
              Logger.debug "[#{record.name}] Skipped attempt since don't have reply pid yet. #{inspect(record.locks)}"
            end

            state
          end
        end)
    end
  end

  def _acquire_locks(state), do: state
end

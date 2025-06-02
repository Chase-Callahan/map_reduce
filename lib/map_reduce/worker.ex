defmodule MapReduce.Worker do
  use GenServer

  alias MapReduce.Assignment

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{})
  end

  @impl GenServer
  def init(init_arg) do
    {:ok, init_arg}
  end

  def assign(pid, assignment) do
    GenServer.call(pid, {:assign, assignment})
  end

  def send_result(pid, assignment, destination) do
    GenServer.cast(pid, {:result, assignment, destination})
  end

  @impl GenServer
  def handle_call({:assign, assignment}, {coordinator, _ref}, state) do
    schedule_assignment(assignment, coordinator)

    {:reply, {:ok, :processing}, state}
  end

  @impl GenSever
  def handle_cast({:result, assignment, destination}, state) do
    send(destination, {:ok, Map.get(state, Assignment.id(assignment))})
    {:noreply, state}
  end

  defp schedule_assignment(assignment, coordinator) do
    send(self(), {:do_assignment, assignment, coordinator})
  end

  @impl GenServer
  def handle_info({:do_assignment, assignment, coordinator}, state) do
    send(coordinator, {:ok, self(), :completed})
    {:noreply, Map.put(state, Assignment.id(assignment), :final_result)}
  end
end

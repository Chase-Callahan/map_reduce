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

  def result(pid, assignment) do
    GenServer.call(pid, {:result, assignment})
  end

  @impl GenServer
  def handle_call({:assign, assignment}, {coordinator, _ref}, state) do
    schedule_assignment(assignment, coordinator)

    {:reply, {:ok, :processing}, state}
  end

  @impl GenServer
  def handle_call({:result, assignment}, _from, state) do
    {:reply, {:ok, Map.get(state, Assignment.id(assignment))}, state}
  end

  defp schedule_assignment(assignment, coordinator) do
    send(self(), {:do_assignment, assignment, coordinator})
  end

  @impl GenServer
  def handle_info({:do_assignment, assignment, coordinator}, state) do
    {:ok, input_data} = Assignment.input_data(assignment)
    output_data = Assignment.process_data(assignment, input_data)

    send(coordinator, {:ok, self(), :completed})
    {:noreply, Map.put(state, Assignment.id(assignment), output_data)}
  end
end

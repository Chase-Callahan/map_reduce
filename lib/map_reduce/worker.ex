defmodule MapReduce.Worker do
  use GenServer

  alias MapReduce.Coordinator
  alias MapReduce.Assignment

  def start_link() do
    GenServer.start_link(__MODULE__, %{})
  end

  @impl GenServer
  def init(_) do
    {:ok, %{}}
  end

  def assign(pid, assignment) do
    GenServer.call(pid, {:assign, assignment})
  end

  def result(pid, assignment) do
    GenServer.call(pid, {:result, assignment})
  end

  def all_results(pid) do
    GenServer.call(pid, :all_results)
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

  def handle_call(:all_results, _from, state) do
    {:reply, {:ok, state}, state}
  end

  defp schedule_assignment(assignment, coordinator) do
    send(self(), {:do_assignment, assignment, coordinator})
  end

  @impl GenServer
  def handle_info({:do_assignment, assignment, coordinator}, state) do
    output_data =
      case Assignment.input_data(assignment) do
        {:ok, []} ->
          Coordinator.assignment_empty(coordinator)
          []

        {:ok, input_data} ->
          output_data = Assignment.process_data(assignment, input_data)
          Coordinator.assignment_complete(coordinator)
          output_data
      end

    {:noreply, Map.put(state, Assignment.id(assignment), output_data)}
  end

  def child_spec(id) do
    %{
      id: id,
      start: {__MODULE__, :start_link, []}
    }
  end
end

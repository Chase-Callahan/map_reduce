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

  @impl GenServer
  def handle_call({:assign, assignment}, {coordinator, _ref}, state) do
    schedule_assignment(assignment, coordinator)

    {:reply, {:ok, :processing}, state}
  end

  defp schedule_assignment(assignment, coordinator) do
    send(self(), {:do_assignment, assignment, coordinator})
  end

  @impl GenServer
  def handle_info({:do_assignment, assignment, coordinator}, state) do
    case Assignment.process(assignment) do
      :empty ->
        :ok = Coordinator.assignment_empty(coordinator)

      :ok ->
        :ok = Coordinator.assignment_complete(coordinator)
    end

    {:noreply, state}
  end

  def child_spec(id) do
    %{
      id: id,
      start: {__MODULE__, :start_link, []}
    }
  end
end

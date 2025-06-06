defmodule MapReduce.Coordinator do
  use GenServer

  alias MapReduce.Assignment
  alias MapReduce.Worker

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl GenServer
  def init(opts) do
    workers =
      1..Keyword.fetch!(opts, :workers)
      |> Enum.map(fn _ ->
        {:ok, worker} = Worker.start_link()
        worker
      end)

    schedule_processing()

    {:ok,
     %{
       workers: %{ready: workers, processing: []},
       input_stream: Keyword.fetch!(opts, :input_stream),
       mapper_fun: Keyword.fetch!(opts, :mapper_fun),
       reduce_fun: Keyword.fetch!(opts, :reduce_fun),
       assignments: %{},
       reduce_assignments: []
     }}
  end

  defp schedule_processing() do
    Process.send(self(), :start_processing, [])
  end

  def workers(pid) do
    GenServer.call(pid, :workers)
  end

  def assignment_empty(pid) do
    GenServer.cast(pid, {:assignment_empty, self()})
  end

  def assignment_complete(pid) do
    GenServer.cast(pid, {:assignment_complete, self()})
  end

  @impl GenServer
  def handle_call(:workers, _from, state) do
    all_workers = Enum.map(state.workers, fn {_, workers} -> workers end) |> List.flatten()

    {:reply, {:ok, all_workers}, state}
  end

  @impl GenServer
  def handle_cast({:assignment_empty, _worker}, state) do
    {:noreply, state}
  end

  def handle_cast({:assignment_complete, worker}, state) do
    {:noreply, handle_completed_assignment(state, worker)}
  end

  defp handle_completed_assignment(%{input_stream: input_stream} = state, worker) do
    map_assignment = Map.fetch!(state.assignments, worker)

    assignment =
      state.input_stream
      |> Stream.take(10)
      |> Assignment.new!(create_mapper(state))

    Worker.assign(worker, assignment)

    # state
    # |> Map.update!(:assignments, &Map.put(worker, assignment))
  end

  defp create_reducer(state, map_assignment) do
    {__MODULE__, :reducer, [state.reduce_fun, map_assignment]}
  end

  # def reducer(data, )

  @impl GenServer
  def handle_info(:start_processing, state) do
    {:noreply, assign_initial_assignments(state)}
  end

  defp assign_initial_assignments(%{workers: %{ready: []}} = state) do
    state
  end

  defp assign_initial_assignments(%{workers: %{ready: [worker | other_workers]}} = state) do
    assignment =
      state.input_stream
      |> Stream.take(10)
      |> Assignment.new!(create_mapper(state))

    {:ok, :processing} = Worker.assign(worker, assignment)

    state
    |> Map.update!(:input_stream, &Stream.drop(&1, 10))
    |> Map.update!(:assignments, &Map.put(&1, worker, assignment))
    |> Map.update!(:workers, fn workers ->
      %{workers | ready: other_workers, processing: [worker | workers.processing]}
    end)
    |> assign_initial_assignments()
  end

  defp create_mapper(state) do
    {__MODULE__, :mapper, [state.mapper_fun]}
  end

  def mapper(input_data, mapper_fun) do
    for datum <- input_data do
      mapper_fun.(datum)
    end
  end
end

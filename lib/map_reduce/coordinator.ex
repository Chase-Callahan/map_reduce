defmodule MapReduce.Coordinator do
  use GenServer

  alias MapReduce.Assignment
  alias MapReduce.Worker
  alias MapReduce.Coordination

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
     Coordination.new!(
       Keyword.fetch!(opts, :input_stream),
       workers,
       Keyword.fetch!(opts, :mapper_fun),
       Keyword.fetch!(opts, :reduce_fun)
     )}
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
  def handle_call(:workers, _from, coordination) do
    {:reply, {:ok, Coordination.workers(coordination)}, coordination}
  end

  def handle_call(:final_result, _from, coordination) do
    {:reply, {:ok, Coordination.final_result(coordination)}, coordination}
  end

  @impl GenServer
  def handle_cast({:assignment_empty, worker}, coordination) do
    coordination = Coordination.assignment_empty(coordination, worker)
    assign_worker_assignments(coordination)

    {:noreply, coordination}
  end

  def handle_cast({:assignment_complete, worker}, coordination) do
    coordination =
      if %{} != Coordination.worker_assignments(coordination) do
        coordination = Coordination.assignment_completed(coordination, worker)
        assign_worker_assignments(coordination)
      else
        coordination
      end

    {:noreply, coordination}
  end

  @impl GenServer
  def handle_info(:start_processing, state) do
    {:noreply, assign_worker_assignments(state)}
  end

  defp assign_worker_assignments(coordination) do
    for {worker, assignment} <- Coordination.worker_assignments(coordination) do
      Worker.assign(worker, assignment)
    end

    coordination
  end

  def final_result(pid) do
    GenServer.call(pid, :final_result)
  end
end

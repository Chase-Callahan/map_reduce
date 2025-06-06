defmodule MapReduce.Coordination do
  alias MapReduce.Assignment

  def new!(input_stream, workers, map_item_fun, reduce_item_fun) do
    if workers != Enum.uniq(workers) do
      raise ArgumentError, message: "All workers provided must be unique"
    end

    %{
      input_stream: input_stream,
      mapper_stream: Stream.chunk_every(input_stream, 10),
      idle_workers: workers,
      worker_assignments: %{},
      map_item_fun: map_item_fun,
      reduce_item_fun: reduce_item_fun,
      reduce_assignments: [],
      combine_assignments: [],
      final_outputs: []
    }
    |> assign_workers()
  end

  defp assign_workers(state) do
    for worker <- state.idle_workers, reduce: state do
      %{mapper_stream: mapper_stream} = state ->
        assignment =
          mapper_stream
          |> Stream.take(1)
          |> Assignment.new!(create_mapper_fun(state), :mapper)

        state
        |> Map.update!(:worker_assignments, &Map.put(&1, worker, assignment))
        |> Map.update!(:mapper_stream, &Stream.drop(&1, 1))

      %{reduce_assignments: [head | tail]} = state ->
        state
        |> Map.update!(:worker_assignments, &Map.put(&1, worker, head))
        |> Map.put(:reduce_assignments, tail)

      %{combine_assignments: [head | tail]} = state ->
        state
        |> Map.update!(:worker_assignments, &Map.put(&1, worker, head))
        |> Map.put(:combine_assignments, tail)

      state ->
        state
    end
    |> Map.put(:idle_workers, [])
  end

  defp create_mapper_fun(state) do
    {__MODULE__, :mapper_fun, [state.map_item_fun]}
  end

  def mapper_fun(data, map_item_fun) do
    for datum <- data, reduce: %{} do
      acc ->
        {key, value} = map_item_fun.(datum)
        Map.update(acc, key, List.wrap(value), &(&1 ++ List.wrap(value)))
    end
  end

  def worker_assignments(state) do
    Map.fetch!(state, :worker_assignments)
  end

  def assignment_completed(state, worker) do
    completed_assignment = Map.fetch!(state.worker_assignments, worker)

    state
    |> create_next_assignment(completed_assignment)
    |> move_worker_to_idle(worker)
    |> assign_workers()
  end

  defp create_next_assignment(state, completed_assignment) do
    case Assignment.type(completed_assignment) do
      :mapper ->
        reduce_assignment =
          Assignment.output_stream(completed_assignment)
          |> Assignment.new!(create_reduce_fun(state), :reduce)

        Map.update!(state, :reduce_assignments, &[reduce_assignment | &1])

      :reduce ->
        assign_combine_assignment(state, completed_assignment)

      :combine ->
        Map.update!(
          state,
          :final_outputs,
          &(&1 ++ [Assignment.output_stream(completed_assignment)])
        )
    end
  end

  defp create_reduce_fun(state) do
    {__MODULE__, :reduce_fun, [state.reduce_item_fun]}
  end

  def reduce_fun(data, reduce_item_fun) do
    for {key, _values} = datum <- data, into: %{} do
      {key, reduce_item_fun.(datum)}
    end
  end

  defp assign_combine_assignment(%{final_outputs: [head | tail]} = state, completed_assignment) do
    assignment =
      Stream.concat(head, Assignment.output_stream(completed_assignment))
      |> Assignment.new!(create_reduce_fun(state), :combine)

    state
    |> Map.put(:final_outputs, tail)
    |> Map.update!(:combine_assignments, &(&1 ++ [assignment]))
  end

  defp assign_combine_assignment(%{final_outputs: []} = state, completed_assignment) do
    Map.put(state, :final_outputs, [Assignment.output_stream(completed_assignment)])
  end

  def assignment_empty(%{mapper_stream: _} = state, worker) do
    move_worker_to_idle(state, worker)
    |> Map.delete(:mapper_stream)
    |> assign_workers()
  end

  def assignment_empty(state, worker) do
    move_worker_to_idle(state, worker)
    |> assign_workers()
  end

  defp move_worker_to_idle(state, worker) do
    state
    |> Map.update!(:worker_assignments, &Map.delete(&1, worker))
    |> Map.update!(:idle_workers, &[worker | &1])
  end

  def final_result(state) do
    all_assignments_complete? = Enum.empty?(state.worker_assignments)

    if all_assignments_complete? do
      [final_result] = state.final_outputs
      final_result
    end
  end

  def workers(state) do
    Map.keys(state.worker_assignments) ++ state.idle_workers
  end
end

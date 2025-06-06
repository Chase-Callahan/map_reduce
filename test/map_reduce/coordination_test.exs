defmodule MapReduce.CoordinationTest do
  use ExUnit.Case, async: true
  alias MapReduce.Assignment
  alias MapReduce.Worker
  alias MapReduce.Coordination

  def parse_cat(csv_string) do
    csv_string
    |> String.trim("\n")
    |> String.split(",")
    |> then(fn [name, age, type] ->
      %{
        name: name,
        age: age,
        type: type
      }
    end)
  end

  setup do
    stream =
      Path.expand("./priv/sample_data/cats.csv")
      |> File.stream!()
      |> Stream.map(&parse_cat/1)

    [stream: stream]
  end

  test "succsessfully creates coordination", ctx do
    Coordination.new!(ctx.stream, create_workers(5), &Function.identity/1, &Function.identity/1)
  end

  test "all workers provided must be unique", ctx do
    [w1, w2] = create_workers(2)

    assert_raise ArgumentError, fn ->
      Coordination.new!(ctx.stream, [w1, w2, w1], &Function.identity/1, &Function.identity/1)
    end
  end

  test "all workers are provided assignments on creation", ctx do
    workers = create_workers(5)

    assert ^workers =
             Coordination.new!(ctx.stream, workers, &Function.identity/1, &Function.identity/1)
             |> Coordination.worker_assignments()
             |> Map.keys()
  end

  test "all workers are given initially given mapper assignments", ctx do
    worker_assignments =
      Coordination.new!(ctx.stream, create_workers(5), &Function.identity/1, &Function.identity/1)
      |> Coordination.worker_assignments()

    for {_worker, assignment} <- worker_assignments do
      assert :mapper == Assignment.type(assignment)
    end
  end

  test "worker assignments are all unique", ctx do
    worker_assignments =
      Coordination.new!(ctx.stream, create_workers(5), &Function.identity/1, &Function.identity/1)
      |> Coordination.worker_assignments()

    assignments = Map.values(worker_assignments)

    assert assignments == Enum.uniq(assignments)
  end

  test "when worker complete assignment, they are given new mapper assignment if available",
       ctx do
    [w1 | _] = workers = create_workers(5)

    coordination =
      Coordination.new!(ctx.stream, workers, &Function.identity/1, &Function.identity/1)

    worker_assignments_1 = Coordination.worker_assignments(coordination)

    worker_assignments_2 =
      Coordination.assignment_completed(coordination, w1)
      |> Coordination.worker_assignments()

    refute Map.fetch!(worker_assignments_1, w1) == Map.fetch!(worker_assignments_2, w1)
  end

  test "when a mapper assignment is marked as empty, we move from mapper assignments to reduce assignments",
       ctx do
    [w1, w2 | _] = workers = create_workers(5)

    worker_assignments =
      Coordination.new!(ctx.stream, workers, &Function.identity/1, &Function.identity/1)
      |> Coordination.assignment_completed(w1)
      |> Coordination.assignment_empty(w2)
      |> Coordination.worker_assignments()

    assignment = Map.fetch!(worker_assignments, w2)

    assert :reduce == Assignment.type(assignment)
  end

  test "when a reduce assignment is marked as empty, we move from reduce, to combine assignments",
       ctx do
    [w1, w2 | _] = workers = create_workers(5)

    worker_assignments =
      Coordination.new!(ctx.stream, workers, &Function.identity/1, &Function.identity/1)
      # First Mapper complete
      |> Coordination.assignment_completed(w1)
      # Second Mapper Complete
      |> Coordination.assignment_completed(w2)
      # Marks Mapper strem as complete
      |> Coordination.assignment_empty(w2)
      # First Reduce Complete
      |> Coordination.assignment_completed(w2)
      # Second Reduce Complete & Combine First and Second Reduce Assignment Created
      |> Coordination.assignment_completed(w2)
      |> Coordination.worker_assignments()

    assignment = Map.fetch!(worker_assignments, w2)

    assert :combine == Assignment.type(assignment)
  end

  test "when all assignments have been completed, an empty map is returned when requesting worker assignments",
       ctx do
    [w1] = workers = create_workers(1)

    assert %{} ==
             Coordination.new!(ctx.stream, workers, &Function.identity/1, &Function.identity/1)
             |> Coordination.assignment_completed(w1)
             |> Coordination.assignment_completed(w1)
             |> Coordination.assignment_empty(w1)
             |> Coordination.assignment_completed(w1)
             |> Coordination.assignment_completed(w1)
             |> Coordination.assignment_completed(w1)
             |> Coordination.worker_assignments()
  end

  test "when all assignments have been completed, the final result can be retrieved", ctx do
    [w1] = workers = create_workers(1)

    final_result =
      Coordination.new!(ctx.stream, workers, &Function.identity/1, &Function.identity/1)
      |> Coordination.assignment_completed(w1)
      |> Coordination.assignment_completed(w1)
      |> Coordination.assignment_empty(w1)
      |> Coordination.assignment_completed(w1)
      |> Coordination.assignment_completed(w1)
      |> Coordination.assignment_completed(w1)
      |> Coordination.final_result()

    assert final_result |> Enum.to_list() |> is_list()
  end

  defp create_workers(n) do
    for i <- 1..n do
      start_supervised!({Worker, i})
    end
  end
end

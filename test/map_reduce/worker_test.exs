defmodule MapReduceTest.WorkerTest do
  use ExUnit.Case

  alias MapReduce.Worker
  alias MapReduce.Assignment

  test "notifies caller of receipt of assignment" do
    worker = start_supervised!(Worker)

    assignment =
      Assignment.new!(input_fun: fn -> {:ok, [1, 2, 3]} end, process_fun: &Function.identity/1)

    {:ok, :processing} = Worker.assign(worker, assignment)
  end

  test "notifies caller of completion" do
    worker = start_supervised!(Worker)

    assignment =
      Assignment.new!(input_fun: fn -> {:ok, [1, 2, 3]} end, process_fun: &Function.identity/1)

    Worker.assign(worker, assignment)

    assert_receive {:ok, ^worker, :completed}
  end

  test "worker returns result when requested" do
    worker = start_supervised!(Worker)

    assignment =
      Assignment.new!(input_fun: fn -> {:ok, [1, 2, 3]} end, process_fun: &Enum.sum/1)

    Worker.assign(worker, assignment)

    assert_receive {:ok, ^worker, :completed}

    {:ok, 6} = Worker.result(worker, assignment)
  end

  test "worker handles new assignment after processing subsequent assignment" do
    worker = start_supervised!(Worker)

    assignment =
      Assignment.new!(input_fun: fn -> {:ok, [1, 2, 3]} end, process_fun: &Enum.sum/1)

    Worker.assign(worker, assignment)

    assert_receive {:ok, ^worker, :completed}

    assignment =
      Assignment.new!(
        input_fun: fn -> {:ok, [1, 2, 3, 4]} end,
        process_fun: &Enum.reduce(&1, 1, fn num, quot -> num * quot end)
      )

    Worker.assign(worker, assignment)

    assert_receive {:ok, ^worker, :completed}

    assert {:ok, 24} = Worker.result(worker, assignment)
  end
end

defmodule MapReduceTest.WorkerTest do
  use ExUnit.Case

  alias MapReduce.Worker
  alias MapReduce.Assignment

  test "notifies caller of receipt of assignment" do
    worker = start_supervised!(Worker)

    stream = Stream.cycle([1]) |> Stream.take(5)

    assignment =
      Assignment.new!(stream, {Function, :identity, []}, :mapper)

    {:ok, :processing} = Worker.assign(worker, assignment)
  end

  test "notifies caller of completion" do
    worker = start_supervised!(Worker)

    stream = Stream.cycle([1]) |> Stream.take(5)

    assignment =
      Assignment.new!(stream, {Function, :identity, []}, :mapper)

    Worker.assign(worker, assignment)

    assert_receive {:"$gen_cast", {:assignment_complete, ^worker}}
  end

  test "worker handles new assignment after processing subsequent assignment" do
    worker = start_supervised!(Worker)

    stream = Stream.cycle([1]) |> Stream.take(5)

    assignment =
      Assignment.new!(stream, {Function, :identity, []}, :mapper)

    Worker.assign(worker, assignment)

    assert_receive {:"$gen_cast", {:assignment_complete, ^worker}}

    Worker.assign(worker, assignment)

    assert_receive {:"$gen_cast", {:assignment_complete, ^worker}}
  end

  test "worker actually processes and writes to file if returning complete message" do
    worker = start_supervised!(Worker)

    stream = Stream.cycle([1]) |> Stream.take(3)

    assignment =
      Assignment.new!(stream, {Enum, :map, [&(&1 * 2)]}, :mapper)

    Worker.assign(worker, assignment)

    assert_receive {:"$gen_cast", {:assignment_complete, ^worker}}

    assert [2, 2, 2] = Assignment.output_stream(assignment) |> Enum.to_list()
  end

  test "worker returns empty message if input data for assignment is empty" do
    worker = start_supervised!(Worker)

    stream = [] |> Stream.map(&Function.identity/1)

    assignment =
      Assignment.new!(stream, {Enum, :map, [&(&1 * 2)]}, :mapper)

    Worker.assign(worker, assignment)

    assert_receive {:"$gen_cast", {:assignment_empty, ^worker}}
  end
end

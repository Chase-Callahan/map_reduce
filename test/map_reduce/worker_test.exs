defmodule MapReduceTest do
  use ExUnit.Case

  alias MapReduce.Worker
  alias MapReduce.Assignment

  test "notifies caller of receipt of assignment" do
    worker = start_supervised!(Worker)

    assignment = Assignment.new!()
    {:ok, :processing} = Worker.assign(worker, assignment)
  end

  test "notifies caller of completion" do
    worker = start_supervised!(Worker)

    assignment = Assignment.new!()
    Worker.assign(worker, assignment)

    assert_receive {:ok, ^worker, :completed}
  end

  test "worker returns result when requested" do
    worker = start_supervised!(Worker)

    assignment = Assignment.new!()
    Worker.assign(worker, assignment)

    assert_receive {:ok, ^worker, :completed}

    Worker.send_result(worker, assignment, self())

    assert_receive {:ok, :final_result}
  end
end

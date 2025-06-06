defmodule MapReduceTest.AssignmentTest do
  use ExUnit.Case

  alias MapReduce.Assignment

  def process_fun(nums) do
    Enum.map(nums, &(&1 * 2))
  end

  describe "new!/1" do
    test "creates new assignment" do
      stream = Stream.cycle([1])
      process_fun = &Function.identity/1
      type = :mapper

      assert %{
               input_stream: ^stream,
               process_fun: ^process_fun,
               type: ^type
             } =
               Assignment.new!(stream, process_fun, type)
    end
  end

  test "returns ok when succesfully writing to file" do
    stream = [1, 2, 3] |> Stream.map(&Function.identity/1)
    assignment = Assignment.new!(stream, {__MODULE__, :process_fun, []}, :mapper)

    assert :ok = Assignment.process(assignment)
  end

  test "reads in data and writes to a file" do
    stream = [1, 2, 3] |> Stream.map(&Function.identity/1)
    assignment = Assignment.new!(stream, {__MODULE__, :process_fun, []}, :mapper)

    Assignment.process(assignment)

    assert [2, 4, 6] ==
             Assignment.output_stream(assignment)
             |> Enum.to_list()
  end

  test "returns :empty when there is no input data" do
    stream = [] |> Stream.map(&Function.identity/1)
    assignment = Assignment.new!(stream, {__MODULE__, :process_fun, []}, :mapper)

    assert :empty = Assignment.process(assignment)
  end
end

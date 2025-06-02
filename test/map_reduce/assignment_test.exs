defmodule MapReduceTest.AssignmentTest do
  use ExUnit.Case

  alias MapReduce.Assignment

  def input_data() do
    {:ok, "some data"}
  end

  def mapper({key, value}) do
    {key, value}
  end

  describe "new!/1" do
    test "creates new assignment" do
      Assignment.new!(input_fun: &input_data/0, process_fun: &Function.identity/1)
    end

    test "missing a core function causes it to raise" do
      assert_raise KeyError, fn ->
        Assignment.new!(process_fun: &Function.identity/1)
      end

      assert_raise KeyError, fn ->
        Assignment.new!(input_fun: &input_data/0)
      end
    end
  end

  describe "id/1" do
    test "generates an int id when called" do
      assert Assignment.new!(
               input_fun: &input_data/0,
               process_fun: &Function.identity/1
             )
             |> Assignment.id()
             |> is_integer()
    end

    test "generates unique id based on assignment values" do
      id_1 =
        Assignment.new!(
          input_fun: &input_data/0,
          process_fun: &Function.identity/1
        )
        |> Assignment.id()

      id_2 =
        Assignment.new!(
          input_fun: &input_data/0,
          process_fun: &mapper/1
        )
        |> Assignment.id()

      refute id_1 == id_2
    end
  end

  describe "input_data/1" do
    test "returns input data" do
      input_data = [1, 2, 3]

      {:ok, ^input_data} =
        Assignment.new!(input_fun: fn -> {:ok, input_data} end, process_fun: &Function.identity/1)
        |> Assignment.input_data()
    end
  end

  describe "process/2" do
    test "processes input data properly" do
      assignment =
        Assignment.new!(input_fun: fn -> {:ok, [1, 2, 3]} end, process_fun: &Enum.sum/1)

      {:ok, input_data} =
        Assignment.input_data(assignment)

      output_data = Assignment.process_data(assignment, input_data)

      assert Enum.sum(input_data) == output_data
    end
  end
end

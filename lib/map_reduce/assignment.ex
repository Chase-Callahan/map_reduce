defmodule MapReduce.Assignment do
  def new!(input_stream, process_fun, type) do
    %{
      input_stream: input_stream,
      process_fun: process_fun,
      type: type
    }
  end

  def id(assignment) do
    :erlang.phash2(assignment)
  end

  def input_data(%{input_stream: input_stream}) do
    {:ok, Enum.to_list(input_stream)}
  end

  def process_data(%{process_fun: {module, function, args}}, data) do
    apply(module, function, [data | args])
  end

  def type(assignment) do
    Map.fetch!(assignment, :type)
  end

  def output_stream(_assignment) do
    Stream.cycle([1])
  end
end

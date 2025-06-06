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

  def process(assignment) do
    output =
      assignment.input_stream
      |> Enum.to_list()
      |> assignment.process_fun.()

    output_file(assignment)
    |> File.write!(:erlang.term_to_binary(output))
  end

  defp output_file(assignment) do
    Path.expand("./priv/tmp/#{id(assignment)}")
  end

  def type(assignment) do
    Map.fetch!(assignment, :type)
  end

  def output_stream(assignment) do
    path = output_file(assignment)

    if not File.exists?(path) do
      File.touch(path)
    end

    path
    |> File.stream!()
    |> Stream.map(&:erlang.binary_to_term/1)
    |> Stream.flat_map(fn map -> Enum.to_list(map) end)
  end
end

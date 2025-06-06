defmodule MapReduce.Assignment do
  def new!(input_stream, {_module, _function, _args} = process_fun, type) do
    %{
      input_stream: input_stream,
      process_fun: process_fun,
      type: type
    }
  end

  def process(%{process_fun: {module, function, args}} = assignment) do
    input_data =
      assignment.input_stream
      |> Enum.to_list()

    case input_data do
      [] ->
        :empty

      input_data ->
        output = apply(module, function, [input_data | args])

        output_file(assignment)
        |> File.write!(:erlang.term_to_binary(output))

        :ok
    end
  end

  def output_stream(assignment) do
    path = output_file(assignment)

    if not File.exists?(path) do
      File.touch(path)
    end

    contents =
      path
      |> File.read!()

    case contents do
      "" -> []
      binary -> :erlang.binary_to_term(binary)
    end
    |> Stream.map(&Function.identity/1)

    #   path
    #   |> File.stream!()
    #   |> Enum.to_list()
    #   |> dbg()

    #   path
    #   |> File.stream!()
    #   |> Stream.into([])
    #   |> Stream.flat_map(&Function.identity/1)
    #   |> Enum.to_list()
    #   |> dbg()

    #   path
    #      |> File.stream!()
    #      |> Stream.map(&:erlang.binary_to_term/1)
    #      |> Stream.flat_map(fn map -> Enum.to_list(map) end)
    # |> Enum.to_list()

    #   path
    #   |> File.stream!()
    #   |> Stream.map(&:erlang.binary_to_term/1)
    #   |> Stream.flat_map(fn map -> Enum.to_list(map) end)
  end

  defp output_file(assignment) do
    Path.expand("./priv/tmp/#{:erlang.phash2(assignment)}")
  end

  def type(assignment) do
    Map.fetch!(assignment, :type)
  end
end

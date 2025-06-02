defmodule MapReduce.Assignment do
  def new!(opts) do
    %{
      input_fun: Keyword.fetch!(opts, :input_fun),
      process_fun: Keyword.fetch!(opts, :process_fun)
    }
  end

  def id(assignment) do
    :erlang.phash2(assignment)
  end

  def input_data(%{input_fun: input_fun}) do
    input_fun.()
  end

  def process_data(%{process_fun: process_fun}, data) do
    process_fun.(data)
  end
end

defmodule MapReduce.Assignment do
  def new!() do
  end

  def id(assignment) do
    :erlang.phash2(assignment)
  end
end

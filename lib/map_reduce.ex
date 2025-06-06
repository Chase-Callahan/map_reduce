defmodule MapReduce do
  @moduledoc """
  Documentation for `MapReduce`.
  """

  @doc """
  Hello world.

  ## Examples

      iex> MapReduce.hello()
      :world

  """
  def hello do
    :world
  end

  def parse_cat(csv_string) do
    csv_string
    |> String.trim("\n")
    |> String.split(",")
    |> then(fn [name, age, type] ->
      %{
        name: name,
        age: age,
        type: type
      }
    end)
  end

  def type_to_age(cat) do
    {cat.type, cat.age}
  end

  def type_median_age({type, ages}) do
    [median_age | _] = Enum.sort(ages) |> Enum.drop(Integer.floor_div(Enum.count(ages), 2))
    [median_age]
  end

  def run do
    path = Path.expand("./priv/sample_data/cats.csv")
    stream = File.stream!(path) |> Stream.map(&parse_cat/1) |> Stream.chunk_every(10)

    {:ok, coordinator} =
      MapReduce.Coordinator.start_link(
        workers: 5,
        input_stream: stream,
        mapper_fun: &type_to_age/1,
        reduce_fun: &type_median_age/1
      )

    Process.sleep(5000)

    MapReduce.Coordinator.final_result(coordinator)
  end
end

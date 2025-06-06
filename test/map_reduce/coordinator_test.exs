defmodule MapReduceTest.CoordinatorTest do
  use ExUnit.Case

  alias MapReduce.Worker
  alias MapReduce.Coordinator

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
    # send(test_proc, {:mapping, self()})
    [{cat.type, cat.age}]
  end

  def type_median_age({type, ages}) do
    [median_age | _] = Enum.sort(ages) |> Enum.drop(Enum.count(ages) / 2)
    [median_age]
  end

  setup do
    path = Path.expand("./priv/sample_data/cats.csv")
    stream = File.stream!(path) |> Stream.map(&parse_cat/1)

    [stream: stream]
  end

  test "creates workers specified by config", ctx do
    coordinator =
      start_link_supervised!(
        {Coordinator,
         [
           workers: 5,
           input_stream: ctx.stream,
           mapper_fun: &type_to_age/1,
           reduce_fun: &type_median_age/1
         ]}
      )

    {:ok, [_, _, _, _, _]} = Coordinator.workers(coordinator)
  end

  test "creates 5 workers", ctx do
    coordinator =
      start_link_supervised!(
        {Coordinator,
         [
           workers: 5,
           input_stream: ctx.stream,
           mapper_fun: &type_to_age/1,
           reduce_fun: &type_median_age/1
         ]}
      )

    assert {:ok, [_w1, _w2, _w3, _w4, _w5]} = Coordinator.workers(coordinator)
  end

  test "farms out task to all workers", ctx do
    coordinator =
      start_link_supervised!(
        {Coordinator,
         [
           workers: 5,
           input_stream: ctx.stream,
           mapper_fun: &type_to_age/1,
           reduce_fun: &type_median_age/1
         ]}
      )

    {:ok, workers} = Coordinator.workers(coordinator)

    for worker <- workers do
      {:ok, results} = Worker.all_results(worker)
      refute results == %{}
    end
  end
end

defmodule Events.Migration do

  def run(query) do
    connect
    |> event(query)
    |> :bson.fields
  end

  def string_to_objectid(string) do
    string
    |> bitstring_to_list
    |> Enum.chunks(2)
    |> Enum.reduce(<<>>, fn(x, acc) -> acc <> <<list_to_integer(x, 16)>> end)
  end

  def objectid_to_string(<<>>), do: ""
  def objectid_to_string(<< head :: size(8), tail :: binary >>) do
    string = head
    |> integer_to_list(16)
    |> list_to_bitstring
    |> String.rjust(2, ?0)
    string <> objectid_to_string(tail)
  end

  defp connect do
    { :ok, connection } = :mongo.connect(:localhost)
    connection
  end

  defp event(connection, query) do
    { :ok, { result } } = :mongo.do(:unsafe, :master, connection, :core_push_development, fn ->
      :mongo.find(:events, query)
      |> :mongo.next
    end)
    result
  end

end

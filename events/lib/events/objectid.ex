defmodule Events.ObjectId do

  def string_to_objectid(string) do
    string
    |> bitstring_to_list
    |> Enum.chunks(2)
    |> Enum.reduce(<<>>, fn(x, acc) -> acc <> <<list_to_integer(x, 16)>> end)
  end

  def objectid_to_string(<< head :: size(8), tail :: binary >>) do
    string = head
    |> integer_to_list(16)
    |> list_to_bitstring
    |> String.rjust(2, ?0)
    string <> objectid_to_string(tail)
    |> String.downcase
  end
  def objectid_to_string(_value), do: ""

end

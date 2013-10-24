defmodule Events.DateUtils do

  def datetime_to_unixtime(datetime) do
    :calendar.datetime_to_gregorian_seconds(datetime) - epoch_gregorian_seconds
    |> :bson.secs_to_unixtime
  end

  defp epoch_gregorian_seconds do
    :calendar.datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}})
  end

end

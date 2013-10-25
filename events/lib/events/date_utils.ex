defmodule Events.DateUtils do

  def datetime_to_unixtime(datetime) do
    :calendar.datetime_to_gregorian_seconds(datetime) - epoch_gregorian_seconds
    |> :bson.secs_to_unixtime
  end

  def fixup_event_date(event_date, app_created_at, event_created_at) do
    cond do
      :bson.unixtime_to_secs(event_date) < :bson.unixtime_to_secs(app_created_at) ->
        :calendar.now_to_universal_time(app_created_at)
      :bson.unixtime_to_secs(event_date) > :bson.unixtime_to_secs(event_created_at) ->
        :calendar.now_to_universal_time(event_created_at)
      true ->
        :calendar.now_to_universal_time(event_date)
    end
  end

  defp epoch_gregorian_seconds do
    :calendar.datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}})
  end

end

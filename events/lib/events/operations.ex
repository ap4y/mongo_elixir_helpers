defmodule Events.Operation do

  @event_open_code 1100
  @event_session_code 1000
  @max_value 9000

  def upsert(@event_open_code, parent_id, {date, {hour, minute, _}}, _value, _with_session, acc)
  when parent_id != nil do
    update = [
      { :"h.#{hour}.c", 1 },
      { :"m.#{hour}.#{minute}.c", 1 }
    ]
    find_clause(parent_id, date)
    |> merge_lists(acc, update)
  end

  def upsert(@event_session_code, parent_id, {date, {hour, minute, _}}, value, true, acc)
  when parent_id != nil and value <= @max_value do
    update = [
      { :"h.#{hour}.s", 1 },
      { :"h.#{hour}.t", value },
      { :"m.#{hour}.#{minute}.s", 1 },
      { :"m.#{hour}.#{minute}.t", value }
    ]
    find_clause(parent_id, date)
    |> merge_lists(acc, update)
  end

  def upsert(_code, _parent_id, _date, _value, _with_session, acc), do: acc

  def csv_string(doc, date) do
    if doc[:device_id] != nil and doc[:value] <= @max_value do
      csv_valid_string(doc, date)
    else
      ""
    end
  end

  defp csv_valid_string(doc, date) do
    value         = doc[:value]
    { device_id}  = doc[:device_id]
    { app_id }    = doc[:app_id]

    app_id    = Events.ObjectId.objectid_to_string(app_id)
    device_id = Events.ObjectId.objectid_to_string(device_id)

    {{year, month, day}, {hour, minutes, seconds}} = date
    args = [year, month, day, hour, minutes, seconds]
    d_string = :io_lib.format("~B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B", args)

    "#{app_id},#{device_id},,#{doc[:code]},#{d_string},#{value},,\n"
  end

  defp merge_lists(id, data, update) do
    current  = data[id] || []
    merged   = Keyword.merge(current, update, fn (_k, v1, v2) -> v1 + v2 end)
    filtered = Enum.filter(data, fn {cid, _value} -> cid != id end)
    filtered ++ [{ id, merged }]
  end

  defp find_clause(parent_id, date) do
    formatted_date = Events.DateUtils.datetime_to_unixtime({ date, { 0, 0, 0 } })
    [ p: parent_id, d: formatted_date ]
  end

end

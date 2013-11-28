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

  defp merge_lists(id, data, update) do
    current  = data[id] || []
    merged   = Keyword.merge(current, update, fn (k, v1, v2) -> v1 + v2 end)
    filtered = Enum.filter(data, fn {cid, value} -> cid != id end)
    filtered ++ [{ id, merged }]
  end

  defp find_clause(parent_id, date) do
    formatted_date = Events.DateUtils.datetime_to_unixtime({ date, { 0, 0, 0 } })
    [ p: parent_id, d: formatted_date ]
  end

end

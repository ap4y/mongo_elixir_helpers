defmodule Events.Operation do

  @event_open_code 1100
  @event_session_code 1000

  def upsert(@event_open_code, parent_id, {date, {hour, minute, _}}, _value, _with_session)
  when parent_id != nil do
    update = {
      "$inc", { "h.#{hour}.c", 1 },
      "$inc", { "m.#{hour}.#{minute}.c", 1 }
    }
    { find_clause(parent_id, date), update }
  end

  def upsert(@event_session_code, parent_id, {date, {hour, minute, _}}, value, true)
  when parent_id != nil do
    update = {
      "$inc", { "h.#{hour}.s", 1 },
      "$inc", { "h.#{hour}.t", value },
      "$inc", { "m.#{hour}.#{minute}.s", 1 },
      "$inc", { "m.#{hour}.#{minute}.t", value }
    }
    { find_clause(parent_id, date), update }
  end

  def upsert(_code, _parent_id, _date, _value, _with_session), do: { :error }

  defp find_clause(parent_id, date) do
    formatted_date = Events.DateUtils.datetime_to_unixtime({ date, { 0, 0, 0 } })
    {
      :_id, { :p, parent_id, :d, formatted_date }
    }
  end

end

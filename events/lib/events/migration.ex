defmodule Events.Migration do

  @events_count 10
  @db_name :core_push_development

  def run(pool, query // {}) do
    case :resource_pool.get(pool) do
      { :ok, connection } ->
        migrate(connection, query)
      { :error, reason } -> IO.puts "Failed to connect #{reason}"
    end
  end

  defp migrate(connection, query, skip // 0, limit // @events_count) do
    :mongo.do(:unsafe, :master, connection, @db_name, fn ->
      :mongo.find(:events, query, {}, skip, limit)
      |> process_cursor(limit)
    end)
  end

  defp process_cursor(cursor, 0), do: :mongo.close_cursor(cursor)
  defp process_cursor(cursor, index) do
    case :mongo.next(cursor) do
    { data } ->
      :bson.fields(data)
      |> parse_document
      process_cursor(cursor, index - 1)
    _ -> :mongo.close_cursor(cursor)
    end
  end

  defp parse_document(doc) do
    code  = doc[:code]
    date  = :calendar.now_to_universal_time(doc[:date])
    value = doc[:value]

    case Events.Operation.upsert(code, doc[:app_id], date, value, true) do
      { find, update } -> :mongo.repsert(:"app_events.daily", find, update)
      _ -> nil
    end

    case Events.Operation.upsert(code, doc[:device_id], date, value, true) do
      { find, update } -> :mongo.repsert(:"device_events.daily", find, update)
      _ -> nil
    end

    case Events.Operation.upsert(code, doc[:notification_id], date, value, false) do
      { find, update } -> :mongo.repsert(:"notification_events.daily", find, update)
      _ -> nil
    end
  end

end

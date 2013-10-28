defmodule Events.Migration do

  @events_count 1_000
  @log_batch_size 1_000
  @db_name :core_push_development

  def run(pid, pool, query // {}, index // 0) do
    case :resource_pool.get(pool) do
      { :ok, connection } ->
        { time, _ } = :timer.tc(__MODULE__, :migrate, [connection, query, index * @events_count])
        pid <- { :finished, self, time }
      { :error, reason } -> IO.puts "Failed to connect #{reason}"
    end
  end

  def migrate(connection, query, skip // 0, limit // @events_count) do
    :mongo.do(:unsafe, :master, connection, @db_name, fn ->
      apps = :mongo.find(:apps, {}, { :created_at, 1 })
      |> :mongo.rest
      |> Enum.map(&:bson.fields(&1))

      :mongo.find(:events, query, {}, skip, limit)
      |> process_cursor(apps, limit)
    end)
  end

  def process_cursor(cursor, _apps, 0), do: :mongo.close_cursor(cursor)
  def process_cursor(cursor, apps, index) do
    case :mongo.next(cursor) do
    { data } ->
      doc = :bson.fields(data)
      app_created_at = Enum.filter(apps, fn(app) -> app[:_id] == doc[:app_id] end)
      |> Enum.map(fn(app) -> app[:created_at] end)
      |> Enum.first
      parse_document(doc, app_created_at)

      if (rem(index, @log_batch_size) == 0), do: IO.puts "#{index}"
      process_cursor(cursor, apps, index - 1)

    _ -> :mongo.close_cursor(cursor)
    end
  end

  defp parse_document(doc, app_created_at) when app_created_at != nil do
    code  = doc[:code]
    date  = Events.DateUtils.fixup_event_date(doc[:date], app_created_at, doc[:created_at])
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
  defp parse_document(_doc, _apps), do: nil

end

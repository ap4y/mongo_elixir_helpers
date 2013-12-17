defmodule Events.Migration do

  @events_count 700_000
  @log_batch_size 100_000
  @db_name :core_push

  def run(pid, pool, query // {}, index // 0) do
    case :resource_pool.get(pool) do
      { :ok, connection } ->
        { time, _ } = :timer.tc(__MODULE__, :migrate, [connection, query, index * @events_count])
        pid <- { :finished, self, time }
      { :error, reason } -> IO.puts "Failed to connect #{reason}"
    end
  end

  def migrate(connection, query, skip // 0, limit // @events_count) do
    IO.puts "starting #{inspect self}, #{skip}, #{limit}"
    :mongo.do(:unsafe, :master, connection, @db_name, fn ->
      apps = :mongo.find(:apps, {}, { :created_at, 1 })
      |> :mongo.rest
      |> Enum.map(&:bson.fields(&1))

      :mongo.find(:events, query, {}, skip, limit)
      |> process_cursor(apps, [], "", limit)
    end)
  end

  def process_cursor(cursor, _apps, acc, csv, 0) do
    :mongo.close_cursor(cursor)
    perform_upsert(acc)
    write_csv(csv, 0)
  end
  def process_cursor(cursor, apps, acc, csv, index) do
    case :mongo.next(cursor) do
    { data } ->
      doc = :bson.fields(data)
      app_created_at = Enum.filter(apps, fn(app) -> app[:_id] == doc[:app_id] end)
      |> Enum.map(fn(app) -> app[:created_at] end)
      |> Enum.first
      { c_counters, c_csv } = parse_document(doc, app_created_at, acc, csv)

      if (rem(index, @log_batch_size) == 0 && index != @events_count) do
        IO.puts "#{inspect self}:#{index}"
        # perform_upsert(c_counters)
        # c_counters = []
        write_csv(c_csv, index)
        c_csv = ""
      end
      process_cursor(cursor, apps, c_counters, c_csv, index - 1)

    _ ->
      :mongo.close_cursor(cursor)
      perform_upsert(acc)
      write_csv(csv, 0)
    end
  end

  defp perform_upsert(events) do
    Enum.each(events, fn { id, values } ->
      find = { :_id, { :p, id[:p], :d, id[:d] } }
      update = Enum.reduce(values, [], fn({ key, value }, acc) ->
        acc ++ [ "$inc", { atom_to_binary(key), value }]
      end)
      |> list_to_tuple
      :mongo.repsert(:"app_events.daily", find, update)

      { p_id } = id[:p]
      update_log = Enum.reduce(values, "", fn({ key, value }, acc) ->
        acc <> "      '#{key}': #{value},\n"
      end)
      |> String.strip

      event_log = """
      db.app_events.daily.update(
        {
          _id: {
            p: ObjectId('#{Events.ObjectId.objectid_to_string(p_id)}'),
            d: new Date(#{:bson.unixtime_to_secs(id[:d])*1000})
          }
        },
        {
          $inc: {
            #{update_log}
          }
        },
        { upsert: true}
      )
      """
      File.write("migrations/migration_#{inspect self}.js", event_log, [ :append ])
    end)
  end

  defp parse_document(doc, app_created_at, acc, csv) when app_created_at != nil do
    code  = doc[:code]
    date  = Events.DateUtils.fixup_event_date(doc[:date], app_created_at, doc[:created_at])
    value = doc[:value]

    c_counters = Events.Operation.upsert(code, doc[:app_id], date, value, true, acc)
    c_csv = csv <> Events.Operation.csv_string(doc, date)
    { c_counters, c_csv }
  end
  defp parse_document(_doc, _app_created_at, acc, csv), do: { acc, csv }

  defp write_csv(csv, index) do
    File.write("csv/events_#{inspect self}_#{index}.csv", csv, [ :append ])
  end

end

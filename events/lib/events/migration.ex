defmodule Events.Migration do

  @events_count 37_300_000
  @log_batch_size 1_000
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
    IO.puts "starting #{inspect self}"
    :mongo.do(:unsafe, :master, connection, @db_name, fn ->
      apps = :mongo.find(:apps, {}, { :created_at, 1 })
      |> :mongo.rest
      |> Enum.map(&:bson.fields(&1))

      :mongo.find(:events, query, {}, skip, limit)
      |> process_cursor(apps, [], limit)
    end)
  end

  def process_cursor(cursor, _apps, acc, 0) do
    :mongo.close_cursor(cursor)
    perform_upsert(acc)
  end
  def process_cursor(cursor, apps, acc, index) do
    case :mongo.next(cursor) do
    { data } ->
      doc = :bson.fields(data)
      app_created_at = Enum.filter(apps, fn(app) -> app[:_id] == doc[:app_id] end)
      |> Enum.map(fn(app) -> app[:created_at] end)
      |> Enum.first
      updated = parse_document(doc, app_created_at, acc)

      if (rem(index, @log_batch_size) == 0), do: IO.puts "#{inspect self}:#{index}"
      process_cursor(cursor, apps, updated, index - 1)

    _ ->
      :mongo.close_cursor(cursor)
      perform_upsert(acc)
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
    end)
  end

  defp parse_document(doc, app_created_at, acc) when app_created_at != nil do
    code  = doc[:code]
    date  = Events.DateUtils.fixup_event_date(doc[:date], app_created_at, doc[:created_at])
    value = doc[:value]

    Events.Operation.upsert(code, doc[:app_id], date, value, true, acc)
  end
  defp parse_document(_doc, _apps), do: nil

end

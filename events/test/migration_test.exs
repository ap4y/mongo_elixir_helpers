defmodule MigrationTest do
  use ExUnit.Case

  import Events.Migration

  @document {
    :app_id, {"foo"},
    :code, 1000,
    :date, Events.DateUtils.datetime_to_unixtime({{2012,11,04},{10,15,23}}),
    :value, 100,
    :created_at, Events.DateUtils.datetime_to_unixtime({{2012,12,05},{0,0,0}}),
  }

  @apps [
    [ _id: {"foo"}, created_at: {1349, 665482, 0} ]
  ]

  test "it correctly iterates through cursor" do
    :meck.new(:mongo)
    :meck.expect(:mongo, :next, fn(_) -> {@document} end)
    :meck.expect(:mongo, :close_cursor, fn(cursor) ->
      assert cursor == "foo"
    end)
    :meck.expect(:mongo, :repsert, fn(collection, find, update) ->
      assert collection == :"app_events.daily"
      assert find == {
        :_id, {:p, {"foo"}, :d, Events.DateUtils.datetime_to_unixtime({{2012,11,04},{0,0,0}}) }
      }
      assert update == {
        "$inc", {"h.10.s", 1},
        "$inc", {"h.10.t", 100},
        "$inc", {"m.10.15.s", 1},
        "$inc", {"m.10.15.t", 100}
      }
    end)

    process_cursor("foo", @apps, 2)

    assert :meck.validate(:mongo) == true
  end
end

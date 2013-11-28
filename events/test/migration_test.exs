defmodule MigrationTest do
  use ExUnit.Case

  import Events.Migration

  @document {
    :app_id, {"foo"},
    :device_id, {"bar"},
    :notification_id, {"baz"},
    :code, 1000,
    :date, Events.DateUtils.datetime_to_unixtime({{2012,11,04},{10,15,23}}),
    :value, 100,
    :created_at, Events.DateUtils.datetime_to_unixtime({{2012,12,05},{0,0,0}}),
  }

  @document2 {
    :app_id, {"foo"},
    :device_id, {"bar"},
    :notification_id, {"baz"},
    :code, 1100,
    :date, Events.DateUtils.datetime_to_unixtime({{2012,11,04},{10,15,23}}),
    :value, 100,
    :created_at, Events.DateUtils.datetime_to_unixtime({{2012,12,05},{0,0,0}}),
  }

  @apps [
    [ _id: {"foo"}, created_at: {1349, 665482, 0} ]
  ]

  setup do
    :meck.new(:mongo)
  end

  teardown do
    :meck.unload(:mongo)
  end

  test "it correctly iterates through cursor. app session mock" do
    :meck.expect(:mongo, :next, fn(_) -> {@document} end)
    :meck.expect(:mongo, :close_cursor, fn(cursor) ->
      assert cursor == "foo"
    end)
    :meck.expect(:mongo, :repsert, fn(collection, find, update) ->
      case find do
        { :_id, {:p, {"bar"}, :d, {1330, 732800, 0}} } ->
          assert update == {
            "$inc", {"h.10.s", 11},
            "$inc", {"h.10.t", 110},
            "$inc", {"m.10.15.s", 13},
            "$inc", {"m.10.15.t", 140}
          }
        { :_id, {:p, {"foo"}, :d, {1351, 987200, 0}} } ->
          assert update == {
            "$inc", {"h.10.s", 13},
            "$inc", {"h.10.t", 310},
            "$inc", {"m.10.15.s", 15},
            "$inc", {"m.10.15.t", 340}
          }
        _ -> flunk "Wrong collection"
      end
    end)

    initial = [
      {
        [p: {"foo"}, d: {1351, 987200, 0}],
        [
          {:"h.10.s", 11},
          {:"h.10.t", 110},
          {:"m.10.15.s", 13},
          {:"m.10.15.t", 140}
        ]
      },
      {
        [p: {"bar"}, d: {1330, 732800, 0}],
        [
          {:"h.10.s", 11},
          {:"h.10.t", 110},
          {:"m.10.15.s", 13},
          {:"m.10.15.t", 140}
        ]
      }
    ]
    process_cursor("foo", @apps, initial, 2)

    assert :meck.validate(:mongo) == true
  end

  test "it correctly iterates through cursor. app open mock" do
    :meck.expect(:mongo, :next, fn(_) -> {@document2} end)
    :meck.expect(:mongo, :close_cursor, fn(cursor) ->
      assert cursor == "foo"
    end)

    :meck.expect(:mongo, :repsert, fn(collection, find, update) ->
      case find do
        { :_id, {:p, {"bar"}, :d, {1330, 732800, 0}} } ->
          assert update == {
            "$inc", {"h.10.c", 10},
            "$inc", {"m.10.15.c", 12}
          }
        { :_id, {:p, {"foo"}, :d, {1351, 987200, 0}} } ->
          assert update == {
            "$inc", {"h.10.c", 12},
            "$inc", {"m.10.15.c", 14}
          }
        _ -> flunk "Wrong collection"
      end
    end)

    initial = [
      {
        [p: {"foo"}, d: {1351, 987200, 0}],
        [
          {:"h.10.c", 10},
          {:"m.10.15.c", 12}
        ]
      },
      {
        [p: {"bar"}, d: {1330, 732800, 0}],
        [
          {:"h.10.c", 10},
          {:"m.10.15.c", 12}
        ]
      }
    ]
    process_cursor("foo", @apps, initial, 2)

    assert :meck.validate(:mongo) == true
  end
end

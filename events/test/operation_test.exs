defmodule OperationTest do
  use ExUnit.Case

  import Events.Operation

  @event_date {{2012, 03, 03},{10, 15, 25}}
  @document [
    _id: {"bar"},
    app_id: {"foo"},
    code: 1000
  ]

  test "event with nil parent_id should return accumulator value" do
    assert upsert(1000, nil, @event_date, 100, true, [ 'foo' ]) == [ 'foo' ]
    assert upsert(1100, nil, @event_date, 100, true, [ 'bar' ]) == [ 'bar' ]
  end

  test "event with with incorrect code should return accumulator value" do
    assert upsert(777, nil, @event_date, 100, true, [ 'foo' ]) == [ 'foo' ]
  end

  test "event with with value bigger than maximum should return :error" do
    assert upsert(1000, nil, @event_date, 10000, true, [ 'foo' ]) == [ 'foo' ]
  end

  test "app open event should produce correct find and update clause" do
    value = upsert(1100, {"foo"}, @event_date, 100, true, [])
    assert value == [{
      [p: {"foo"}, d: {1330, 732800, 0}],
      [
        {:"h.10.c", 1},
        {:"m.10.15.c", 1}
      ]
    }]
  end

  test "app open event should accumulate counters" do
    initial = [
      {
        [p: {"foo"}, d: {1330, 732800, 0}],
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
    value = upsert(1100, {"foo"}, @event_date, 100, true, initial)
    assert value == [
      {
        [p: {"bar"}, d: {1330, 732800, 0}],
        [
          {:"h.10.c", 10},
          {:"m.10.15.c", 12}
        ]
      },
      {
        [p: {"foo"}, d: {1330, 732800, 0}],
        [
          {:"h.10.c", 11},
          {:"m.10.15.c", 13}
        ]
      }
    ]
  end

  test "app session event should produce correct find and update clause" do
    value = upsert(1000, {"foo"}, @event_date, 100, true, [])
    assert value == [{
      [p: {"foo"}, d: {1330, 732800, 0}],
      [
        {:"h.10.s", 1},
        {:"h.10.t", 100},
        {:"m.10.15.s", 1},
        {:"m.10.15.t", 100}
      ]
    }]
  end

  test "app session event should accumulate counters" do
    initial = [{
      [p: {"foo"}, d: {1330, 732800, 0}],
      [
        {:"h.10.s", 11},
        {:"h.10.t", 110},
        {:"m.10.15.s", 13},
        {:"m.10.15.t", 140}
      ]
    }]
    value = upsert(1000, {"foo"}, @event_date, 100, true, initial)
    assert value == [{
      [p: {"foo"}, d: {1330, 732800, 0}],
      [
        {:"h.10.s", 12},
        {:"h.10.t", 210},
        {:"m.10.15.s", 14},
        {:"m.10.15.t", 240}
      ]
    }]
  end

  test "events with large value return empty string" do
    document = [
      _id:        {"bar"},
      app_id:     {"foo"},
      device_id:  {"baz"},
      code:       1000,
      value:      10000
    ]
    assert csv_string(document, @event_date) == ""
  end

  test "events without device_id return empty string" do
    document = [
      _id:     {"bar"},
      app_id:  {"foo"},
      code:    1000,
      value:   100
    ]
    assert csv_string(document, @event_date) == ""
  end

  test "events return correct csv string" do
    document = [
      _id:        {"bar"},
      app_id:     {"foo"},
      device_id:  {"baz"},
      code:       1000,
      value:      1000
    ]
    assert csv_string(document, @event_date) == "666f6f,62617a,,1000,2012-03-03 10:15:25,1000,,\n"
  end

  test "app open events return correct csv string" do
    document = [
      _id:        {"bar"},
      app_id:     {"foo"},
      device_id:  {"baz"},
      code:       1100
    ]
    assert csv_string(document, @event_date) == "666f6f,62617a,,1100,2012-03-03 10:15:25,,,\n"
  end

end

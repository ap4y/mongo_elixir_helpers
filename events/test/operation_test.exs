defmodule OperationTest do
  use ExUnit.Case

  import Events.Operation

  @event_date {{2012, 03, 03},{10, 15, 25}}

  test "event with nil parent_id should return :error" do
    assert upsert(1000, nil, @event_date, 100, true) == { :error }
    assert upsert(1100, nil, @event_date, 100, true) == { :error }
  end

  test "event with with incorrect code should return :error" do
    assert upsert(777, nil, @event_date, 100, true) == { :error }
  end

  test "app open event should produce correct find and update clause" do
    { find, update } = upsert(1100, {"foo"}, @event_date, 100, true)
    assert find == {
      :_id, {:p, {"foo"}, :d, {1330, 732800, 0} }
    }
    assert update == {
      "$inc", {"h.10.c", 1},
      "$inc", {"m.10.15.c", 1}
    }
  end

  test "app session event should produce correct find and update clause" do
    { find, update } = upsert(1000, {"foo"}, @event_date, 100, true)
    assert find == {
      :_id, {:p, {"foo"}, :d, {1330, 732800, 0} }
    }
    assert update == {
      "$inc", {"h.10.s", 1},
      "$inc", {"h.10.t", 100},
      "$inc", {"m.10.15.s", 1},
      "$inc", {"m.10.15.t", 100}
    }
  end
end

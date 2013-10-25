defmodule DateUtilsTest do
  use ExUnit.Case

  import Events.DateUtils

  test "datetime_to_unixtime" do
    assert datetime_to_unixtime({{2012, 03, 03},{10,15,25}}) == {1330,769725,0}
  end

  test "when event_date < app_create_at it fixups to app_create_at" do
    event_date = datetime_to_unixtime({{1990, 03, 03}, {0,0,0}})
    app_created_at = datetime_to_unixtime({{2012, 03, 03}, {0,0,0}})

    assert fixup_event_date(event_date, app_created_at, nil) == {{2012,3,3},{0,0,0}}
  end

  test "when event_date > event_create_at it fixups to event_create_at" do
    event_date = datetime_to_unixtime({{2013, 03, 03}, {0,0,0}})
    app_created_at = datetime_to_unixtime({{2011, 03, 03}, {0,0,0}})
    event_created_at = datetime_to_unixtime({{2012, 03, 03}, {0,0,0}})

    assert fixup_event_date(event_date, app_created_at, event_created_at) == {{2012,3,3},{0,0,0}}
  end

  test "when app_created_at < event_date < event_create_at it fixups to event_date" do
    event_date = datetime_to_unixtime({{2012, 03, 03}, {0,0,0}})
    app_created_at = datetime_to_unixtime({{2011, 03, 03}, {0,0,0}})
    event_created_at = datetime_to_unixtime({{2013, 03, 03}, {0,0,0}})

    assert fixup_event_date(event_date, app_created_at, event_created_at) == {{2012,3,3},{0,0,0}}
  end

end

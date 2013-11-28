defmodule ObjectIdTest do
  use ExUnit.Case

  import Events.ObjectId

  test "it converts string to objectid" do
    assert string_to_objectid("507242caaed3564b0c00001c") == <<80,114,66,202,174,211,86,75,12,0,0,28>>
  end

  test "it converts objectid to string" do
    assert objectid_to_string(<<80,114,66,202,174,211,86,75,12,0,0,28>>) == "507242caaed3564b0c00001c"
  end

end

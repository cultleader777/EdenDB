#[cfg(test)]
use super::common::assert_compiles_data;
#[cfg(test)]
use super::common::assert_test_validaton_exception;
#[cfg(test)]
use crate::checker::errors::DatabaseValidationError;
#[cfg(test)]
use serde_json::json;

#[test]
fn test_lua_corrupt_global_variable() {
    assert_test_validaton_exception(
        DatabaseValidationError::LuaDataTableError {
            error: "error converting Lua nil to table".to_string(),
        },
        r#"
TABLE stuff {
    id INT
}

INCLUDE LUA {
-- there are all kinds of douchebags in the world
__do_not_refer_to_this_internal_value_in_your_code_dumbo__ = nil
}
"#,
    )
}

#[test]
fn test_lua_invalid_key_type() {
    assert_test_validaton_exception(
        DatabaseValidationError::LuaDataTableInvalidKeyTypeIsNotString {
            found_value: "integer 123".to_string(),
        },
        r#"
TABLE stuff {
    id INT
}

INCLUDE LUA {
data(123, {})
}
"#,
    )
}

#[test]
fn test_lua_invalid_utf8_key() {
    assert_test_validaton_exception(
        DatabaseValidationError::LuaDataTableInvalidKeyTypeIsNotValidUtf8String {
            lossy_value: "�������".to_string(),
            bytes: vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        },
        r#"
TABLE stuff {
    id INT
}

INCLUDE LUA {

function utf8_from(t)
  local bytearr = {}
  for _, v in ipairs(t) do
    local utf8byte = v < 0 and (0xff + v + 1) or v
    table.insert(bytearr, string.char(utf8byte))
  end
  return table.concat(bytearr)
end

data(utf8_from({255,255,255,255,255,255,255}), {})

}
"#,
    )
}

#[test]
fn test_lua_invalid_value_type() {
    assert_test_validaton_exception(
        DatabaseValidationError::LuaDataTableInvalidTableValue {
            found_value: "string \"sup bois\"".to_string(),
        },
        r#"
TABLE stuff {
    id INT
}

INCLUDE LUA {
-- there are all kinds of douchebags in the world
__do_not_refer_to_this_internal_value_in_your_code_dumbo__['rekt'] = 'sup bois'
}
"#,
    )
}

#[test]
fn test_lua_data_no_such_table() {
    assert_test_validaton_exception(
        DatabaseValidationError::LuaDataTableNoSuchTable {
            expected_insertion_table: "stoof".to_string(),
        },
        r#"
TABLE stuff {
    id INT
}

INCLUDE LUA {
    data('stoof', {moo = 1})
}
"#,
    )
}

#[test]
fn test_lua_data_invalid_record_value() {
    assert_test_validaton_exception(
        DatabaseValidationError::LuaDataTableInvalidRecordValue {
            found_value: "string \"hello bois\"".to_string(),
        },
        r#"
TABLE stuff {
    id INT
}

INCLUDE LUA {
    data('stuff', 'hello bois')
}
"#,
    )
}

#[test]
fn test_lua_data_invalid_record_column_name_value() {
    assert_test_validaton_exception(
        DatabaseValidationError::LuaDataTableInvalidRecordColumnNameValue {
            found_value: "integer 123".to_string(),
        },
        r#"
TABLE stuff {
    id INT
}

INCLUDE LUA {
    the_table = {}
    the_table[123] = 'lol'
    data('stuff', the_table)
}
"#,
    )
}

#[test]
fn test_lua_data_invalid_record_column_name_utf8() {
    assert_test_validaton_exception(
        DatabaseValidationError::LuaDataTableRecordInvalidColumnNameUtf8String {
            lossy_value: "�������".to_string(),
            bytes: vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        },
        r#"
TABLE stuff {
    id INT
}

INCLUDE LUA {

function utf8_from(t)
  local bytearr = {}
  for _, v in ipairs(t) do
    local utf8byte = v < 0 and (0xff + v + 1) or v
    table.insert(bytearr, string.char(utf8byte))
  end
  return table.concat(bytearr)
end

the_table = {}
the_table[utf8_from({255,255,255,255,255,255,255})] = 'lol'
data('stuff', the_table)
}
"#,
    )
}

#[test]
fn test_lua_data_invalid_record_try_insert_function() {
    assert_test_validaton_exception(
        DatabaseValidationError::LuaDataTableRecordInvalidColumnValue {
            column_name: "id".to_string(),
            column_value: "*lua function*".to_string(),
        },
        r#"
TABLE stuff {
    id INT
}

INCLUDE LUA {
data('stuff', { id = function() return 1 + 2 end })
}
"#,
    )
}

#[test]
fn test_lua_data_exclusive_insert() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExclusiveDataDefinedMultipleTimes {
            table_name: "stuff".to_string(),
        },
        r#"
TABLE stuff {
    id INT
}

DATA EXCLUSIVE stuff {
    777
}

INCLUDE LUA {
data('stuff', { id = 13 })
}
"#,
    )
}

#[test]
fn test_lua_data_exclusive_insert_struct() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExclusiveDataDefinedMultipleTimes {
            table_name: "stuff".to_string(),
        },
        r#"
TABLE stuff {
    id INT
}

DATA STRUCT EXCLUSIVE stuff {
    id: 777
}

INCLUDE LUA {
data('stuff', { id = 13 })
}
"#,
    )
}

#[test]
fn test_lua_data_insertion() {
    assert_compiles_data(
        r#"
TABLE stuff {
    id INT
}

INCLUDE LUA {
data('stuff', { id = 777 })
}
"#,
        json!({
            "stuff": [
                {"id": 777.0}
            ]
        }),
    )
}

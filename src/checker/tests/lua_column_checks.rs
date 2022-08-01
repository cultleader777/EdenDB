#[cfg(test)]
use serde_json::json;
#[cfg(test)]
use crate::checker::errors::DatabaseValidationError;
#[cfg(test)]
use super::common::assert_compiles_data;
#[cfg(test)]
use super::common::assert_test_validaton_exception_return_error;

#[test]
fn test_lua_extra_runtime_error() {
    let err = assert_test_validaton_exception_return_error(
        r#"
INCLUDE LUA {
    this is invalid lua syntax
}

TABLE moo {
    id INT,
}
        "#,
    );
    if let DatabaseValidationError::LuaSourcesLoadError { error, source_file } = err {
        assert!(error.contains("syntax error"));
        assert_eq!(source_file, "inline");
    } else { panic!() }
}

#[test]
fn test_lua_check_synax_error() {
    let e = assert_test_validaton_exception_return_error(
        r#"
TABLE cholo {
    id INT PRIMARY KEY,
    CHECK { bozoso (() * moo }
}

DATA cholo {
    0
}
        "#,
    );

    match e {
        DatabaseValidationError::LuaCheckExpressionLoadError { error, table_name, expression } => {
            assert!(error.contains("syntax error"));
            assert_eq!(table_name, "cholo");
            assert_eq!(expression, " bozoso (() * moo ");
        }
        _ => panic!()
    }
}

#[test]
fn test_lua_bad_type_error() {
    let e = assert_test_validaton_exception_return_error(
        r#"
TABLE cholo {
    id INT PRIMARY KEY,
    CHECK { id * 3 }
}

DATA cholo {
    2
}
        "#,
    );

    match e {
        DatabaseValidationError::LuaCheckEvaluationErrorUnexpectedReturnType {
            error, table_name, expression, column_names, row_values
        } => {
            assert_eq!(table_name, "cholo");
            assert_eq!(expression, " id * 3 ");
            assert_eq!(column_names, vec!["id".to_string()]);
            assert_eq!(row_values, vec!["2".to_string()]);
            assert_eq!(error, "Unexpected expression return value, expected boolean, got integer");
        }
        e => {
            panic!("{e}")
        }
    }
}

#[test]
fn test_lua_check_fails() {
    let e = assert_test_validaton_exception_return_error(
        r#"
TABLE cholo {
    id INT PRIMARY KEY,
    CHECK { id > 7 }
}

DATA cholo {
    2
}
        "#,
    );

    match e {
        DatabaseValidationError::LuaCheckEvaluationFailed {
            table_name, expression, column_names, row_values, error
         } => {
            assert_eq!(table_name, "cholo");
            assert_eq!(expression, " id > 7 ");
            assert_eq!(column_names, vec!["id".to_string()]);
            assert_eq!(row_values, vec!["2".to_string()]);
            assert_eq!(error, "Expression check for the row didn't pass.");
        }
        e => {
            panic!("{e}")
        }
    }
}

#[test]
fn test_lua_check_succeeds() {
    assert_compiles_data(
        r#"
TABLE cholo {
    id INT PRIMARY KEY,
    some_f FLOAT,
    some_text TEXT,
    CHECK { id > 0 and id < 10 },
    CHECK { some_f < 10.0 },
    CHECK { string.find(some_text, "salami") ~= nil },
}

DATA cholo {
    2, 3.5, "and a salami!"
}
        "#,
        json!({
            "cholo": [
                {"id": 2.0, "some_f": 3.5, "some_text": "and a salami!"}
            ]
        })
    );
}

#[test]
fn test_lua_check_succeeds_multiline() {
    assert_compiles_data(
        r#"
TABLE cholo {
    id INT PRIMARY KEY,
    some_f FLOAT,
    some_text TEXT,
    CHECK {
        local firstOk = id > 0
        local secondOk = id < 10
        firstOk and secondOk
    },
}

DATA cholo {
    2, 3.5, "and a salami!"
}
        "#,
        json!({
            "cholo": [
                {"id": 2.0, "some_f": 3.5, "some_text": "and a salami!"}
            ]
        })
    );
}

#[test]
fn test_lua_check_succeeds_multiline_explicit_returns() {
    assert_compiles_data(
        r#"
TABLE cholo {
    id INT PRIMARY KEY,
    some_f FLOAT,
    some_text TEXT,
    CHECK {
        do return true end
        return false
    },
}

DATA cholo {
    2, 3.5, "and a salami!"
}
        "#,
        json!({
            "cholo": [
                {"id": 2.0, "some_f": 3.5, "some_text": "and a salami!"}
            ]
        })
    );
}

#[test]
fn test_lua_check_succeeds_extra_runtime() {
    assert_compiles_data(
        r#"
INCLUDE LUA {
    function isSalamiGood(salami)
        return string.find(salami, "salami") ~= nil
    end
}

TABLE cholo {
    id INT PRIMARY KEY,
    some_f FLOAT,
    some_text TEXT,
    CHECK { isSalamiGood(some_text) },
}

DATA cholo {
    2, 3.5, "and a salami!"
}
        "#,
        json!({
            "cholo": [
                {"id": 2.0, "some_f": 3.5, "some_text": "and a salami!"}
            ]
        })
    );
}
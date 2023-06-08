#[cfg(test)]
use super::common::assert_compiles_data;
#[cfg(test)]
use super::common::assert_test_validaton_exception;
#[cfg(test)]
use crate::checker::errors::DatabaseValidationError;
#[cfg(test)]
use serde_json::json;

#[test]
fn test_detached_default_undefined() {
    assert_test_validaton_exception(
        DatabaseValidationError::DetachedDefaultUndefined {
            table: "kukushkin".to_string(),
            column: "some_column".to_string(),
        },
        r#"
TABLE kukushkin {
    some_column TEXT DETACHED DEFAULT,
}
        "#,
    );
}

#[test]
fn test_detached_default_defined_multiple_times() {
    assert_test_validaton_exception(
        DatabaseValidationError::DetachedDefaultDefinedMultipleTimes {
            table: "kukushkin".to_string(),
            column: "some_column".to_string(),
            expression_a: "123".to_string(),
            expression_b: "321".to_string(),
        },
        r#"
TABLE kukushkin {
    some_column TEXT DETACHED DEFAULT,
}

DEFAULTS {
    kukushkin.some_column 123,
    kukushkin.some_column 321,
}

        "#,
    );
}

#[test]
fn test_detached_default_non_existing_table() {
    assert_test_validaton_exception(
        DatabaseValidationError::DetachedDefaultNonExistingTable {
            table: "non_existing".to_string(),
            column: "some_column".to_string(),
            expression: "123".to_string(),
        },
        r#"
TABLE kukushkin {
    some_column TEXT DETACHED DEFAULT,
}

DEFAULTS {
    non_existing.some_column 123,
}

        "#,
    );
}

#[test]
fn test_detached_default_non_existing_column() {
    assert_test_validaton_exception(
        DatabaseValidationError::DetachedDefaultNonExistingColumn {
            table: "kukushkin".to_string(),
            column: "non_existing".to_string(),
            expression: "123".to_string(),
        },
        r#"
TABLE kukushkin {
    some_column TEXT DETACHED DEFAULT,
}

DEFAULTS {
    kukushkin.non_existing 123,
}

        "#,
    );
}

#[test]
fn test_detached_default_bad_value() {
    assert_test_validaton_exception(
        DatabaseValidationError::DetachedDefaultBadValue {
            table: "kukushkin".to_string(),
            column: "some_column".to_string(),
            expected_type: crate::checker::types::DBType::Int,
            value: "hello".to_string(),
            error: "Cannot parse value to expected type for this column".to_string(),
        },
        r#"
TABLE kukushkin {
    some_column INT DETACHED DEFAULT,
}

DEFAULTS {
    kukushkin.some_column hello,
}

        "#,
    );
}

#[test]
fn test_detached_default_smoke() {
    assert_compiles_data(
        r#"
TABLE kukushkin {
    id INT PRIMARY KEY,
    int_col INT DETACHED DEFAULT,
    bool_col BOOL DETACHED DEFAULT,
    text_col TEXT DETACHED DEFAULT,
    float_col FLOAT DETACHED DEFAULT,
}

DEFAULTS {
    kukushkin.int_col 7,
    kukushkin.bool_col true,
    kukushkin.text_col 'hello detached defaults',
    kukushkin.float_col 7.77,
}

DATA kukushkin { 1 }

        "#,
        json!({
            "kukushkin": [
                {
                    "id": 1.0,
                    "int_col": 7.0,
                    "bool_col": true,
                    "text_col": "hello detached defaults",
                    "float_col": 7.77,
                }
            ]
        }),
    );
}

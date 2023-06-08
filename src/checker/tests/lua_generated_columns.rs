#[cfg(test)]
use super::common::assert_compiles_data;
#[cfg(test)]
use super::common::assert_test_validaton_exception;
#[cfg(test)]
use super::common::assert_test_validaton_exception_return_error;
#[cfg(test)]
use crate::checker::errors::DatabaseValidationError;
#[cfg(test)]
use serde_json::json;

#[test]
fn test_lua_computed_column_cannot_be_primary_key() {
    assert_test_validaton_exception(
        DatabaseValidationError::PrimaryOrForeignKeysCannotHaveComputedValue {
            table_name: "cholo".to_string(),
            column_name: "id".to_string(),
        },
        r#"
TABLE cholo {
    id INT GENERATED AS { 1 } PRIMARY KEY,
}
        "#,
    );
}

#[test]
fn test_lua_computed_column_cannot_be_foreign_key() {
    assert_test_validaton_exception(
        DatabaseValidationError::PrimaryOrForeignKeysCannotHaveComputedValue {
            table_name: "bolo".to_string(),
            column_name: "for_id".to_string(),
        },
        r#"
TABLE cholo {
    id INT PRIMARY KEY,
}

TABLE bolo {
    for_id REF cholo GENERATED AS { 1 },
}
        "#,
    );
}

#[test]
fn test_lua_computed_column_cannot_be_child_pkey() {
    assert_test_validaton_exception(
        DatabaseValidationError::PrimaryOrForeignKeysCannotHaveComputedValue {
            table_name: "bolo".to_string(),
            column_name: "child_id".to_string(),
        },
        r#"
TABLE cholo {
    id INT PRIMARY KEY,
}

TABLE bolo {
    child_id INT GENERATED AS { 1 } PRIMARY KEY CHILD OF cholo,
}
        "#,
    );
}

#[test]
fn test_lua_computed_column_cannot_have_default_value() {
    assert_test_validaton_exception(
        DatabaseValidationError::DefaultValueAndComputedValueAreMutuallyExclusive {
            table_name: "cholo".to_string(),
            column_name: "id".to_string(),
        },
        r#"
TABLE cholo {
    id INT DEFAULT 1 GENERATED AS { 2 },
}
        "#,
    );
}

#[test]
fn test_lua_computed_column_basic() {
    assert_compiles_data(
        r#"
INCLUDE LUA {
    function round(num, numDecimalPlaces)
        local mult = 10^(numDecimalPlaces or 0)
        return math.floor(num * mult + 0.5) / mult
    end
}

TABLE cholo {
    id INT,
    t TEXT,
    f FLOAT,
    computed_int INT GENERATED AS { id + 2 },
    computed_float FLOAT GENERATED AS { round(computed_int * 2.4, 1) },
    computed_text TEXT GENERATED AS { "hello " .. computed_float },
    computed_float_of_int FLOAT GENERATED AS { computed_int * 3 },
    computed_text_of_text TEXT GENERATED AS { t .. "!" },
    computed_float_of_float FLOAT GENERATED AS { f * 2.0 },
    computed_is_even BOOL GENERATED AS { id % 2 == 0 },
}

DATA cholo {
    1, a, 1.1;
    2, b, 2.2;
}
        "#,
        json!({
            "cholo": [
                {
                    "id": 1.0, "t": "a", "f": 1.1,
                    "computed_int": 3.0, "computed_float": 7.2,
                    "computed_text": "hello 7.2",
                    "computed_float_of_int": 9.0,
                    "computed_text_of_text": "a!",
                    "computed_float_of_float": 2.2,
                    "computed_is_even": false,
                },
                {
                    "id": 2.0, "t": "b", "f": 2.2,
                    "computed_int": 4.0, "computed_float": 9.6,
                    "computed_text": "hello 9.6",
                    "computed_float_of_int": 12.0,
                    "computed_text_of_text": "b!",
                    "computed_float_of_float": 4.4,
                    "computed_is_even": true,
                },
            ]
        }),
    );
}

#[test]
fn test_lua_computed_column_basic_with_checks() {
    assert_compiles_data(
        r#"
INCLUDE LUA {
    function round(num, numDecimalPlaces)
        local mult = 10^(numDecimalPlaces or 0)
        return math.floor(num * mult + 0.5) / mult
    end
}

TABLE cholo {
    id INT,
    t TEXT,
    f FLOAT,
    computed_int INT GENERATED AS { id + 2 },
    computed_float FLOAT GENERATED AS { round(computed_int * 2.4, 1) },
    computed_text TEXT GENERATED AS { "hello " .. computed_float },
    computed_float_of_int FLOAT GENERATED AS { computed_int * 3 },
    computed_text_of_text TEXT GENERATED AS { t .. "!" },
    computed_float_of_float FLOAT GENERATED AS { f * 2.0 },

    CHECK { computed_int > 0 }
    CHECK { computed_float > 0 }
}

DATA cholo {
    1, a, 1.1;
    2, b, 2.2;
}
        "#,
        json!({
            "cholo": [
                {
                    "id": 1.0, "t": "a", "f": 1.1,
                    "computed_int": 3.0, "computed_float": 7.2,
                    "computed_text": "hello 7.2",
                    "computed_float_of_int": 9.0,
                    "computed_text_of_text": "a!",
                    "computed_float_of_float": 2.2,
                },
                {
                    "id": 2.0, "t": "b", "f": 2.2,
                    "computed_int": 4.0, "computed_float": 9.6,
                    "computed_text": "hello 9.6",
                    "computed_float_of_int": 12.0,
                    "computed_text_of_text": "b!",
                    "computed_float_of_float": 4.4,
                },
            ]
        }),
    );
}

#[test]
fn test_lua_computed_column_wrong_type_text() {
    assert_test_validaton_exception(
        DatabaseValidationError::LuaColumnGenerationExpressionComputeTypeMismatch {
            table_name: "cholo".to_string(),
            column_name: "computed_text".to_string(),
            input_row_fields: vec!["id".to_string()],
            input_row_values: vec!["1".to_string()],
            expression: " id + 1 ".to_string(),
            computed_value: "2".to_string(),
            error: "Computed column expects lua expression to evaluate to type string, got integer"
                .to_string(),
        },
        r#"
TABLE cholo {
    id INT,
    computed_text TEXT GENERATED AS { id + 1 },
}

DATA cholo {
    1;
    2;
}
        "#,
    );
}

#[test]
fn test_lua_computed_column_wrong_type_integer() {
    assert_test_validaton_exception(
        DatabaseValidationError::LuaColumnGenerationExpressionComputeTypeMismatch {
            table_name: "cholo".to_string(),
            column_name: "computed_int".to_string(),
            input_row_fields: vec!["id".to_string()],
            input_row_values: vec!["1".to_string()],
            expression: " \"hello \" .. id ".to_string(),
            computed_value: "hello 1".to_string(),
            error: "Computed column expects lua expression to evaluate to type integer, got string"
                .to_string(),
        },
        r#"
TABLE cholo {
    id INT,
    computed_int INT GENERATED AS { "hello " .. id },
}

DATA cholo {
    1;
    2;
}
        "#,
    );
}

#[test]
fn test_lua_computed_column_wrong_type_float_from_int() {
    assert_test_validaton_exception(
        DatabaseValidationError::LuaColumnGenerationExpressionComputeTypeMismatch {
            table_name: "cholo".to_string(),
            column_name: "computed_int".to_string(),
            input_row_fields: vec!["id".to_string()],
            input_row_values: vec!["1".to_string()],
            expression: " id * 1.5 ".to_string(),
            error: "Computed column expects lua expression to evaluate to type integer, got number"
                .to_string(),
            computed_value: "1.5".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
    computed_int INT GENERATED AS { id * 1.5 },
}

DATA cholo {
    1;
    2;
}
        "#,
    );
}

#[test]
fn test_lua_computed_column_wrong_type_float() {
    assert_test_validaton_exception(
        DatabaseValidationError::LuaColumnGenerationExpressionComputeTypeMismatch {
            table_name: "cholo".to_string(),
            column_name: "computed_text".to_string(),
            input_row_fields: vec!["id".to_string()],
            input_row_values: vec!["1".to_string()],
            expression: " nil ".to_string(),
            computed_value: "nil".to_string(),
            error: "Computed column expects lua expression to evaluate to type number, got nil"
                .to_string(),
        },
        r#"
TABLE cholo {
    id INT,
    computed_text FLOAT GENERATED AS { nil },
}

DATA cholo {
    1;
    2;
}
        "#,
    );
}

#[test]
fn test_lua_computed_column_wrong_type_nil() {
    assert_test_validaton_exception(
        DatabaseValidationError::LuaColumnGenerationExpressionComputeTypeMismatch {
            table_name: "cholo".to_string(),
            column_name: "computed_text".to_string(),
            input_row_fields: vec!["id".to_string()],
            input_row_values: vec!["1".to_string()],
            expression: " nil ".to_string(),
            computed_value: "nil".to_string(),
            error: "Computed column expects lua expression to evaluate to type string, got nil"
                .to_string(),
        },
        r#"
TABLE cholo {
    id INT,
    computed_text TEXT GENERATED AS { nil },
}

DATA cholo {
    1;
    2;
}
        "#,
    );
}

#[test]
fn test_lua_computed_column_wrong_expression() {
    let e = assert_test_validaton_exception_return_error(
        r#"
TABLE cholo {
    id INT,
    computed_text TEXT GENERATED AS { not a valid lua },
}

DATA cholo {
    1;
    2;
}
        "#,
    );

    if let DatabaseValidationError::LuaColumnGenerationExpressionLoadError {
        table_name,
        column_name,
        expression,
        error,
    } = e
    {
        assert_eq!(table_name, "cholo");
        assert_eq!(column_name, "computed_text");
        assert_eq!(expression, " not a valid lua ");
        assert!(error.contains("syntax error"));
    } else {
        panic!()
    }
}

#[test]
fn test_lua_computed_column_wrong_expression_order() {
    let e = assert_test_validaton_exception_return_error(
        r#"
TABLE cholo {
    id INT,
    computed_text_2 TEXT GENERATED AS { computed_text .. "!" },
    computed_text TEXT GENERATED AS {
        "hello" .. id
    },
}

DATA cholo {
    1;
    2;
}
        "#,
    );

    if let DatabaseValidationError::LuaColumnGenerationExpressionComputeError {
        table_name,
        column_name,
        input_row_fields,
        input_row_values,
        expression,
        error,
    } = e
    {
        assert_eq!(table_name, "cholo");
        assert_eq!(column_name, "computed_text_2");
        assert_eq!(input_row_fields, vec!["id".to_string()]);
        assert_eq!(input_row_values, vec!["1".to_string()]);
        assert_eq!(expression, " computed_text .. \"!\" ");
        assert!(error.contains("attempt to concatenate global 'computed_text' (a nil value)"));
    } else {
        panic!()
    }
}

#[test]
fn test_lua_use_default_value_for_computed_column() {
    assert_compiles_data(
        r#"
TABLE cholo {
    name TEXT,
    id INT DEFAULT 2,
    id2 INT GENERATED AS { id + 1 },
}

DATA cholo {
    henlo;
}
        "#,
        json!({
            "cholo": [
                {
                    "name": "henlo",
                    "id": 2.0,
                    "id2": 3.0,
                },
            ]
        }),
    );
}

#[test]
fn test_lua_use_default_value_for_computed_column_struct() {
    assert_compiles_data(
        r#"
TABLE cholo {
    name TEXT,
    id INT DEFAULT 2,
    id2 INT GENERATED AS { id + 1 },
}

DATA STRUCT cholo {
    name: henlo,
}
        "#,
        json!({
            "cholo": [
                {
                    "name": "henlo",
                    "id": 2.0,
                    "id2": 3.0,
                },
            ]
        }),
    );
}

#[test]
fn test_explicit_specification_of_computer_column_error() {
    assert_test_validaton_exception(
        DatabaseValidationError::ComputerColumnCannotBeExplicitlySpecified {
            table_name: "cholo".to_string(),
            column_name: "id2".to_string(),
            compute_expression: " 7 ".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
    id2 INT GENERATED AS { 7 },
}

DATA cholo(id, id2) {
    1, 2;
}
        "#,
    );
}

#[test]
fn test_explicit_specification_of_computer_column_error_struct() {
    assert_test_validaton_exception(
        DatabaseValidationError::ComputerColumnCannotBeExplicitlySpecified {
            table_name: "cholo".to_string(),
            column_name: "id2".to_string(),
            compute_expression: " 7 ".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
    id2 INT GENERATED AS { 7 },
}

DATA STRUCT cholo {
    id: 1, id2: 2
}
        "#,
    );
}

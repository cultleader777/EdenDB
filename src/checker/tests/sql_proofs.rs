#[cfg(test)]
use crate::checker::errors::DatabaseValidationError;
#[cfg(test)]
use super::common::assert_test_validaton_exception;
#[cfg(test)]

#[test]
fn test_sql_proof_table_not_found() {
    assert_test_validaton_exception(
        DatabaseValidationError::SqlProofTableNotFound {
            table_name: "moo".to_string(),
            comment: "table not found".to_string(),
            proof_expression: "\n    SELECT 1\n".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
}

PROOF "table not found" NONE EXIST OF moo {
    SELECT 1
}
        "#,
    );
}

#[test]
fn test_sql_proof_invalid_syntax() {
    assert_test_validaton_exception(
        DatabaseValidationError::SqlProofQueryPlanningError {
            table_name: "cholo".to_string(),
            proof_expression: " invalid sql syntax ".to_string(),
            error: "near \"invalid\": syntax error in  invalid sql syntax  at offset 1".to_string(),
            comment: "invalid syntax".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
}

PROOF "invalid syntax"
NONE EXIST OF cholo { invalid sql syntax }
        "#,
    );
}

#[test]
fn test_sql_proof_invalid_column_count() {
    assert_test_validaton_exception(
        DatabaseValidationError::SqlProofQueryErrorSingleRowIdColumnExpected {
            table_name: "cholo".to_string(),
            proof_expression: " SELECT 1, 2 ".to_string(),
            error: "Required output column count is 1, got 2".to_string(),
            comment: "too many columns".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
}

PROOF "too many columns" NONE EXIST OF cholo { SELECT 1, 2 }
        "#,
    );
}

#[test]
fn test_sql_proof_invalid_column_name() {
    assert_test_validaton_exception(
        DatabaseValidationError::SqlProofQueryErrorSingleRowIdColumnExpected {
            table_name: "cholo".to_string(),
            proof_expression: " SELECT id FROM cholo ".to_string(),
            error: "Required output column name must be rowid, got id".to_string(),
            comment: "invalid column name".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
}

PROOF "invalid column name" NONE EXIST OF cholo { SELECT id FROM cholo }
        "#,
    );
}

#[test]
fn test_sql_proof_invalid_column_source_by_table() {
    assert_test_validaton_exception(
        DatabaseValidationError::SqlProofQueryColumnOriginMismatchesExpected {
            proof_expression: " SELECT rowid FROM bolo ".to_string(),
            error: "Actual column origin table name or origin mistmaches expectations".to_string(),
            expected_column_origin_table: "cholo".to_string(),
            expected_column_origin_name: "rowid".to_string(),
            actual_column_origin_table: "bolo".to_string(),
            actual_column_origin_name: "rowid".to_string(),
            comment: "invalid source table".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
}

TABLE bolo {
    id INT,
}

PROOF "invalid source table" NONE EXIST OF cholo { SELECT rowid FROM bolo }
        "#,
    );
}

#[test]
fn test_sql_proof_invalid_column_source_by_expression() {
    assert_test_validaton_exception(
        DatabaseValidationError::SqlProofQueryColumnOriginMismatchesExpected {
            proof_expression: " SELECT 1 AS rowid ".to_string(),
            error: "Actual column origin table name or origin mistmaches expectations".to_string(),
            expected_column_origin_table: "cholo".to_string(),
            expected_column_origin_name: "rowid".to_string(),
            actual_column_origin_table: "NULL".to_string(),
            actual_column_origin_name: "NULL".to_string(),
            comment: "good column name but source bad".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
}

PROOF "good column name but source bad"
NONE EXIST OF cholo { SELECT 1 AS rowid }
        "#,
    );
}

#[test]
fn test_sql_proof_read_only_runtime_error_binding_values_forbidden() {
    assert_test_validaton_exception(
        DatabaseValidationError::SqlProofQueryError {
            error: "Wrong number of parameters passed to query. Got 0, needed 1".to_string(),
            table_name: "cholo".to_string(),
            proof_expression: " SELECT rowid FROM cholo WHERE id = ? ".to_string(),
            comment: "bound values fail".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
}

PROOF "bound values fail"
NONE EXIST OF cholo { SELECT rowid FROM cholo WHERE id = ? }
        "#,
    );
}

#[test]
fn test_sql_proof_read_only_runtime_error() {
    assert_test_validaton_exception(
        DatabaseValidationError::SqlProofQueryError {
            error: "attempt to write a readonly database".to_string(),
            table_name: "cholo".to_string(),
            proof_expression: " INSERT INTO cholo VALUES(2) RETURNING rowid ".to_string(),
            comment: "readonly database".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
}

PROOF "readonly database"
NONE EXIST OF cholo { INSERT INTO cholo VALUES(2) RETURNING rowid }
        "#,
    );
}

#[test]
fn test_sql_proof_offenders_found() {
    assert_test_validaton_exception(
        DatabaseValidationError::SqlProofOffendersFound {
            table_name: "cholo".to_string(),
            proof_expression: " SELECT rowid FROM cholo WHERE id > 1 ".to_string(),
            comment: "no id is more than 1".to_string(),
            offending_columns: vec![
"{
  \"id\": 2.0
}".to_string(),
"{
  \"id\": 3.0
}".to_string(),
            ],
        },
        r#"
TABLE cholo {
    id INT,
}

DATA cholo {
    1;
    2;
    3;
}

PROOF "no id is more than 1" NONE EXIST OF cholo { SELECT rowid FROM cholo WHERE id > 1 }
        "#,
    );
}


#[test]
fn test_sql_proof_boolean_offenders_found() {
    // we convert boolean to 1 or 0 integers in sqlite
    assert_test_validaton_exception(
        DatabaseValidationError::SqlProofOffendersFound {
            table_name: "cholo".to_string(),
            proof_expression: " SELECT rowid FROM cholo WHERE offends ".to_string(),
            comment: "offends column".to_string(),
            offending_columns: vec![
"{
  \"id\": 2.0,
  \"offends\": true
}".to_string(),
            ],
        },
        r#"
TABLE cholo {
    id INT,
    offends BOOL GENERATED AS { id % 2 == 0 },
}

DATA cholo {
    1;
    2;
    3;
}

PROOF "offends column" NONE EXIST OF cholo { SELECT rowid FROM cholo WHERE offends }
        "#,
    );
}
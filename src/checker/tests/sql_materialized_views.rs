#[cfg(test)]
use serde_json::json;
#[cfg(test)]
use crate::checker::errors::DatabaseValidationError;
#[cfg(test)]
use crate::checker::types::DBType;
#[cfg(test)]
use super::common::assert_compiles_data;
#[cfg(test)]
use super::common::assert_test_validaton_exception;

#[test]
fn test_mat_views_no_default_expr() {
    assert_test_validaton_exception(
        DatabaseValidationError::MaterializedViewsCannotHaveDefaultColumnExpression {
            table_name: "cholo".to_string(),
            column_name: "id".to_string(),
        },
        r#"
MATERIALIZED VIEW cholo {
    id INT DEFAULT 1,
} AS {
    SELECT 1
}
        "#,
    );
}

#[test]
fn test_mat_views_no_computed_expr() {
    assert_test_validaton_exception(
        DatabaseValidationError::MaterializedViewsCannotHaveComputedColumnExpression {
            table_name: "cholo".to_string(),
            column_name: "id".to_string(),
        },
        r#"
MATERIALIZED VIEW cholo {
    id INT GENERATED AS { 1 },
} AS {
    SELECT 1
}
        "#,
    );
}

#[test]
fn test_mat_views_no_explicit_data() {
    assert_test_validaton_exception(
        DatabaseValidationError::DataInsertionsToMaterializedViewsNotAllowed {
            table_name: "cholo".to_string(),
        },
        r#"
MATERIALIZED VIEW cholo {
    id INT,
} AS {
    SELECT 1
}

DATA cholo {
    1
}
        "#,
    );
}

#[test]
fn test_mat_views_invalid_syntax() {
    assert_test_validaton_exception(
        DatabaseValidationError::SqlMatViewStatementPrepareException {
            table_name: "cholo".to_string(),
            sql_expression: "\n    invalid sql syntax\n".to_string(),
            error: "near \"invalid\": syntax error in \n    invalid sql syntax\n at offset 5".to_string(),
        },
        r#"
MATERIALIZED VIEW cholo {
    id INT,
} AS {
    invalid sql syntax
}
        "#,
    );
}

#[test]
fn test_mat_views_invalid_column_count() {
    assert_test_validaton_exception(
        DatabaseValidationError::SqlMatViewWrongColumnCount {
            table_name: "cholo".to_string(),
            sql_expression: "\n    SELECT 1\n".to_string(),
            expected_columns: 2,
            actual_columns: 1,
        },
        r#"
MATERIALIZED VIEW cholo {
    id INT,
    id2 INT,
} AS {
    SELECT 1
}
        "#,
    );
}

#[test]
fn test_mat_views_no_query_bindings() {
    assert_test_validaton_exception(
        DatabaseValidationError::SqlMatViewStatementInitException {
            table_name: "cholo".to_string(),
            sql_expression: "\n    SELECT ?\n".to_string(),
            error: "Wrong number of parameters passed to query. Got 0, needed 1".to_string(),
        },
        r#"
MATERIALIZED VIEW cholo {
    id INT,
} AS {
    SELECT ?
}
        "#,
    );
}

#[test]
fn test_mat_views_try_mutation() {
    assert_test_validaton_exception(
        DatabaseValidationError::SqlMatViewStatementQueryException {
            table_name: "cholo".to_string(),
            sql_expression: "\n    INSERT INTO cholo VALUES(1) RETURNING id\n".to_string(),
            error: "attempt to write a readonly database".to_string(),
        },
        r#"
MATERIALIZED VIEW cholo {
    id INT,
} AS {
    INSERT INTO cholo VALUES(1) RETURNING id
}
        "#,
    );
}

#[test]
fn test_mat_views_prevent_nulls() {
    assert_test_validaton_exception(
        DatabaseValidationError::SqlMatViewNullReturnsUnsupported {
            table_name: "cholo".to_string(),
            sql_expression: "\n    SELECT null\n".to_string(),
            column_name: "id".to_string(),
            return_row_index: 1,
        },
        r#"
MATERIALIZED VIEW cholo {
    id INT,
} AS {
    SELECT null
}
        "#,
    );
}

#[test]
fn test_mat_views_wrong_type_text_to_int() {
    assert_test_validaton_exception(
        DatabaseValidationError::SqlMatViewWrongColumnTypeReturned {
            table_name: "cholo".to_string(),
            sql_expression: "\n    SELECT 'hello'\n".to_string(),
            column_name: "id".to_string(),
            return_row_index: 1,
            actual_column_type: "TEXT".to_string(),
            expected_column_type: DBType::DBInt,
        },
        r#"
MATERIALIZED VIEW cholo {
    id INT,
} AS {
    SELECT 'hello'
}
        "#,
    );
}

#[test]
fn test_mat_views_coerce_types_ok() {
    assert_compiles_data(
        r#"
MATERIALIZED VIEW cholo {
    f1 FLOAT,
    f2 BOOL,
    f3 TEXT,
    f4 TEXT,
} AS {
    SELECT 7, false, true, 7.7
}
        "#,
        json!({
            "cholo": [
                {"f1": 7.0, "f2": false, "f3": "1", "f4": "7.7"}
            ]
        })
    );
}

#[test]
fn test_mat_views_success() {
    assert_compiles_data(
        r#"
TABLE source {
    i INT,
    b BOOL,
    f FLOAT,
    t TEXT,
}

DATA source {
    1, true, 1.23, sup;
    2, false, 2.23, milky;
    3, true, 3.23, boi;
}

MATERIALIZED VIEW cholo {
    other_i INT,
    other_b BOOL,
    other_f FLOAT,
    other_t TEXT,
} AS {
    SELECT * FROM source
}
        "#,
        json!({
            "source": [
                {"i": 1.0, "b": true, "f": 1.23, "t": "sup"},
                {"i": 2.0, "b": false, "f": 2.23, "t": "milky"},
                {"i": 3.0, "b": true, "f": 3.23, "t": "boi"},
            ],
            "cholo": [
                {"other_i": 1.0, "other_b": true, "other_f": 1.23, "other_t": "sup"},
                {"other_i": 2.0, "other_b": false, "other_f": 2.23, "other_t": "milky"},
                {"other_i": 3.0, "other_b": true, "other_f": 3.23, "other_t": "boi"},
            ],
        })
    );
}

#[test]
fn test_mat_views_dependencies() {
    assert_compiles_data(
        r#"
TABLE source {
    i INT,
    b BOOL,
    f FLOAT,
    t TEXT,
}

DATA source {
    1, true, 1.23, sup;
    2, false, 2.23, milky;
    3, true, 3.23, boi;
}

MATERIALIZED VIEW cholo {
    other_i INT,
    other_b BOOL,
    other_f FLOAT,
    other_t TEXT,
} AS {
    SELECT * FROM source
}

MATERIALIZED VIEW bolo {
    other_i INT,
    other_b BOOL,
    other_f FLOAT,
    other_t TEXT,
} AS {
    SELECT * FROM cholo
}
        "#,
        json!({
            "source": [
                {"i": 1.0, "b": true, "f": 1.23, "t": "sup"},
                {"i": 2.0, "b": false, "f": 2.23, "t": "milky"},
                {"i": 3.0, "b": true, "f": 3.23, "t": "boi"},
            ],
            "cholo": [
                {"other_i": 1.0, "other_b": true, "other_f": 1.23, "other_t": "sup"},
                {"other_i": 2.0, "other_b": false, "other_f": 2.23, "other_t": "milky"},
                {"other_i": 3.0, "other_b": true, "other_f": 3.23, "other_t": "boi"},
            ],
            "bolo": [
                {"other_i": 1.0, "other_b": true, "other_f": 1.23, "other_t": "sup"},
                {"other_i": 2.0, "other_b": false, "other_f": 2.23, "other_t": "milky"},
                {"other_i": 3.0, "other_b": true, "other_f": 3.23, "other_t": "boi"},
            ],
        })
    );
}

#[test]
fn test_mat_views_primary_keys() {
    assert_compiles_data(
        r#"
TABLE ref_table {
    pkey INT PRIMARY KEY,
}

DATA ref_table {
    10;
    11;
}

TABLE source {
    a INT,
    b INT,
}

DATA source {
    1, 10;
    2, 11;
    3, 10;
}

MATERIALIZED VIEW cholo {
    pkey INT PRIMARY KEY,
    ref_key REF ref_table,
} AS {
    SELECT * FROM source
}
        "#,
        json!({
            "source": [
                {"a": 1.0, "b": 10.0},
                {"a": 2.0, "b": 11.0},
                {"a": 3.0, "b": 10.0},
            ],
            "cholo": [
                {"pkey": 1.0, "ref_key": 10.0},
                {"pkey": 2.0, "ref_key": 11.0},
                {"pkey": 3.0, "ref_key": 10.0},
            ],
            "ref_table": [
                {"pkey": 10.0},
                {"pkey": 11.0},
            ],
        })
    );
}

#[test]
fn test_mat_views_foreign_keys_fail() {
    assert_test_validaton_exception(
        DatabaseValidationError::NonExistingForeignKey {
            table_with_foreign_key: "cholo".to_string(),
            foreign_key_column: "ref_key".to_string(),
            referred_table: "ref_table".to_string(),
            referred_table_column: "pkey".to_string(),
            key_value: "12".to_string(),
        },
        r#"
TABLE ref_table {
    pkey INT PRIMARY KEY,
}

DATA ref_table {
    10;
    11;
}

TABLE source {
    a INT,
    b INT,
}

DATA source {
    1, 10;
    2, 11;
    3, 12;
}

MATERIALIZED VIEW cholo {
    pkey INT PRIMARY KEY,
    ref_key REF ref_table,
} AS {
    SELECT * FROM source
}
        "#,
    );
}

#[test]
fn test_mat_views_dupe_pkeys_fail() {
    assert_test_validaton_exception(
        DatabaseValidationError::DuplicatePrimaryKey {
            table_name: "cholo".to_string(),
            value: "2".to_string(),
        },
        r#"
TABLE source {
    a INT,
}

DATA source {
    1;
    2;
    2;
}

MATERIALIZED VIEW cholo {
    pkey INT PRIMARY KEY,
} AS {
    SELECT * FROM source
}
        "#,
    );
}

#[test]
fn test_mat_views_uniq_constraints_fail() {
    assert_test_validaton_exception(
        DatabaseValidationError::UniqConstraintViolated {
            table_name: "cholo".to_string(),
            tuple_definition: "(akey, bkey)".to_string(),
            tuple_value: "(1, 2)".to_string(),
        },
        r#"
TABLE source {
    a INT,
    b INT,
}

DATA source {
    1, 1;
    1, 2;
    2, 1;
    1, 2;
}

MATERIALIZED VIEW cholo {
    akey INT,
    bkey INT,
    UNIQUE(akey, bkey)
} AS {
    SELECT * FROM source
}
        "#,
    );
}

#[test]
fn test_mat_views_with_primary_key() {
    assert_compiles_data(
        r#"
TABLE source {
    i INT,
}

DATA source {
    1;
    2;
    3;
}

MATERIALIZED VIEW cholo {
    other_i INT PRIMARY KEY,
} AS {
    SELECT * FROM source
}
        "#,
        json!({
            "source": [
                {"i": 1.0},
                {"i": 2.0},
                {"i": 3.0},
            ],
            "cholo": [
                {"other_i": 1.0},
                {"other_i": 2.0},
                {"other_i": 3.0},
            ],
        })
    );
}

#[test]
fn test_mat_views_with_child_primary_key() {
    assert_compiles_data(
        r#"
TABLE source {
    i INT PRIMARY KEY,
}

DATA source {
    1;
    2;
    3;
}

MATERIALIZED VIEW cholo {
    child_key INT PRIMARY KEY CHILD OF source,
} AS {
    SELECT i, 7 FROM source
}
        "#,
        json!({
            "source": [
                {"i": 1.0},
                {"i": 2.0},
                {"i": 3.0},
            ],
            "cholo": [
                {"i": 1.0, "child_key": 7.0},
                {"i": 2.0, "child_key": 7.0},
                {"i": 3.0, "child_key": 7.0},
            ],
        })
    );
}

#[test]
fn test_mat_views_with_foreign_key() {
    assert_compiles_data(
        r#"
TABLE source {
    i INT PRIMARY KEY,
}

DATA source {
    1;
    2;
    3;
}

MATERIALIZED VIEW cholo {
    foreign_key REF source,
} AS {
    SELECT i FROM source
}
        "#,
        json!({
            "source": [
                {"i": 1.0},
                {"i": 2.0},
                {"i": 3.0},
            ],
            "cholo": [
                {"foreign_key": 1.0},
                {"foreign_key": 2.0},
                {"foreign_key": 3.0},
            ],
        })
    );
}

#[test]
fn test_mat_views_ref_foreign_child() {
    assert_compiles_data(
        r#"
TABLE parent {
    pk INT PRIMARY KEY,
}

TABLE child {
    ck INT PRIMARY KEY CHILD OF parent,
}

TABLE source {
    i TEXT,
}

DATA parent {
    1 WITH child {
        10
    };
    2 WITH child {
        20
    };
}

DATA source {
    1->10;
    2->20;
}

MATERIALIZED VIEW cholo {
    foreign_key REF FOREIGN CHILD child,
} AS {
    SELECT i FROM source
}
        "#,
        json!({
            "parent": [
                {"pk": 1.0},
                {"pk": 2.0},
            ],
            "child": [
                {"pk": 1.0, "ck": 10.0},
                {"pk": 2.0, "ck": 20.0},
            ],
            "source": [
                {"i": "1->10"},
                {"i": "2->20"},
            ],
            "cholo": [
                {"foreign_key": "1->10"},
                {"foreign_key": "2->20"},
            ],
        })
    );
}
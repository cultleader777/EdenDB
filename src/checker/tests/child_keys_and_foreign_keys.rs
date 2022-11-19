#[cfg(test)]
use serde_json::json;
#[cfg(test)]
use super::common::assert_compiles_data;
#[cfg(test)]
use crate::checker::errors::DatabaseValidationError;
#[cfg(test)]
use super::common::assert_test_validaton_exception;

#[test]
fn test_child_key_can_be_foreign_key() {
    assert_compiles_data(
        r#"
TABLE pkey_table {
    some_key TEXT PRIMARY KEY,
}

TABLE other_table {
    other_key REF pkey_table PRIMARY KEY,
}

DATA pkey_table {
    point_to
}

DATA other_table {
    point_to
}

        "#,
        json!({
            "pkey_table":[
                {"some_key":"point_to"},
            ],
            "other_table":[
                {"other_key":"point_to"},
            ],
        })
    );
}

#[test]
fn test_child_key_can_be_foreign_key_not_found_key() {
    assert_test_validaton_exception(
        DatabaseValidationError::NonExistingForeignKey {
            table_with_foreign_key: "other_table".to_string(),
            foreign_key_column: "other_key".to_string(),
            referred_table: "pkey_table".to_string(),
            referred_table_column: "some_key".to_string(),
            key_value: "point_toz".to_string(),
        },
        r#"
TABLE pkey_table {
    some_key TEXT PRIMARY KEY,
}

TABLE other_table {
    other_key REF pkey_table PRIMARY KEY,
}

DATA pkey_table {
    point_to
}

DATA other_table {
    point_toz
}

        "#,
    );
}

#[test]
fn test_child_inner_foreign_and_primary_key_all_good() {
    assert_compiles_data(
        r#"
TABLE existant_parent {
    some_key TEXT PRIMARY KEY,
}

TABLE existant_child {
    some_child_key TEXT PRIMARY KEY CHILD OF existant_parent,
}

TABLE existant_child_2 {
    some_child_key_2 TEXT PRIMARY KEY CHILD OF existant_child,
}

TABLE good_ref {
    ref_key REF FOREIGN CHILD existant_child_2 PRIMARY KEY CHILD OF existant_parent,
}

DATA existant_parent {
    outer_val WITH existant_child {
        inner_val WITH existant_child_2 {
            more_inner_val
        }
    } WITH good_ref {
        inner_val->more_inner_val
    }
}
        "#,
        json!({
            "existant_parent":[
                {"some_key":"outer_val"},
            ],
            "existant_child":[
                {"some_key":"outer_val", "some_child_key": "inner_val"},
            ],
            "existant_child_2":[
                {"some_key":"outer_val", "some_child_key": "inner_val", "some_child_key_2": "more_inner_val"},
            ],
            "good_ref":[
                {"some_key":"outer_val", "ref_key": "inner_val->more_inner_val"},
            ],
        })
    );
}
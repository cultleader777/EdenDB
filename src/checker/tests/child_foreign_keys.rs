#[cfg(test)]
use serde_json::json;
#[cfg(test)]
use super::common::assert_compiles_data;
#[cfg(test)]
use crate::checker::errors::DatabaseValidationError;
#[cfg(test)]
use super::common::assert_test_validaton_exception;

#[test]
fn test_child_foreign_key_refers_to_non_existing_table() {
    assert_test_validaton_exception(
        DatabaseValidationError::ForeignKeyTableDoesntExist {
            referrer_table: "good_ref".to_string(),
            referrer_column: "ref_port_no".to_string(),
            referred_table: "non_existant".to_string(),
        },
        r#"
TABLE good_ref {
    ref_port_no REF CHILD non_existant,
}
        "#
    );
}

#[test]
fn test_child_foreign_key_refers_to_non_table_who_is_not_child() {
    assert_test_validaton_exception(
        DatabaseValidationError::ForeignChildKeyTableDoesntHaveParentTable {
            referrer_table: "good_ref".to_string(),
            referrer_column: "ref_key".to_string(),
            referred_table: "existant".to_string(),
        },
        r#"
TABLE existant {
    some_key INT PRIMARY KEY,
}

TABLE good_ref {
    ref_key REF CHILD existant,
}
        "#
    );
}


#[test]
fn test_child_foreign_key_refers_equal_ancestry_child() {
    assert_test_validaton_exception(
        DatabaseValidationError::ForeignChildKeyTableIsHigherOrEqualInAncestryThanTheReferrer {
            referrer_table: "bad_ref".to_string(),
            referrer_column: "ref_key".to_string(),
            referred_table: "existant_child".to_string(),
        },
        r#"
TABLE existant_parent {
    some_key INT PRIMARY KEY,
}

TABLE existant_child {
    some_child_key INT PRIMARY KEY CHILD OF existant_parent,
}

TABLE bad_ref {
    inner_key INT PRIMARY KEY CHILD OF existant_parent,
    ref_key REF CHILD existant_child,
}
        "#
    );
}

#[test]
fn test_child_foreign_key_refers_higher_ancestry_child() {
    assert_test_validaton_exception(
        DatabaseValidationError::ForeignChildKeyTableIsHigherOrEqualInAncestryThanTheReferrer {
            referrer_table: "bad_ref".to_string(),
            referrer_column: "ref_key".to_string(),
            referred_table: "existant_child".to_string(),
        },
        r#"
TABLE existant_parent {
    some_key INT PRIMARY KEY,
}

TABLE existant_child {
    some_child_key INT PRIMARY KEY CHILD OF existant_parent,
}

TABLE child_of_parent {
    some_child_key_2 INT PRIMARY KEY CHILD OF existant_parent,
}

TABLE bad_ref {
    inner_key INT PRIMARY KEY CHILD OF child_of_parent,
    ref_key REF CHILD existant_child,
}
        "#
    );
}

#[test]
fn test_child_foreign_key_parent_has_negative_key_segment() {
    assert_test_validaton_exception(
        DatabaseValidationError::ForeignChildKeyTableIntegerKeyMustBeNonNegative {
            offending_column: "some_key".to_string(),
            offending_value: -123,
            referred_table: "existant_parent".to_string(),
        },
        r#"
TABLE existant_parent {
    some_key INT PRIMARY KEY,
}

TABLE existant_child {
    some_child_key INT PRIMARY KEY CHILD OF existant_parent,
}

TABLE good_ref {
    ref_key REF CHILD existant_child,
}

DATA existant_parent {
    -123
}
        "#
    );
}

#[test]
fn test_child_foreign_key_child_has_negative_key_segment() {
    assert_test_validaton_exception(
        DatabaseValidationError::ForeignChildKeyTableIntegerKeyMustBeNonNegative {
            offending_column: "some_child_key".to_string(),
            offending_value: -7,
            referred_table: "existant_child".to_string(),
        },
        r#"
TABLE existant_parent {
    some_key INT PRIMARY KEY,
}

TABLE existant_child {
    some_child_key INT PRIMARY KEY CHILD OF existant_parent,
}

TABLE good_ref {
    ref_key REF CHILD existant_child,
}

DATA existant_parent {
    123 WITH existant_child {
        -7
    }
}
        "#
    );
}

#[test]
fn test_child_foreign_key_parent_has_non_snake_case_segment() {
    assert_test_validaton_exception(
        DatabaseValidationError::ForeignChildKeyTableStringMustBeSnakeCase {
            offending_column: "some_key".to_string(),
            offending_value: "NonSnakeCase".to_string(),
            referred_table: "existant_parent".to_string(),
        },
        r#"
TABLE existant_parent {
    some_key TEXT PRIMARY KEY,
}

TABLE existant_child {
    some_child_key TEXT PRIMARY KEY CHILD OF existant_parent,
}

TABLE good_ref {
    ref_key REF CHILD existant_child,
}

DATA existant_parent {
    NonSnakeCase
}
        "#
    );
}

#[test]
fn test_child_foreign_key_child_has_non_snake_case_segment() {
    assert_test_validaton_exception(
        DatabaseValidationError::ForeignChildKeyTableStringMustBeSnakeCase {
            offending_column: "some_child_key".to_string(),
            offending_value: "NonSnakeCase".to_string(),
            referred_table: "existant_child".to_string(),
        },
        r#"
TABLE existant_parent {
    some_key TEXT PRIMARY KEY,
}

TABLE existant_child {
    some_child_key TEXT PRIMARY KEY CHILD OF existant_parent,
}

TABLE good_ref {
    ref_key REF CHILD existant_child,
}

DATA existant_parent {
    snake_case WITH existant_child {
        NonSnakeCase
    }
}
        "#
    );
}

#[test]
fn test_child_foreign_key_wrong_segment_count() {
    assert_test_validaton_exception(
        DatabaseValidationError::ForeignChildKeyReferrerHasIncorrectSegmentsInCompositeKey {
            referee_table: "existant_child".to_string(),
            referrer_table: "good_ref".to_string(),
            referrer_column: "ref_key".to_string(),
            offending_value: "single_segment".to_string(),
            expected_segments: 2,
            actual_segments: 1,
        },
        r#"
TABLE existant_parent {
    some_key TEXT PRIMARY KEY,
}

TABLE existant_child {
    some_child_key TEXT PRIMARY KEY CHILD OF existant_parent,
}

TABLE good_ref {
    ref_key REF CHILD existant_child,
}

DATA existant_parent {
    outer_val WITH existant_child {
        inner_val
    }
}

DATA good_ref {
    single_segment
}
        "#
    );
}

#[test]
fn test_child_foreign_key_whitespace_in_segments() {
    assert_test_validaton_exception(
        DatabaseValidationError::ForeignChildKeyReferrerCannotHaveWhitespaceInSegments {
            referee_table: "existant_child".to_string(),
            referrer_table: "good_ref".to_string(),
            referrer_column: "ref_key".to_string(),
            offending_value: "outer_val -> inner_val".to_string(),
        },
        r#"
TABLE existant_parent {
    some_key TEXT PRIMARY KEY,
}

TABLE existant_child {
    some_child_key TEXT PRIMARY KEY CHILD OF existant_parent,
}

TABLE good_ref {
    ref_key REF CHILD existant_child,
}

DATA existant_parent {
    outer_val WITH existant_child {
        inner_val
    }
}

DATA good_ref {
    "outer_val -> inner_val"
}
        "#
    );
}

#[test]
fn test_child_foreign_key_referring_to_non_existing_element() {
    assert_test_validaton_exception(
        DatabaseValidationError::NonExistingForeignKeyToChildTable {
            table_parent_keys: vec![],
            table_parent_tables: vec![],
            table_parent_columns: vec![],
            table_with_foreign_key: "good_ref".to_string(),
            foreign_key_column: "ref_key".to_string(),
            referred_table: "existant_child".to_string(),
            referred_table_column: "some_key->some_child_key".to_string(),
            key_value: "no_such->inner_val".to_string(),
        },
        r#"
TABLE existant_parent {
    some_key TEXT PRIMARY KEY,
}

TABLE existant_child {
    some_child_key TEXT PRIMARY KEY CHILD OF existant_parent,
}

TABLE good_ref {
    ref_key REF CHILD existant_child,
}

DATA existant_parent {
    outer_val WITH existant_child {
        inner_val
    }
}

DATA good_ref {
    "no_such->inner_val"
}
        "#
    );
}

#[test]
fn test_child_inner_foreign_key_referring_to_non_existing_element() {
    assert_test_validaton_exception(
        DatabaseValidationError::NonExistingForeignKeyToChildTable {
            table_parent_keys: vec!["outer_val".to_string()],
            table_parent_tables: vec!["existant_parent".to_string()],
            table_parent_columns: vec!["some_key".to_string()],
            table_with_foreign_key: "good_ref".to_string(),
            foreign_key_column: "ref_key".to_string(),
            referred_table: "existant_child_2".to_string(),
            referred_table_column: "some_child_key->some_child_key_2".to_string(),
            key_value: "outer_val->more_inner_val".to_string(),
        },
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
    uniq_key INT PRIMARY KEY CHILD OF existant_parent,
    ref_key REF CHILD existant_child_2,
}

DATA existant_parent {
    outer_val WITH existant_child {
        inner_val WITH existant_child_2 {
            more_inner_val
        }
    } WITH good_ref {
        7, "outer_val->more_inner_val"
    }
}
        "#
    );
}


#[test]
fn test_child_inner_foreign_key_all_good() {
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
    uniq_key INT PRIMARY KEY CHILD OF existant_parent,
    ref_key REF CHILD existant_child_2,
}

DATA existant_parent {
    outer_val WITH existant_child {
        inner_val WITH existant_child_2 {
            more_inner_val
        }
    } WITH good_ref {
        7, "inner_val->more_inner_val"
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
                {"some_key":"outer_val", "uniq_key": 7.0, "ref_key": "inner_val->more_inner_val"},
            ],
        })
    );
}

#[test]
fn test_child_inner_foreign_key_three_levels() {
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
    ref_key REF CHILD existant_child_2,
}

DATA existant_parent {
    outer_val WITH existant_child {
        inner_val WITH existant_child_2 {
            more_inner_val
        }
    }
}

DATA good_ref {
    "outer_val->inner_val->more_inner_val"
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
                { "ref_key": "outer_val->inner_val->more_inner_val"},
            ],
        })
    );
}

#[test]
fn test_child_inner_foreign_key_two_levels() {
    assert_compiles_data(
        r#"
TABLE existant_parent {
    some_key TEXT PRIMARY KEY,
}

TABLE existant_child {
    some_child_key TEXT PRIMARY KEY CHILD OF existant_parent,
}

TABLE good_ref {
    ref_key REF CHILD existant_child,
}

DATA existant_parent {
    outer_val WITH existant_child {
        inner_val
    }
}

DATA good_ref {
    outer_val->inner_val
}
        "#,
        json!({
            "existant_parent":[
                {"some_key":"outer_val"},
            ],
            "existant_child":[
                {"some_key":"outer_val", "some_child_key": "inner_val"},
            ],
            "good_ref":[
                { "ref_key": "outer_val->inner_val"},
            ],
        })
    );
}

#[test]
fn test_child_inner_foreign_key_diff_buckets_no_key() {
    assert_test_validaton_exception(
        DatabaseValidationError::NonExistingForeignKeyToChildTable {
            table_parent_keys: vec!["other_outer_val".to_string()],
            table_parent_tables: vec!["existant_parent".to_string()],
            table_parent_columns: vec!["some_key".to_string()],
            table_with_foreign_key: "good_ref".to_string(),
            foreign_key_column: "ref_key".to_string(),
            referred_table: "existant_child_2".to_string(),
            referred_table_column: "some_child_key->some_child_key_2".to_string(),
            key_value: "inner_val->more_inner_val".to_string(),
        },
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
    uniq_key INT PRIMARY KEY CHILD OF existant_parent,
    ref_key REF CHILD existant_child_2,
}

DATA existant_parent {
    outer_val WITH existant_child {
        inner_val WITH existant_child_2 {
            more_inner_val
        }
    };
    other_outer_val WITH good_ref {
        7, "inner_val->more_inner_val"
    };
}
        "#
    );
}

#[test]
fn test_child_inner_foreign_key_can_refer_to_unrelated_child_from_deeper() {
    assert_compiles_data(
        r#"
TABLE existant_parent {
    some_key TEXT PRIMARY KEY,
}

TABLE existant_child {
    some_child_key TEXT PRIMARY KEY CHILD OF existant_parent,
}

TABLE good_ref_parent_1 {
    p1 TEXT PRIMARY KEY,
}
TABLE good_ref_parent_2 {
    p2 TEXT PRIMARY KEY CHILD OF good_ref_parent_1,
}

TABLE good_ref {
    p3 TEXT PRIMARY KEY CHILD OF good_ref_parent_2,
    ref_key REF CHILD existant_child,
}

DATA existant_parent {
    outer_val WITH existant_child {
        inner_val
    }
}

DATA good_ref_parent_1 {
    k1 WITH good_ref_parent_2 {
        k2 WITH good_ref {
            k3, outer_val->inner_val
        }
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
            "good_ref_parent_1":[
                {"p1": "k1"}
            ],
            "good_ref_parent_2":[
                {"p1": "k1", "p2": "k2"}
            ],
            "good_ref":[
                {"p1": "k1", "p2": "k2", "p3": "k3", "ref_key": "outer_val->inner_val"}
            ],
        })
    );
}
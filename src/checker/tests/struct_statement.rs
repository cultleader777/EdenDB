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
fn test_child_primary_keys_with_nested_syntax() {
    assert_compiles_data(
        r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE disk {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
}

DATA STRUCT server [
    {
        hostname: mclassen
        WITH disk {
            dev_slot: "/dev/sda",
        }
    },
    {
        hostname: doofus
        WITH disk {
            dev_slot: "/dev/sda",
        }
    }
]
        "#,
        json!({
            "server": [
                {"hostname": "mclassen"},
                {"hostname": "doofus"},
            ],
            "disk": [
                {"hostname": "mclassen", "dev_slot": "/dev/sda"},
                {"hostname": "doofus", "dev_slot": "/dev/sda"},
            ],
        })
    );
}

#[test]
fn test_child_primary_keys_with_nested_syntax_deeper() {
    assert_compiles_data(
        r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE disk {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
}

TABLE partition {
    part_no INT PRIMARY KEY CHILD OF disk,
}

TABLE sector {
    sect_no INT PRIMARY KEY CHILD OF partition,
}

DATA STRUCT server [
    {
        hostname: mclassen
        WITH disk {
            dev_slot: "/dev/sda" WITH partition {
                part_no: 7 WITH sector {
                    sect_no: 77
                }
            }
        }
    }
]
        "#,
        json!({
            "server": [
                {"hostname": "mclassen"},
            ],
            "disk": [
                {"hostname": "mclassen", "dev_slot": "/dev/sda"},
            ],
            "partition": [
                {"hostname": "mclassen", "dev_slot": "/dev/sda", "part_no": 7.0},
            ],
            "sector": [
                {"hostname": "mclassen", "dev_slot": "/dev/sda", "part_no": 7.0, "sect_no": 77.0},
            ],
        })
    );
}

#[test]
fn test_child_primary_keys_with_nested_syntax_diff_children() {
    assert_compiles_data(
        r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE disk {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
}

TABLE cpu {
    cpu_slot INT PRIMARY KEY CHILD OF server,
}

DATA STRUCT server [
    {
        hostname: mclassen
        WITH disk [{
            dev_slot: "/dev/sda"
        }]
        WITH cpu [{
            cpu_slot: 0
        }]
    }
]
        "#,
        json!({
            "server": [
                {"hostname": "mclassen"},
            ],
            "disk": [
                {"hostname": "mclassen", "dev_slot": "/dev/sda"},
            ],
            "cpu": [
                {"hostname": "mclassen", "cpu_slot": 0.0},
            ],
        })
    );
}

#[test]
fn test_structured_data_errors_table_not_found() {
    assert_test_validaton_exception(
        DatabaseValidationError::TargetTableForDataNotFound {
            table_name: "server".to_string(),
        },
        r#"
DATA STRUCT server [
    {
        hostname: mclassen
    },
]
        "#,
    );
}

#[test]
fn test_structured_data_errors_column_not_found() {
    assert_test_validaton_exception(
        DatabaseValidationError::DataTargetColumnNotFound {
            table_name: "server".to_string(),
            target_column_name: "hostname".to_string(),
        },
        r#"
TABLE server {
    meow TEXT
}

DATA STRUCT server [
    {
        hostname: mclassen
    },
]
        "#,
    );
}


#[test]
fn test_structured_data_errors_duplicate_columns() {
    assert_test_validaton_exception(
        DatabaseValidationError::DuplicateStructuredDataFields {
            table_name: "server".to_string(),
            duplicated_column: "hostname".to_string(),
        },
        r#"
TABLE server {
    hostname TEXT,
}

DATA STRUCT server [
    {
        hostname: mclassen,
        hostname: boi,
    },
]
        "#,
    );
}

#[test]
fn test_structured_data_no_required_column() {
    assert_test_validaton_exception(
        DatabaseValidationError::DataRequiredNonDefaultColumnValueNotProvided {
            table_name: "server".to_string(),
            column_name: "hostname".to_string(),
        },
        r#"
TABLE server {
    hostname TEXT,
    ip TEXT,
}

DATA STRUCT server [
    {
        ip: 127.0.0.1,
    },
]
        "#,
    );
}

#[test]
fn test_structured_data_cant_parse_int() {
    assert_test_validaton_exception(
        DatabaseValidationError::DataCannotParseDataStructColumnValue {
            table_name: "stoof".to_string(),
            column_name: "some_int".to_string(),
            column_value: "thicc".to_string(),
            expected_type: DBType::DBInt,
        },
        r#"
TABLE stoof {
    some_int INT,
}

DATA STRUCT stoof [
    {
        some_int: thicc,
    },
]
        "#,
    );
}

#[test]
fn test_structured_data_defaults() {
    assert_compiles_data(
        r#"
TABLE stoof {
    some_int INT PRIMARY KEY,
    def_str TEXT DEFAULT d1,
    def_int INT DEFAULT 7,
    def_float FLOAT DEFAULT 7.77,
    def_bool BOOL DEFAULT true,
}

DATA STRUCT stoof [
    {
        some_int: 1,
    },
]
        "#,
        json!({
            "stoof": [
                {"some_int": 1.0, "def_str": "d1", "def_int": 7.0, "def_float": 7.77, "def_bool": true}
            ]
        })
    );
}

#[test]
fn test_structured_data_all_types() {
    assert_compiles_data(
        r#"
DATA STRUCT stoof { f_str: some text, f_int: 7, f_float: 7.77, f_bool: false }

TABLE stoof {
    f_str TEXT,
    f_int INT,
    f_float FLOAT,
    f_bool BOOL,
}
        "#,
        json!({
            "stoof": [
                {"f_str": "some text", "f_int": 7.0, "f_float": 7.77, "f_bool": false}
            ]
        })
    );
}

#[test]
fn test_structured_data_nested_duplicate_contextual_stack_regression() {
    assert_compiles_data(
        r#"
TABLE simple_table {
    some_field TEXT PRIMARY KEY,
    ot REF other_table,
}

TABLE other_table {
    other_field INT PRIMARY KEY,
    st REF simple_table,
}

DATA STRUCT simple_table {
    some_field: "abc", ot: 1 WITH other_table {
        other_field: 1
    }
}
        "#,
        json!({
            "simple_table": [
                {"some_field": "abc", "ot": 1.0},
            ],
            "other_table": [
                {"other_field": 1.0, "st": "abc"},
            ]
        })
    );
}

#[test]
fn test_structured_data_cant_parse_float() {
    assert_test_validaton_exception(
        DatabaseValidationError::DataCannotParseDataStructColumnValue {
            table_name: "stoof".to_string(),
            column_name: "some_int".to_string(),
            column_value: "thicc".to_string(),
            expected_type: DBType::DBFloat,
        },
        r#"
TABLE stoof {
    some_int FLOAT,
}

DATA STRUCT stoof [
    {
        some_int: thicc,
    },
]
        "#,
    );
}

#[test]
fn test_structured_data_recursive_insert() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExtraDataRecursiveInsert {
            parent_table: "simple_table".to_string(),
            extra_table: "simple_table".to_string(),
        },
        r#"
TABLE simple_table {
    some_field TEXT PRIMARY KEY,
}

DATA STRUCT simple_table {
    some_field: abc, WITH simple_table {
        some_field: "bca"
    }
}
        "#
    );
}

#[test]
fn test_structured_data_extra_table_not_found() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExtraDataTableNotFound {
            parent_table: "simple_table".to_string(),
            extra_table: "bozo".to_string(),
        },
        r#"
TABLE simple_table {
    some_field TEXT PRIMARY KEY,
}

DATA STRUCT simple_table {
    some_field: abc, WITH bozo {
        some_field: "bca"
    }
}
        "#
    );
}

#[test]
fn test_structured_data_no_fkeys_to_this_table() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExtraTableHasNoForeignKeysToThisTable {
            parent_table: "simple_table".to_string(),
            extra_table: "dummy_table".to_string(),
        },
        r#"
TABLE simple_table {
    some_field TEXT PRIMARY KEY,
}

TABLE dummy_table {
    other_field TEXT PRIMARY KEY,
}

DATA STRUCT simple_table {
    some_field: abc, WITH dummy_table {
        other_field: bca
    }
}
        "#
    );
}

#[test]
fn test_structured_data_multiple_fkeys_to_this_table() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExtraTableMultipleAmbigousForeignKeysToThisTable {
            parent_table: "simple_table".to_string(),
            extra_table: "dummy_table".to_string(),
            column_list: vec!["st_1".to_string(), "st_2".to_string()],
        },
        r#"
TABLE simple_table {
    some_field TEXT PRIMARY KEY,
}

TABLE dummy_table {
    other_field TEXT PRIMARY KEY,
    st_1 REF simple_table,
    st_2 REF simple_table,
}

DATA STRUCT simple_table {
    some_field: abc, WITH dummy_table {
        other_field: bca
    }
}
        "#
    );
}

#[test]
fn test_structured_data_cannot_redefine_ref_key_fk() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExtraTableCannotRedefineReferenceKey {
            parent_table: "simple_table".to_string(),
            extra_table: "dummy_table".to_string(),
            column_name: "st_1".to_string(),
        },
        r#"
TABLE simple_table {
    some_field TEXT PRIMARY KEY,
}

TABLE dummy_table {
    other_field TEXT PRIMARY KEY,
    st_1 REF simple_table,
}

DATA STRUCT simple_table {
    some_field: abc, WITH dummy_table {
        other_field: bca, st_1: boi
    }
}
        "#
    );
}

#[test]
fn test_structured_data_cannot_redefine_ref_key_parent_child() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExtraTableCannotRedefineReferenceKey {
            parent_table: "simple_table".to_string(),
            extra_table: "dummy_table".to_string(),
            column_name: "some_field".to_string(),
        },
        r#"
TABLE simple_table {
    some_field TEXT PRIMARY KEY,
}

TABLE dummy_table {
    other_field TEXT PRIMARY KEY CHILD OF simple_table,
}

DATA STRUCT simple_table {
    some_field: abc, WITH dummy_table {
        other_field: bca, some_field: boi
    }
}
        "#
    );
}

#[test]
fn test_structured_data_nested_duplicate_contextual_stack() {
    assert_test_validaton_exception(
        DatabaseValidationError::CyclingTablesInContextualInsertsNotAllowed {
            table_loop: vec![
                "simple_table".to_string(),
                "other_table".to_string(),
                "simple_table".to_string(),
            ],
        },
        r#"
TABLE simple_table {
    some_field TEXT PRIMARY KEY,
    ot REF other_table,
}

TABLE other_table {
    other_field INT PRIMARY KEY,
    st REF simple_table,
}

DATA STRUCT simple_table {
    some_field: "abc", ot: 1 WITH other_table {
        other_field: 1 WITH simple_table {
            some_field: "bca"
        }
    }
}
        "#
    );
}

#[test]
fn test_structured_data_nested_no_primary_key() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExtraDataParentMustHavePrimaryKey {
            parent_table: "simple_table".to_string(),
        },
        r#"
TABLE simple_table {
    some_field TEXT,
}

DATA STRUCT simple_table {
    some_field: abc WITH bananas {
        other_field: 1
    }
}
        "#
    );
}
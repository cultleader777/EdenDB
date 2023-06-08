#[cfg(test)]
use super::common::assert_compiles_data;
#[cfg(test)]
use super::common::assert_test_validaton_exception;
#[cfg(test)]
use crate::checker::errors::DatabaseValidationError;
#[cfg(test)]
use serde_json::json;

#[test]
fn test_validation_exception_data_exta_data_parent_must_have_pkey() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExtraDataParentMustHavePrimaryKey {
            parent_table: "cholo".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
}

DATA cholo {
    1 WITH cholo {
        2
    };
}
        "#,
    );
}

#[test]
fn test_validation_exception_data_exta_data_recursive_insert() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExtraDataRecursiveInsert {
            parent_table: "cholo".to_string(),
            extra_table: "cholo".to_string(),
        },
        r#"
TABLE cholo {
    id INT PRIMARY KEY,
}

DATA cholo {
    1 WITH cholo {
        2
    };
}
        "#,
    );
}

#[test]
fn test_validation_exception_data_extra_data_table_not_found() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExtraDataTableNotFound {
            parent_table: "cholo".to_string(),
            extra_table: "kukushkin".to_string(),
        },
        r#"
TABLE cholo {
    id INT PRIMARY KEY,
}

DATA cholo {
    1 WITH kukushkin {
        2
    };
}
        "#,
    );
}

#[test]
fn test_validation_exception_data_extra_data_table_no_foreign_keys_to_this_table() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExtraTableHasNoForeignKeysToThisTable {
            parent_table: "cholo".to_string(),
            extra_table: "kukushkin".to_string(),
        },
        r#"
TABLE cholo {
    id INT PRIMARY KEY,
}

TABLE kukushkin {
    id INT,
}

DATA cholo {
    1 WITH kukushkin {
        2
    };
}
        "#,
    );
}

#[test]
fn test_validation_exception_data_extra_data_table_multiple_ambig_foreign_keys() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExtraTableMultipleAmbigousForeignKeysToThisTable {
            parent_table: "cholo".to_string(),
            extra_table: "kukushkin".to_string(),
            column_list: vec!["first_cholo".to_string(), "second_cholo".to_string()],
        },
        r#"
TABLE cholo {
    id INT PRIMARY KEY,
}

TABLE kukushkin {
    first_cholo REF cholo,
    second_cholo REF cholo,
    id INT,
}

DATA cholo {
    1 WITH kukushkin {
        2
    };
}
        "#,
    );
}

#[test]
fn test_validation_exception_data_extra_data_table_cannot_redefine_foreign_key() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExtraTableCannotRedefineReferenceKey {
            parent_table: "cholo".to_string(),
            extra_table: "kukushkin".to_string(),
            column_name: "first_cholo".to_string(),
        },
        r#"
TABLE cholo {
    id INT PRIMARY KEY,
}

TABLE kukushkin {
    first_cholo REF cholo,
    id INT,
}

DATA cholo {
    1 WITH kukushkin(first_cholo, id) {
        1, 2;
    };
}
        "#,
    );
}

#[test]
fn test_dataframe_data_no_fields_set_fk() {
    assert_compiles_data(
        r#"
TABLE simple_table {
    some_field TEXT PRIMARY KEY,
}

TABLE dummy_table {
    other_field TEXT PRIMARY KEY,
    st REF simple_table,
}

DATA simple_table {
    trololo WITH dummy_table {
        chololo
    }
}
        "#,
        json!({
            "simple_table": [
                {"some_field": "trololo"},
            ],
            "dummy_table": [
                {"other_field": "chololo", "st": "trololo"},
            ],
        }),
    );
}

#[test]
fn test_dataframe_data_no_fields_set_fk_more_columns() {
    assert_compiles_data(
        r#"
TABLE simple_table {
    some_field TEXT PRIMARY KEY,
}

TABLE dummy_table {
    other_field TEXT PRIMARY KEY,
    col_1 INT,
    col_2 INT,
    st REF simple_table,
}

DATA simple_table {
    trololo WITH dummy_table {
        chololo, 1, 2
    }
}
        "#,
        json!({
            "simple_table": [
                {"some_field": "trololo"},
            ],
            "dummy_table": [
                {"other_field": "chololo", "st": "trololo", "col_1": 1.0, "col_2": 2.0},
            ],
        }),
    );
}

#[test]
fn test_dataframe_data_cannot_redefine_ref_key_child() {
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

DATA simple_table {
    trololo WITH dummy_table(other_field, some_field) {
        chololo, mooo
    }
}
        "#,
    );
}

#[test]
fn test_validation_exception_extra_data_too_many_columns() {
    assert_test_validaton_exception(
        DatabaseValidationError::DataTooManyColumns {
            table_name: "child".to_string(),
            row_index: 1,
            row_size: 3,
            expected_size: 2,
        },
        r#"
TABLE cholo {
    id INT PRIMARY KEY
}

TABLE child {
    child_id INT PRIMARY KEY CHILD OF cholo
}

DATA cholo {
    0 WITH child {
        1, 2;
    }
}
        "#,
    );
}

#[test]
fn test_validation_exception_extra_data_too_few_columns() {
    assert_test_validaton_exception(
        DatabaseValidationError::DataTooFewColumns {
            table_name: "child".to_string(),
            row_index: 1,
            row_size: 2,
            expected_size: 3,
        },
        r#"
TABLE cholo {
    id INT PRIMARY KEY
}

TABLE child {
    child_id INT PRIMARY KEY CHILD OF cholo,
    moar TEXT
}

DATA cholo {
    0 WITH child {
        1
    }
}
        "#,
    );
}

#[test]
fn test_dataframe_data_nested_duplicate_contextual_stack() {
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

DATA simple_table(some_field, ot) {
    abc, 1 WITH other_table(other_field) {
        1 WITH simple_table(some_field) {
            bca
        }
    }
}
        "#,
    );
}

#[test]
fn test_child_primary_keys_with_dataframe_syntax_diff_children() {
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

DATA server {
    mclassen
    WITH disk {
        "/dev/sda"
    }
    WITH cpu {
        0
    }
}
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
        }),
    );
}

#[test]
fn test_child_primary_keys_with_foreign_keys_exp_fields() {
    assert_compiles_data(
        r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE disks {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
    other_server REF server,
}

DATA server {
    mclassen WITH disks(dev_slot, other_server) {
        "/dev/sda", doofus;
    };
    doofus WITH disks(dev_slot, other_server) {
        "/dev/sda", mclassen;
    };
}
        "#,
        json!({
            "server": [
                {"hostname": "mclassen"},
                {"hostname": "doofus"},
            ],
            "disks": [
                {"hostname": "mclassen", "dev_slot": "/dev/sda", "other_server": "doofus"},
                {"hostname": "doofus", "dev_slot": "/dev/sda", "other_server": "mclassen"},
            ],
        }),
    );
}

#[test]
fn test_child_primary_keys_with_foreign_keys() {
    assert_compiles_data(
        r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE disks {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
    other_server REF server,
}

DATA server {
    mclassen WITH disks {
        "/dev/sda", doofus;
    };
    doofus WITH disks {
        "/dev/sda", mclassen;
    };
}
        "#,
        json!({
            "server": [
                {"hostname": "mclassen"},
                {"hostname": "doofus"},
            ],
            "disks": [
                {"hostname": "mclassen", "dev_slot": "/dev/sda", "other_server": "doofus"},
                {"hostname": "doofus", "dev_slot": "/dev/sda", "other_server": "mclassen"},
            ],
        }),
    );
}

#[test]
fn test_child_primary_keys_with_with_with() {
    assert_compiles_data(
        r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE disks {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
}

TABLE partitions {
    part_no INT PRIMARY KEY CHILD OF disks,
}

DATA server {
    mclassen WITH disks {
        "/dev/sda" WITH partitions {
            1;
            2;
        };
        "/dev/sdb" WITH partitions {
            3;
            4;
        };
    }
}
        "#,
        json!({
            "server": [
                {"hostname": "mclassen"},
            ],
            "disks": [
                {"hostname": "mclassen", "dev_slot": "/dev/sda"},
                {"hostname": "mclassen", "dev_slot": "/dev/sdb"},
            ],
            "partitions": [
                {"hostname": "mclassen", "dev_slot": "/dev/sda", "part_no": 1.0},
                {"hostname": "mclassen", "dev_slot": "/dev/sda", "part_no": 2.0},
                {"hostname": "mclassen", "dev_slot": "/dev/sdb", "part_no": 3.0},
                {"hostname": "mclassen", "dev_slot": "/dev/sdb", "part_no": 4.0},
            ]
        }),
    );
}

#[test]
fn test_child_primary_keys_with_with_rev_order() {
    assert_compiles_data(
        r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE disks {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
}

TABLE partition {
    part INT PRIMARY KEY CHILD OF disks,
}

TABLE sector {
    sec INT PRIMARY KEY CHILD OF partition,
}

DATA server {
    mclassen WITH disks {
        "/dev/sda" WITH partition {
            1 WITH sector {
                100;
            };
        };
        "/dev/sdb" WITH partition {
            2 WITH sector {
                200;
            };
        };
    }
}
        "#,
        json!({
            "server": [
                {"hostname": "mclassen"},
            ],
            "disks": [
                {"hostname": "mclassen", "dev_slot": "/dev/sda"},
                {"hostname": "mclassen", "dev_slot": "/dev/sdb"},
            ],
            "partition": [
                {"hostname": "mclassen", "dev_slot": "/dev/sda", "part": 1.0},
                {"hostname": "mclassen", "dev_slot": "/dev/sdb", "part": 2.0},
            ],
            "sector": [
                {"hostname": "mclassen", "dev_slot": "/dev/sda", "part": 1.0, "sec": 100.0},
                {"hostname": "mclassen", "dev_slot": "/dev/sdb", "part": 2.0, "sec": 200.0},
            ]
        }),
    );
}

#[test]
fn test_child_primary_keys_with_with_default_tuple_order() {
    assert_compiles_data(
        r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE disks {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
}

DATA server {
    mclassen WITH disks {
        "/dev/sda";
        "/dev/sdb";
    }
}
        "#,
        json!({
            "server": [
                {"hostname": "mclassen"},
            ],
            "disks": [
                {"hostname": "mclassen", "dev_slot": "/dev/sda"},
                {"hostname": "mclassen", "dev_slot": "/dev/sdb"},
            ]
        }),
    );
}

#[test]
fn test_child_primary_keys_with_with() {
    assert_compiles_data(
        r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE disks {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
}

DATA server {
    mclassen WITH disks(dev_slot) {
        "/dev/sda";
        "/dev/sdb";
    }
}
        "#,
        json!({
            "server": [
                {"hostname": "mclassen"}
            ],
            "disks": [
                {"hostname": "mclassen", "dev_slot": "/dev/sda"},
                {"hostname": "mclassen", "dev_slot": "/dev/sdb"},
            ],
        }),
    );
}

#[cfg(test)]
use super::common::assert_compiles_data;
#[cfg(test)]
use super::common::assert_test_validaton_exception;
#[cfg(test)]
use crate::checker::errors::DatabaseValidationError;
#[cfg(test)]
use serde_json::json;

#[test]
fn test_common_parent_foreign_keys() {
    assert_compiles_data(
        r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE reserved_port {
    port_number INT PRIMARY KEY CHILD OF server,
}

TABLE docker_container {
    container_name TEXT PRIMARY KEY CHILD OF server,
}

TABLE docker_container_port {
    port_name TEXT PRIMARY KEY CHILD OF docker_container,
    reserved_port REF reserved_port,
}

DATA server {
    epyc-1
}

DATA reserved_port {
    epyc-1, 1234
}

DATA docker_container {
    epyc-1, doofus;
}

DATA docker_container_port {
    epyc-1, doofus, somethin, 1234;
}

        "#,
        json!({
            "server": [
                {"hostname": "epyc-1"}
            ],
            "reserved_port": [
                {"hostname": "epyc-1", "port_number": 1234.0}
            ],
            "docker_container": [
                {"hostname": "epyc-1", "container_name": "doofus"}
            ],
            "docker_container_port": [
                {"hostname": "epyc-1", "container_name": "doofus", "port_name": "somethin", "reserved_port": 1234.0}
            ]
        }),
    );
}

#[test]
fn test_common_parent_error_no_common_ancestry() {
    assert_test_validaton_exception(
        DatabaseValidationError::ForeignKeyTableDoesNotShareCommonAncestorWithRefereeTable {
            referrer_table: "bogus_ref".to_string(),
            referrer_column: "ref_port_no".to_string(),
            referred_table: "reserved_port".to_string(),
        },
        r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE reserved_port {
    port_number INT PRIMARY KEY CHILD OF server,
}

TABLE bogus_ref {
    ref_port_no REF reserved_port,
}

        "#,
    );
}

#[test]
fn test_common_parent_error_no_common_ancestry_bad_parent() {
    assert_test_validaton_exception(
        DatabaseValidationError::ForeignKeyTableDoesNotShareCommonAncestorWithRefereeTable {
            referrer_table: "bogus_ref".to_string(),
            referrer_column: "ref_port_no".to_string(),
            referred_table: "reserved_port".to_string(),
        },
        r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE reserved_port {
    port_number INT PRIMARY KEY CHILD OF server,
}

TABLE disconnected_parent {
    some_column TEXT PRIMARY KEY,
}

TABLE bogus_ref {
    child_column TEXT PRIMARY KEY CHILD OF disconnected_parent,
    ref_port_no REF reserved_port,
}

        "#,
    );
}

#[test]
fn test_non_existing_children_elements_check() {
    assert_test_validaton_exception(
        DatabaseValidationError::NonExistingForeignKeyToChildTable {
            table_parent_keys: vec!["epyc-1".to_string()],
            table_parent_tables: vec!["server".to_string()],
            table_parent_columns: vec!["hostname".to_string()],
            table_with_foreign_key: "docker_container_port".to_string(),
            foreign_key_column: "res_port".to_string(),
            referred_table: "reserved_port".to_string(),
            referred_table_column: "port_number".to_string(),
            key_value: "4321".to_string(),
        },
        r#"

TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE reserved_port {
    port_number INT PRIMARY KEY CHILD OF server,
}

TABLE docker_container {
    container_name TEXT PRIMARY KEY CHILD OF server,
}

TABLE docker_container_port {
    port_name TEXT PRIMARY KEY CHILD OF docker_container,
    res_port REF reserved_port,
}

DATA server {
    epyc-1
}

DATA reserved_port {
    epyc-1, 1234
}

DATA docker_container {
    epyc-1, doofus;
}

DATA docker_container_port {
    epyc-1, doofus, somethin, 4321;
}

        "#,
    );
}

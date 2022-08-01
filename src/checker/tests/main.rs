#[cfg(test)]
use serde_json::json;
#[cfg(test)]
use crate::{checker::{errors::DatabaseValidationError, types::{DBIdentifier, DBType, KeyType, ColumnVector}, logic::AllData}, db_parser::InputSource};
#[cfg(test)]
use super::common::{assert_test_validaton_exception, assert_compiles_data};

#[test]
fn test_validating_basic_table_smoke() {
    let input = r#"

TABLE regions {
    mnemonic TEXT PRIMARY KEY,
    full_name TEXT,
}

TABLE servers {
    hostname TEXT PRIMARY KEY,
    region REF regions,
    disks INT DEFAULT 1,
}

DATA regions {
    europe, "Le europe, alpes and stuff";
    usa, MURICA;
    australia, "Doesn't exist";
}

DATA servers(hostname, region) {
    fizzle, europe;
    drizzle, usa;
}

    "#;
    use crate::db_parser::InputSource;

    let inp = &mut [InputSource { path: "test".to_string(), contents: Some(input.to_string()), source_dir: None, }];
    let parsed = crate::db_parser::parse_sources(inp);
    assert!(parsed.is_ok());
    let parsed = parsed.unwrap();
    let all_data = AllData::new(parsed);
    match all_data {
        Ok(all_data) => {
            assert_eq!(all_data.tables.len(), 2);

            let regions = &all_data.tables[0];
            let servers = &all_data.tables[1];

            assert_eq!(regions.name.as_str(), "regions");
            assert_eq!(regions.columns.len(), 2);
            let regions_mnemonic = &regions.columns[0];
            let regions_full_name = &regions.columns[1];

            assert_eq!(regions_mnemonic.column_name.as_str(), "mnemonic");
            assert_eq!(regions_mnemonic.data.column_type(), DBType::DBText);
            assert_eq!(regions_mnemonic.key_type, KeyType::PrimaryKey);
            assert!(!regions_mnemonic.data.has_default_value());

            assert_eq!(regions_full_name.column_name.as_str(), "full_name");
            assert_eq!(regions_full_name.data.column_type(), DBType::DBText);
            assert_eq!(regions_full_name.key_type, KeyType::NotAKey);
            assert!(!regions_full_name.data.has_default_value());

            if let ColumnVector::Strings(v) = &regions_mnemonic.data {
                assert_eq!(v.len(), 3);
                assert_eq!(v.v[0], "europe");
                assert_eq!(v.v[1], "usa");
                assert_eq!(v.v[2], "australia");
            } else {
                panic!()
            }
            if let ColumnVector::Strings(v) = &regions_full_name.data {
                assert_eq!(v.len(), 3);
                assert_eq!(v.v[0], "Le europe, alpes and stuff");
                assert_eq!(v.v[1], "MURICA");
                assert_eq!(v.v[2], "Doesn't exist");
            } else {
                panic!()
            }

            assert_eq!(servers.name.as_str(), "servers");
            assert_eq!(servers.columns.len(), 3);
            let servers_hostname = &servers.columns[0];
            let servers_region = &servers.columns[1];
            let servers_disks = &servers.columns[2];

            assert_eq!(servers_hostname.column_name.as_str(), "hostname");
            assert_eq!(servers_hostname.data.column_type(), DBType::DBText);
            assert_eq!(servers_hostname.key_type, KeyType::PrimaryKey);
            assert!(!servers_hostname.data.has_default_value());

            assert_eq!(servers_region.column_name.as_str(), "region");
            assert_eq!(servers_region.data.column_type(), DBType::DBText);
            assert_eq!(
                servers_region.key_type,
                KeyType::ForeignKey {
                    foreign_table: DBIdentifier::new("regions").unwrap()
                }
            );
            assert!(!servers_region.data.has_default_value());

            assert_eq!(servers_disks.column_name.as_str(), "disks");
            assert_eq!(servers_disks.data.column_type(), DBType::DBInt);
            assert_eq!(servers_disks.key_type, KeyType::NotAKey);
            if let ColumnVector::Ints(i) = &servers_disks.data {
                assert_eq!(i.default_value, Some(1));
            } else {
                panic!()
            }

            if let ColumnVector::Strings(v) = &servers_hostname.data {
                assert_eq!(v.len(), 2);
                assert_eq!(v.v[0], "fizzle");
                assert_eq!(v.v[1], "drizzle");
            } else {
                panic!()
            }
            if let ColumnVector::Strings(v) = &servers_region.data {
                assert_eq!(v.len(), 2);
                assert_eq!(v.v[0], "europe");
                assert_eq!(v.v[1], "usa");
            } else {
                panic!()
            }
            if let ColumnVector::Ints(v) = &servers_disks.data {
                assert_eq!(v.len(), 2);
                assert_eq!(v.v[0], 1);
                assert_eq!(v.v[1], 1);
            } else {
                panic!()
            }
        }
        Err(err) => {
            panic!("{err}")
        }
    }
}

#[test]
fn test_validating_with_statements() {
    let input = r#"

TABLE regions {
    mnemonic TEXT PRIMARY KEY,
    full_name TEXT,
}

TABLE servers {
    hostname TEXT PRIMARY KEY,
    region REF regions,
    disks INT DEFAULT 1,
}

DATA regions {
    europe, "Le europe, alpes and stuff" WITH servers(hostname) {
        fizzle;
    };
    usa, MURICA WITH servers(hostname) {
        drizzle;
    };
    australia, "Doesn't exist";
}

    "#;

    let inp = &mut [InputSource { path: "test".to_string(), contents: Some(input.to_string()), source_dir: None, }];
    let parsed = crate::db_parser::parse_sources(inp);
    assert!(parsed.is_ok());
    let parsed = parsed.unwrap();
    let all_data = AllData::new(parsed);
    match all_data {
        Ok(all_data) => {
            assert_eq!(all_data.tables.len(), 2);

            let regions = &all_data.tables[0];
            let servers = &all_data.tables[1];

            assert_eq!(regions.name.as_str(), "regions");
            assert_eq!(regions.columns.len(), 2);
            let regions_mnemonic = &regions.columns[0];
            let regions_full_name = &regions.columns[1];

            assert_eq!(regions_mnemonic.column_name.as_str(), "mnemonic");
            assert_eq!(regions_mnemonic.data.column_type(), DBType::DBText);
            assert_eq!(regions_mnemonic.key_type, KeyType::PrimaryKey);
            assert!(!regions_mnemonic.data.has_default_value());

            assert_eq!(regions_full_name.column_name.as_str(), "full_name");
            assert_eq!(regions_full_name.data.column_type(), DBType::DBText);
            assert_eq!(regions_full_name.key_type, KeyType::NotAKey);
            assert!(!regions_full_name.data.has_default_value());

            if let ColumnVector::Strings(v) = &regions_mnemonic.data {
                assert_eq!(v.len(), 3);
                assert_eq!(v.v[0], "europe");
                assert_eq!(v.v[1], "usa");
                assert_eq!(v.v[2], "australia");
            } else {
                panic!()
            }
            if let ColumnVector::Strings(v) = &regions_full_name.data {
                assert_eq!(v.len(), 3);
                assert_eq!(v.v[0], "Le europe, alpes and stuff");
                assert_eq!(v.v[1], "MURICA");
                assert_eq!(v.v[2], "Doesn't exist");
            } else {
                panic!()
            }

            assert_eq!(servers.name.as_str(), "servers");
            assert_eq!(servers.columns.len(), 3);
            let servers_hostname = &servers.columns[0];
            let servers_region = &servers.columns[1];
            let servers_disks = &servers.columns[2];

            assert_eq!(servers_hostname.column_name.as_str(), "hostname");
            assert_eq!(servers_hostname.data.column_type(), DBType::DBText);
            assert_eq!(servers_hostname.key_type, KeyType::PrimaryKey);
            assert!(!servers_hostname.data.has_default_value());

            assert_eq!(servers_region.column_name.as_str(), "region");
            assert_eq!(servers_region.data.column_type(), DBType::DBText);
            assert_eq!(
                servers_region.key_type,
                KeyType::ForeignKey {
                    foreign_table: DBIdentifier::new("regions").unwrap()
                }
            );
            assert!(!servers_region.data.has_default_value());

            assert_eq!(servers_disks.column_name.as_str(), "disks");
            assert_eq!(servers_disks.data.column_type(), DBType::DBInt);
            assert_eq!(servers_disks.key_type, KeyType::NotAKey);
            if let ColumnVector::Ints(v) = &servers_disks.data {
                assert_eq!(v.default_value, Some(1));
            } else {
                panic!()
            }

            if let ColumnVector::Strings(v) = &servers_hostname.data {
                assert_eq!(v.len(), 2);
                assert_eq!(v.v[0], "fizzle");
                assert_eq!(v.v[1], "drizzle");
            } else {
                panic!()
            }
            if let ColumnVector::Strings(v) = &servers_region.data {
                assert_eq!(v.len(), 2);
                assert_eq!(v.v[0], "europe");
                assert_eq!(v.v[1], "usa");
            } else {
                panic!()
            }
            if let ColumnVector::Ints(v) = &servers_disks.data {
                assert_eq!(v.len(), 2);
                assert_eq!(v.v[0], 1);
                assert_eq!(v.v[1], 1);
            } else {
                panic!()
            }
        }
        Err(err) => {
            panic!("{err}")
        }
    }
}


#[test]
fn test_validation_exception_defined_twice() {
    assert_test_validaton_exception(
        DatabaseValidationError::TableDefinedTwice {
            table_name: "cholo".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
}

TABLE cholo {
    moar INT,
}
        "#,
    );
}

#[test]
fn test_validation_exception_lowercase_table_name() {
    assert_test_validaton_exception(
        DatabaseValidationError::TableNameIsNotLowercase {
            table_name: "cHoLo".to_string(),
        },
        r#"
TABLE cHoLo {
    id INT,
}
        "#,
    );
}

#[test]
fn test_validation_exception_lowercase_column_name() {
    assert_test_validaton_exception(
        DatabaseValidationError::ColumnNameIsNotLowercase {
            table_name: "cholo".to_string(),
            column_name: "iD".to_string(),
        },
        r#"
TABLE cholo {
    iD INT,
}
        "#,
    );
}

#[test]
fn test_validation_exception_pkey_and_foreign_key_column() {
    assert_test_validaton_exception(
        DatabaseValidationError::ColumnIsPrimaryKeyAndForeignKey {
            table_name: "cholo".to_string(),
            column_name: "id".to_string(),
        },
        r#"
TABLE cholo {
    id REF some_table PRIMARY KEY,
}
        "#,
    );
}

#[test]
fn test_validation_exception_duplicate_column_names() {
    assert_test_validaton_exception(
        DatabaseValidationError::DuplicateColumnNames {
            table_name: "cholo".to_string(),
            column_name: "id".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
    id INT,
}
        "#,
    );
}

#[test]
fn test_validation_exception_more_than_one_pkey() {
    assert_test_validaton_exception(
        DatabaseValidationError::MoreThanOnePrimaryKey {
            table_name: "cholo".to_string(),
        },
        r#"
TABLE cholo {
    id1 INT PRIMARY KEY,
    id2 INT PRIMARY KEY,
}
        "#,
    );
}

#[test]
fn test_validation_exception_primary_key_must_be_first() {
    assert_test_validaton_exception(
        DatabaseValidationError::PrimaryKeyColumnMustBeFirst {
            table_name: "cholo".to_string(),
            column_name: "id2".to_string(),
        },
        r#"
TABLE cholo {
    id1 INT,
    id2 INT PRIMARY KEY,
}
        "#,
    );
}

#[test]
fn test_validation_exception_float_cannot_be_pkey() {
    assert_test_validaton_exception(
        DatabaseValidationError::FloatColumnCannotBePrimaryKey {
            table_name: "cholo".to_string(),
            column_name: "id".to_string(),
        },
        r#"
TABLE cholo {
    id FLOAT PRIMARY KEY,
}
        "#,
    );
}

#[test]
fn test_validation_exception_bool_cannot_be_pkey() {
    assert_test_validaton_exception(
        DatabaseValidationError::BooleanColumnCannotBePrimaryKey {
            table_name: "cholo".to_string(),
            column_name: "id".to_string(),
        },
        r#"
TABLE cholo {
    id BOOL PRIMARY KEY,
}
        "#,
    );
}

#[test]
fn test_validation_exception_uniq_constraint_column_doesnt_exist() {
    assert_test_validaton_exception(
        DatabaseValidationError::UniqConstraintColumnDoesntExist {
            table_name: "cholo".to_string(),
            column_name: "idz".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
    UNIQUE(idz)
}
        "#,
    );
}

#[test]
fn test_validation_exception_duplicate_uniq_constraints() {
    assert_test_validaton_exception(
        DatabaseValidationError::DuplicateUniqConstraints {
            table_name: "cholo".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
    key TEXT,
    UNIQUE(id, key),
    UNIQUE(key, id),
}
        "#,
    );
}

#[test]
fn test_validation_exception_unknown_column_type() {
    assert_test_validaton_exception(
        DatabaseValidationError::UnknownColumnType {
            table_name: "cholo".to_string(),
            column_name: "key".to_string(),
            column_type: "kukushkin".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
    key kukushkin,
}
        "#,
    );
}

#[test]
fn test_validation_exception_foreign_key_table_doesnt_exist() {
    assert_test_validaton_exception(
        DatabaseValidationError::ForeignKeyTableDoesntExist {
            referrer_table: "cholo".to_string(),
            referrer_column: "key".to_string(),
            referred_table: "kukushkin".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
    key REF kukushkin,
}
        "#,
    );
}

#[test]
fn test_validation_exception_foreign_key_table_doesnt_have_primary_key() {
    assert_test_validaton_exception(
        DatabaseValidationError::ForeignKeyTableDoesntHavePrimaryKey {
            referrer_table: "cholo".to_string(),
            referrer_column: "key".to_string(),
            referred_table: "kukushkin".to_string(),
        },
        r#"
TABLE kukushkin {
    id INT,
}

TABLE cholo {
    id INT,
    key REF kukushkin,
}
        "#,
    );
}

#[test]
fn test_validation_exception_cannot_parse_default_column_value() {
    assert_test_validaton_exception(
        DatabaseValidationError::CannotParseDefaultColumnValue {
            table_name: "cholo".to_string(),
            column_type: DBType::DBInt,
            column_name: "id".to_string(),
            the_value: "1.23".to_string(),
        },
        r#"
TABLE cholo {
    id INT DEFAULT 1.23,
}
        "#,
    );
}

#[test]
fn test_validation_exception_data_table_not_found() {
    assert_test_validaton_exception(
        DatabaseValidationError::TargetTableForDataNotFound {
            table_name: "cholo".to_string(),
        },
        r#"
DATA cholo {
    1,2,3;
}
        "#,
    );
}

#[test]
fn test_validation_exception_uniq_constraint_duplicate_column() {
    assert_test_validaton_exception(
        DatabaseValidationError::UniqConstraintDuplicateColumn {
            table_name: "cholo".to_string(),
            column_name: "id".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
    UNIQUE(id, id),
}
        "#,
    );
}

#[test]
fn test_validation_exception_data_column_not_found() {
    assert_test_validaton_exception(
        DatabaseValidationError::DataTargetColumnNotFound {
            table_name: "cholo".to_string(),
            target_column_name: "idz".to_string(),
        },
        r#"
TABLE cholo {
    id INT
}

DATA cholo(idz) {
    1;
}
        "#,
    );
}

#[test]
fn test_validation_exception_data_too_many_columns() {
    assert_test_validaton_exception(
        DatabaseValidationError::DataTooManyColumns {
            table_name: "cholo".to_string(),
            row_index: 2,
            row_size: 2,
            expected_size: 1,
        },
        r#"
TABLE cholo {
    id INT
}

DATA cholo(id) {
    0;
    1, 2;
}
        "#,
    );
}

#[test]
fn test_validation_exception_data_too_few_columns() {
    assert_test_validaton_exception(
        DatabaseValidationError::DataTooFewColumns {
            table_name: "cholo".to_string(),
            row_index: 2,
            row_size: 1,
            expected_size: 2,
        },
        r#"
TABLE cholo {
    id INT,
    id2 INT,
}

DATA cholo(id, id2) {
    0, 1;
    1;
}
        "#,
    );
}

#[test]
fn test_validation_exception_duplicate_data_fields() {
    assert_test_validaton_exception(
        DatabaseValidationError::DuplicateDataColumnNames {
            table_name: "cholo".to_string(),
            column_name: "id".to_string(),
        },
        r#"
TABLE cholo {
    id INT
}

DATA cholo(id, id) {
    0, 1;
    2, 3;
}
        "#,
    );
}

#[test]
fn test_validation_exception_data_cannot_parse_column_value() {
    assert_test_validaton_exception(
        DatabaseValidationError::DataCannotParseDataColumnValue {
            table_name: "cholo".to_string(),
            row_index: 1,
            column_index: 2,
            column_name: "id2".to_string(),
            column_value: "hello bois".to_string(),
            expected_type: DBType::DBInt,
        },
        r#"
TABLE cholo {
    id INT,
    id2 INT,
}

DATA cholo(id, id2) {
    1, hello bois;
}
        "#,
    );
}

#[test]
fn test_validation_exception_data_non_default_not_provided() {
    assert_test_validaton_exception(
        DatabaseValidationError::DataRequiredNonDefaultColumnValueNotProvided {
            table_name: "cholo".to_string(),
            column_name: "id2".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
    id2 INT,
}

DATA cholo(id) {
    1;
}
        "#,
    );
}

#[test]
fn test_validation_exception_foreign_keys_no_default_value() {
    assert_test_validaton_exception(
        DatabaseValidationError::PrimaryOrForeignKeysCannotHaveDefaultValue {
            table_name: "cholo".to_string(),
            column_name: "id2".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
    id2 REF kukushkin DEFAULT sup,
}
        "#,
    );
}

#[test]
fn test_validation_exception_data_duplicate_primary_key() {
    assert_test_validaton_exception(
        DatabaseValidationError::DuplicatePrimaryKey {
            table_name: "cholo".to_string(),
            value: "1".to_string(),
        },
        r#"
TABLE cholo {
    id INT PRIMARY KEY,
}
DATA cholo {
    1;
    1;
}
        "#,
    );
}

#[test]
fn test_validation_exception_data_duplicate_primary_key_string() {
    assert_test_validaton_exception(
        DatabaseValidationError::DuplicatePrimaryKey {
            table_name: "cholo".to_string(),
            value: "a".to_string(),
        },
        r#"
TABLE cholo {
    id TEXT PRIMARY KEY,
}
DATA cholo {
    a;
    a;
}
        "#,
    );
}

#[test]
fn test_validation_exception_data_non_existing_foreign_key() {
    assert_test_validaton_exception(
        DatabaseValidationError::NonExistingForeignKey {
            table_with_foreign_key: "kukushkin".to_string(),
            foreign_key_column: "cholo_id".to_string(),
            referred_table: "cholo".to_string(),
            referred_table_column: "id".to_string(),
            key_value: "2".to_string(),
        },
        r#"
TABLE cholo {
    id INT PRIMARY KEY,
}

TABLE kukushkin {
    cholo_id REF cholo,
}

DATA cholo {
    1;
}

DATA kukushkin {
    2;
}
        "#,
    );
}

#[test]
fn test_validation_exception_data_non_existing_foreign_key_text() {
    assert_test_validaton_exception(
        DatabaseValidationError::NonExistingForeignKey {
            table_with_foreign_key: "kukushkin".to_string(),
            foreign_key_column: "cholo_id".to_string(),
            referred_table: "cholo".to_string(),
            referred_table_column: "id".to_string(),
            key_value: "b".to_string(),
        },
        r#"
TABLE cholo {
    id TEXT PRIMARY KEY,
}

TABLE kukushkin {
    cholo_id REF cholo,
}

DATA cholo {
    a;
}

DATA kukushkin {
    b;
}
        "#,
    );
}

#[test]
fn test_validation_exception_float_cannot_be_in_unique_constraint() {
    assert_test_validaton_exception(
        DatabaseValidationError::FloatColumnCannotBeInUniqueConstraint {
            table_name: "cholo".to_string(),
            column_name: "other".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
    other FLOAT,
    UNIQUE(id, other)
}
        "#,
    );
}

#[test]
fn test_validation_exception_uniq_constraint_violated() {
    assert_test_validaton_exception(
        DatabaseValidationError::UniqConstraintViolated {
            table_name: "cholo".to_string(),
            tuple_definition: "(id_1, id_2)".to_string(),
            tuple_value: "(1, ab)".to_string(),
        },
        r#"
TABLE cholo {
    id_1 INT,
    id_2 TEXT,
    UNIQUE(id_1, id_2)
}

DATA cholo {
    1, ab;
    1, ab;
}
        "#,
    );
}

#[test]
fn test_child_primary_keys_simple() {
    assert_compiles_data(
        r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE disks {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
}

DATA server {
    mclassen;
}

DATA disks {
    mclassen, "/dev/sda";
    mclassen, "/dev/sdb";
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
        })
    );
}

#[test]
fn test_child_primary_keys_duplicates_with_diff_parents_allowed() {
    assert_compiles_data(
        r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE disks {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
}

DATA server {
    mclassen;
    doofus;
}

DATA disks {
    mclassen, "/dev/sda";
    doofus, "/dev/sda";
}
        "#,
        json!({
            "server": [
                {"hostname": "mclassen"},
                {"hostname": "doofus"},
            ],
            "disks": [
                {"hostname": "mclassen", "dev_slot": "/dev/sda"},
                {"hostname": "doofus", "dev_slot": "/dev/sda"},
            ],
        })
    );
}

#[test]
fn test_validation_exception_duplicate_child_keys() {
    // every parent must exist with such key?..
    // check if my next parent with such key prefix exists while discarding my own key!!
    assert_test_validaton_exception(
        DatabaseValidationError::FoundDuplicateChildPrimaryKeySet {
            table_name: "disks".to_string(),
            columns: "(hostname, dev_slot)".to_string(),
            duplicate_values: "(mclassen, /dev/sda)".to_string(),
        },
        r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE disks {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
}

DATA server {
    mclassen;
}

DATA disks {
    mclassen, "/dev/sda";
    mclassen, "/dev/sda";
}
        "#,
    );
}

#[test]
fn test_validation_exception_duplicate_child_keys_l2() {
    // every parent must exist with such key?..
    // check if my next parent with such key prefix exists while discarding my own key!!
    assert_test_validaton_exception(
        DatabaseValidationError::FoundDuplicateChildPrimaryKeySet {
            table_name: "disks".to_string(),
            columns: "(hostname, dev_slot)".to_string(),
            duplicate_values: "(mclassen, /dev/sda)".to_string(),
        },
        r#"
TABLE partitions {
    partition TEXT PRIMARY KEY CHILD OF disks,
}

TABLE disks {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
}

TABLE server {
    hostname TEXT PRIMARY KEY,
}

DATA server {
    mclassen;
}

DATA disks {
    mclassen, "/dev/sda";
    mclassen, "/dev/sda";
}

DATA partitions {
    mclassen, "/dev/sda", "/dev/sda1";
}
        "#,
    );
}

#[test]
fn test_validation_exception_non_existing_parent() {
    // every parent must exist with such key?..
    // check if my next parent with such key prefix exists while discarding my own key!!
    assert_test_validaton_exception(
        DatabaseValidationError::ParentRecordWithSuchPrimaryKeysDoesntExist {
            parent_table: "server".to_string(),
            parent_columns_names_searched: "(hostname)".to_string(),
            parent_columns_to_find: "(thiccboi)".to_string(),
        },
        r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE disks {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
}

DATA server {
    mclassen;
}

DATA disks {
    thiccboi, "/dev/sda";
}
        "#,
    );
}

#[test]
fn test_validation_exception_non_existing_child_table() {
    assert_test_validaton_exception(
        DatabaseValidationError::NonExistingChildPrimaryKeyTable {
            table_name: "disks".to_string(),
            column_name: "dev_slot".to_string(),
            referred_table: "server".to_string(),
        },
        r#"
TABLE disks {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
}
        "#,
    );
}

#[test]
fn test_validation_exception_parent_table_no_pkey() {
    assert_test_validaton_exception(
        DatabaseValidationError::ParentTableHasNoPrimaryKey {
            table_name: "disks".to_string(),
            column_name: "dev_slot".to_string(),
            referred_table: "server".to_string(),
        },
        r#"
TABLE server {
    hostname TEXT,
}

TABLE disks {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
}
        "#,
    );
}

#[test]
fn test_validation_exception_parent_table_looping() {
    assert_test_validaton_exception(
        DatabaseValidationError::ChildPrimaryKeysLoopDetected {
            table_names: vec![
                "datacenter".to_string(),
                "disks".to_string(),
                "server".to_string(),
                "datacenter".to_string(),
            ],
        },
        r#"
TABLE datacenter {
    mnemonic TEXT PRIMARY KEY CHILD OF disks,
}

TABLE server {
    hostname TEXT PRIMARY KEY CHILD OF datacenter,
}

TABLE disks {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
}
        "#,
    );
}

#[test]
fn test_validation_exception_child_of_itself() {
    assert_test_validaton_exception(
        DatabaseValidationError::ChildPrimaryKeysLoopDetected {
            table_names: vec!["datacenter".to_string(), "datacenter".to_string()],
        },
        r#"
TABLE datacenter {
    mnemonic TEXT PRIMARY KEY CHILD OF datacenter,
}
        "#,
    );
}

#[test]
fn test_validation_exception_child_columns_clash_with_parent() {
    assert_test_validaton_exception(
        DatabaseValidationError::ParentPrimaryKeyColumnNameClashesWithChildColumnName {
            parent_table: "datacenter".to_string(),
            parent_column: "mnemonic".to_string(),
            child_table: "server".to_string(),
            child_column: "mnemonic".to_string(),
        },
        r#"
TABLE datacenter {
    mnemonic TEXT PRIMARY KEY,
}

TABLE server {
    datacenter TEXT PRIMARY KEY CHILD OF datacenter,
    mnemonic TEXT
}
        "#,
    );
}

#[test]
fn test_validation_exception_exclusive_data_defined_multiple_times() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExclusiveDataDefinedMultipleTimes {
            table_name: "datacenter".to_string(),
        },
        r#"
TABLE datacenter {
    mnemonic TEXT PRIMARY KEY,
}

DATA EXCLUSIVE datacenter {
    dc1
}

DATA datacenter {
    dc2
}
        "#,
    );
}

#[test]
fn test_validation_exception_exclusive_data_defined_multiple_times_with() {
    assert_test_validaton_exception(
        DatabaseValidationError::ExclusiveDataDefinedMultipleTimes {
            table_name: "server".to_string(),
        },
        r#"
TABLE datacenter {
    mnemonic TEXT PRIMARY KEY,
}

TABLE server {
    hostname TEXT PRIMARY KEY,
    datacenter REF datacenter,
}

DATA datacenter {
    dc1 WITH server {
        roofus;
    }
}

DATA EXCLUSIVE server {
    doofus, dc1;
}
        "#,
    );
}

#[test]
fn test_boolean_basic() {
    assert_compiles_data(
        r#"
DATA stoof {
    true, false;
    false, true;
}

TABLE stoof {
    a_bool BOOL,
    b_bool BOOL,
}
        "#,
        json!({
            "stoof": [
                {"a_bool": true, "b_bool": false},
                {"a_bool": false, "b_bool": true},
            ]
        })
    );
}

#[test]
fn test_boolean_parse_failure() {
    assert_test_validaton_exception(
        DatabaseValidationError::DataCannotParseDataColumnValue {
            table_name: "stoof".to_string(),
            row_index: 1,
            column_index: 1,
            column_name: "a_bool".to_string(),
            column_value: "1".to_string(),
            expected_type: DBType::DBBool,
        },
        r#"
DATA stoof {
    1, 0;
    0, 1;
}

TABLE stoof {
    a_bool BOOL,
    b_bool BOOL,
}
        "#,
    );
}

#[test]
fn test_reserved_column_name() {
    assert_test_validaton_exception(
        DatabaseValidationError::ColumnNameIsReserved {
            table_name: "cholo".to_string(),
            column_name: "rowid".to_string(),
            reserved_names: vec![
                "rowid".to_string(),
                "parent".to_string(),
                "children_".to_string(),
                "referrers_".to_string(),
            ],
        },
        r#"
TABLE cholo {
    rowid INT,
}
        "#,
    );
}

#[test]
fn test_prohibit_nan_floats() {
    assert_test_validaton_exception(
        DatabaseValidationError::NanOrInfiniteFloatNumbersAreNotAllowed {
            table_name: "cholo".to_string(),
            column_name: "col".to_string(),
            column_value: "NaN".to_string(),
            row_index: 2,
        },
        r#"
TABLE cholo {
    col FLOAT,
}

DATA cholo {
    2.5;
    NaN;
}
        "#,
    );
}


#[test]
fn test_prohibit_inf_floats() {
    assert_test_validaton_exception(
        DatabaseValidationError::NanOrInfiniteFloatNumbersAreNotAllowed {
            table_name: "cholo".to_string(),
            column_name: "col".to_string(),
            column_value: "inf".to_string(),
            row_index: 2,
        },
        r#"
TABLE cholo {
    col FLOAT,
}

DATA cholo {
    2.5;
    inf;
}
        "#,
    );
}

#[test]
fn test_prohibit_neg_inf_floats() {
    assert_test_validaton_exception(
        DatabaseValidationError::NanOrInfiniteFloatNumbersAreNotAllowed {
            table_name: "cholo".to_string(),
            column_name: "col".to_string(),
            column_value: "-inf".to_string(),
            row_index: 2,
        },
        r#"
TABLE cholo {
    col FLOAT,
}

DATA cholo {
    2.5;
    -inf;
}
        "#,
    );
}

#[test]
fn test_source_comments() {
    assert_compiles_data(
        r#"
// hello
TABLE server {// I'm commenty
    hostname TEXT PRIMARY KEY, // lots of comm
} // today

TABLE disks {
    dev_slot TEXT PRIMARY KEY CHILD OF server,
}

DATA server { // moar comment
    mclassen; // another comment
}

DATA disks {
    mclassen, "/dev/sda";// almost thear
    mclassen, "/dev/sdb"; //comm
}

// fin
        "#,
        json!({
            "server": [
                {"hostname": "mclassen"},
            ],
            "disks": [
                {"hostname": "mclassen", "dev_slot": "/dev/sda"},
                {"hostname": "mclassen", "dev_slot": "/dev/sdb"},
            ]
        })
    );
}
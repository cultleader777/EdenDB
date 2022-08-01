#[cfg(test)]
use serde_json::json;

#[cfg(test)]
use super::common::assert_compiles_data;

#[test]
fn test_regression_1() {
    assert_compiles_data(r#"
TABLE thic_boi {
  id INT,
  name TEXT,
  b BOOL,
  f FLOAT,
  fk REF some_enum,
}

TABLE some_enum {
  name TEXT PRIMARY KEY,
}

TABLE enum_child_a {
  inner_name_a TEXT PRIMARY KEY CHILD OF some_enum,
}

TABLE enum_child_b {
  inner_name_b TEXT PRIMARY KEY CHILD OF some_enum,
}

DATA thic_boi {
  1, hey ho, true, 1.23, warm;
  2, here she goes, false, 3.21, hot;
  3, either blah, true, 5.43, hot;
}

DATA EXCLUSIVE some_enum {
  warm WITH enum_child_a {
    barely warm;
    medium warm;
  } WITH enum_child_b {
    barely degrees;
    medium degrees;
  };
  hot;
}
"#, json!({
        "thic_boi": [
            {"id": 1.0, "name": "hey ho", "b": true, "f": 1.23, "fk": "warm"},
            {"id": 2.0, "name": "here she goes", "b": false, "f": 3.21, "fk": "hot"},
            {"id": 3.0, "name": "either blah", "b": true, "f": 5.43, "fk": "hot"},
        ],
        "some_enum": [
            {"name": "warm"},
            {"name": "hot"},
        ],
        "enum_child_a": [
            {"name": "warm", "inner_name_a": "barely warm"},
            {"name": "warm", "inner_name_a": "medium warm"},
        ],
        "enum_child_b": [
            {"name": "warm", "inner_name_b": "barely degrees"},
            {"name": "warm", "inner_name_b": "medium degrees"},
        ],
    }));
}

#[test]
fn test_regression_2() {
    // colons pass, wut?
    let source = r#"
    TABLE docker_container_port {
        port_name: TEXT PRIMARY KEY CHILD OF docker_container,
        reserved_port: REF reserved_port,
    }
"#;
    let mut inp = [crate::db_parser::InputSource { contents: Some(source.to_string()), path: "test".to_string(), source_dir: None, }];
    let parsed = crate::db_parser::parse_sources(&mut inp);
    assert!(parsed.is_err());
}

#[test]
fn test_regression_sparse_passing_of_child_columns() {
    assert_compiles_data(r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE reserved_port {
    port_number INT PRIMARY KEY,
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
    1234
}

DATA docker_container {
    epyc-1, doofus WITH docker_container_port {
        somethin, 1234
    }
}
"#, json!({
        "server": [
            {"hostname": "epyc-1"},
        ],
        "reserved_port": [
            {"port_number": 1234.0},
        ],
        "docker_container": [
            {"hostname": "epyc-1", "container_name": "doofus"},
        ],
        "docker_container_port": [
            {"hostname": "epyc-1", "container_name": "doofus", "reserved_port": 1234.0, "port_name": "somethin"},
        ],
    }));
}

#[test]
fn test_regression_sparse_passing_of_child_columns_structured() {
    assert_compiles_data(r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE reserved_port {
    port_number INT PRIMARY KEY,
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
    1234
}

DATA STRUCT docker_container {
    hostname: epyc-1, container_name: doofus WITH docker_container_port {
        port_name: somethin, reserved_port: 1234
    }
}
"#, json!({
        "server": [
            {"hostname": "epyc-1"},
        ],
        "reserved_port": [
            {"port_number": 1234.0},
        ],
        "docker_container": [
            {"hostname": "epyc-1", "container_name": "doofus"},
        ],
        "docker_container_port": [
            {"hostname": "epyc-1", "container_name": "doofus", "reserved_port": 1234.0, "port_name": "somethin"},
        ],
    }));
}

#[test]
fn test_regression_uniq_constraints_involve_parent_keys() {
    assert_compiles_data(r#"
TABLE server {
    hostname TEXT PRIMARY KEY,
}

TABLE server_volume {
  volume_name TEXT PRIMARY KEY CHILD OF server,
  directory_path TEXT,
  UNIQUE(hostname, directory_path)
}
"#, json!({
        "server": [],
        "server_volume": [],
    }));
}

#[test]
fn test_regression_multiple_ancestors_reference() {
    assert_compiles_data(r#"
TABLE server {
  hostname TEXT PRIMARY KEY,
}

TABLE server_volume {
  volume_name TEXT PRIMARY KEY CHILD OF server,
  directory_path TEXT,
  UNIQUE(hostname, directory_path)
}

TABLE server_volume_usage_type {
    usage_type TEXT PRIMARY KEY,
}

DATA EXCLUSIVE server_volume_usage_type {
    read;
    write;
}

TABLE server_volume_usage_contract {
    usage_contract TEXT PRIMARY KEY,
}

DATA EXCLUSIVE server_volume_usage_contract {
    read_only;
    one_writer_many_readers;
    exclusive;
}

TABLE server_volume_use {
  volume_user TEXT PRIMARY KEY CHILD OF server_volume,
  usage_kind REF server_volume_usage_type,
  usage_contract REF server_volume_usage_contract,
  UNIQUE(hostname, volume_user)
}

TABLE docker_image {
  reference TEXT PRIMARY KEY,
  tag TEXT,
  version TEXT,
  UNIQUE(tag)
}

TABLE docker_container {
  name TEXT PRIMARY KEY CHILD OF server,
  image REF docker_image,
}

TABLE docker_container_mount {
  path_in_container TEXT PRIMARY KEY CHILD OF docker_container,
  volume_use REF server_volume_use,
}

DATA docker_image {
    postgres, postgres-tag, 12.12;
}

DATA server {
    host-a
      WITH server_volume {
        vol-a, "/volumes/vol-a" WITH server_volume_use {
            postgres_instance, write, exclusive
        }
      }
      WITH docker_container {
        pg-instance, postgres WITH docker_container_mount {
            "/var/lib/postgresql", postgres_instance
        }
      };
}

"#, json!({
        "server": [
            {"hostname": "host-a"},
        ],
        "server_volume": [
            {"hostname": "host-a", "volume_name": "vol-a", "directory_path": "/volumes/vol-a"},
        ],
        "server_volume_use": [
            {"hostname": "host-a", "volume_name": "vol-a", "volume_user": "postgres_instance", "usage_kind": "write", "usage_contract": "exclusive"},
        ],
        "docker_image": [
            {"reference": "postgres", "tag": "postgres-tag", "version": "12.12"},
        ],
        "docker_container": [
            {"hostname": "host-a", "name": "pg-instance", "image": "postgres"},
        ],
        "docker_container_mount": [
            {"hostname": "host-a", "name": "pg-instance", "path_in_container": "/var/lib/postgresql", "volume_use": "postgres_instance"},
        ],
        "server_volume_usage_type": [
            {"usage_type": "read"},
            {"usage_type": "write"},
        ],
        "server_volume_usage_contract": [
            {"usage_contract": "read_only"},
            {"usage_contract": "one_writer_many_readers"},
            {"usage_contract": "exclusive"},
        ],
    }));
}
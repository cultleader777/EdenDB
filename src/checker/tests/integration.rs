#[cfg(test)]
use super::common::assert_compiles_data_paths;
#[cfg(feature = "datalog")]
#[cfg(test)]
use super::common::assert_test_validaton_exception;
#[cfg(test)]
use super::common::random_test_dir;
#[cfg(feature = "datalog")]
#[cfg(test)]
use crate::checker::errors::DatabaseValidationError;
#[cfg(test)]
use serde_json::json;

#[cfg(feature = "datalog")]
#[test]
fn test_lua_and_datalog_integration() {
    assert_test_validaton_exception(
        DatabaseValidationError::DatalogProofOffendersFound {
            table_name: "cholo".to_string(),
            proof_expression: "\n    OUTPUT(Offender) :- t_cholo__is_even(true, Offender).\n"
                .to_string(),
            comment: "fail even offenders".to_string(),
            offending_columns: vec!["{
  \"id\": 2.0,
  \"is_even\": true
}"
            .to_string()],
        },
        r#"
TABLE cholo {
    id INT,
    is_even BOOL GENERATED AS { id % 2 == 0 },
}

DATA cholo {
    1;
    2;
    3;
}

PROOF "fail even offenders" NONE EXIST OF cholo DATALOG {
    OUTPUT(Offender) :- t_cholo__is_even(true, Offender).
}
        "#,
    );
}

#[test]
fn test_smoke_multiple_files() {
    let tmp_dir = random_test_dir();
    std::fs::write(
        tmp_dir.join("test.lua"),
        r#"
      function isEven(number)
        return number % 2 == 0
      end
    "#,
    )
    .unwrap();

    std::fs::write(
        tmp_dir.join("root.edl"),
        r#"
      TABLE test_table {
        id INT,
        is_even BOOL GENERATED AS { isEven(id) }
      }

      MATERIALIZED VIEW test_mview {
        maybe_id INT
      } AS {
        SELECT id * is_even
        FROM test_table
      }

      INCLUDE LUA "TMP_DIR/test.lua"
      INCLUDE "TMP_DIR/data.edl"
    "#
        .replace("TMP_DIR", tmp_dir.to_str().unwrap()),
    )
    .unwrap();

    std::fs::write(
        tmp_dir.join("data.edl"),
        r#"
      DATA test_table {
        1;
        2;
        3;
      }

      INCLUDE "TMP_DIR/sql_proof.edl"
    "#
        .replace("TMP_DIR", tmp_dir.to_str().unwrap()),
    )
    .unwrap();

    std::fs::write(
        tmp_dir.join("sql_proof.edl"),
        r#"
      PROOF "all ids above 0" NONE EXIST OF test_table SQL {
        SELECT rowid
        FROM test_table
        WHERE id <= 0
      }
    "#,
    )
    .unwrap();

    #[cfg(feature = "datalog")]
    std::fs::write(
        tmp_dir.join("prolog_test.edl"),
        r#"
      PROOF "no id with number 4" NONE EXIST OF test_table DATALOG {
        OUTPUT(Offender) :- t_test_table__id(4, Offender).
      }
    "#,
    )
    .unwrap();

    let paths = [
      "root.edl",
      #[cfg(feature = "datalog")]
      "prolog_test.edl"
    ]
        .iter()
        .map(|i| tmp_dir.join(i).to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    assert_compiles_data_paths(
        paths.as_slice(),
        json!({
            "test_table": [
                {"id": 1.0, "is_even": false},
                {"id": 2.0, "is_even": true},
                {"id": 3.0, "is_even": false},
            ],
            "test_mview": [
                {"maybe_id": 0.0},
                {"maybe_id": 0.0},
                {"maybe_id": 2.0},
            ],
        }),
    );
}

#[test]
fn test_smoke_multiple_files_diff_dir() {
    let tmp_dir = random_test_dir();
    let inner_a = tmp_dir.join("inner-a");
    let inner_b = tmp_dir.join("inner-b");
    let inner_c = inner_b.join("inner-c");
    let ocaml_data_module = tmp_dir.join("ocaml-data-module");
    let ocaml_data_module_bin = ocaml_data_module.join("bin");

    std::fs::create_dir(&ocaml_data_module).unwrap();
    std::fs::create_dir(&ocaml_data_module_bin).unwrap();
    std::fs::create_dir(&inner_a).unwrap();
    std::fs::create_dir(&inner_b).unwrap();
    std::fs::create_dir(&inner_c).unwrap();
    std::fs::write(
        inner_a.join("test.lua"),
        r#"
      function isEven(number)
        return number % 2 == 0
      end
    "#,
    )
    .unwrap();

    std::fs::write(
        tmp_dir.join("root.edl"),
        r#"
      TABLE test_table {
        id INT,
        is_even BOOL GENERATED AS { isEven(id) }
      }

      MATERIALIZED VIEW test_mview {
        maybe_id INT
      } AS {
        SELECT id * is_even
        FROM test_table
      }

      DATA MODULE OCAML "ocaml-data-module"
      INCLUDE LUA "inner-a/test.lua"
      INCLUDE "inner-b/data.edl"
    "#
        .replace("TMP_DIR", tmp_dir.to_str().unwrap()),
    )
    .unwrap();

    std::fs::write(
        inner_b.join("data.edl"),
        r#"
      DATA test_table {
        1;
        2;
        3;
      }

      INCLUDE "inner-c/sql_proof.edl"
    "#
        .replace("TMP_DIR", tmp_dir.to_str().unwrap()),
    )
    .unwrap();

    std::fs::write(
        inner_c.join("sql_proof.edl"),
        r#"
      PROOF "all ids above 0" NONE EXIST OF test_table SQL {
        SELECT rowid
        FROM test_table
        WHERE id <= 0
      }
    "#,
    )
    .unwrap();

    #[cfg(feature = "datalog")]
    std::fs::write(
        tmp_dir.join("prolog_test.edl"),
        r#"
      PROOF "no id with number 4" NONE EXIST OF test_table DATALOG {
        OUTPUT(Offender) :- t_test_table__id(4, Offender).
      }
    "#,
    )
    .unwrap();

    std::fs::write(
        ocaml_data_module_bin.join("implementation.ml"),
        r#"
open! Context
open! Db_types

let define_data () =
  def_test_table { id = 4 };
  ()
    "#
    )
    .unwrap();

    let paths = [
      "root.edl",
      #[cfg(feature = "datalog")]
      "prolog_test.edl",
    ]
        .iter()
        .map(|i| tmp_dir.join(i).to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    assert_compiles_data_paths(
        paths.as_slice(),
        json!({
            "test_table": [
                {"id": 1.0, "is_even": false},
                {"id": 2.0, "is_even": true},
                {"id": 3.0, "is_even": false},
                {"id": 4.0, "is_even": true},
            ],
            "test_mview": [
                {"maybe_id": 0.0},
                {"maybe_id": 0.0},
                {"maybe_id": 2.0},
                {"maybe_id": 4.0},
            ],
        }),
    );
}

#[test]
fn test_ocaml_data_modules() {
    let tmp_dir = random_test_dir();
    let data_module = tmp_dir.join("data-module");
    let data_module_bin = data_module.join("bin");

    std::fs::create_dir(data_module).unwrap();
    std::fs::create_dir(&data_module_bin).unwrap();

    std::fs::write(
        tmp_dir.join("root.edl"),
        r#"
      DATA MODULE OCAML "data-module"

      TABLE test_table {
        id INT PRIMARY KEY,
        is_even BOOL GENERATED AS { id % 2 == 0 }
      }

      TABLE other_table {
        other_id INT PRIMARY KEY CHILD OF test_table,
        some_float FLOAT,
        some_bool BOOL,
        some_text TEXT,
        some_int INT,
        some_default INT DEFAULT 4,
        generated_int INT GENERATED AS { some_int * 7 },
      }
    "#
        .replace("TMP_DIR", tmp_dir.to_str().unwrap()),
    )
    .unwrap();

    std::fs::write(data_module_bin.join("implementation.ml"), r#"
open! Context
open! Db_types

let define_data () =
  (* define data in tables here *)
  (* def_server (mk_server ~hostname:"foo" ~ram_mb:777); *)
  let a = mk_test_table ~id:123 () in
  let parent = pkey_of_test_table a in
  def_test_table a;
  def_other_table (mk_other_table_child_of_test_table
    ~parent ~other_id:21 ~some_float:3.14 ~some_bool:false ~some_text:"henlo" ~some_int:42 ());
  def_other_table (mk_other_table
    ~id:123 ~other_id:28
    ~some_float:7.14
    ~some_bool:true ~some_text:"moo"
    ~some_default:40
    ~some_int:777 ());
  ()
"#).unwrap();

    let paths = [
      "root.edl",
    ]
        .iter()
        .map(|i| tmp_dir.join(i).to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    assert_compiles_data_paths(
        paths.as_slice(),
        json!({
            "test_table": [
                {"id": 123.0, "is_even": false},
            ],
            "other_table": [
                {"id": 123.0,
                 "other_id": 21.0,
                 "some_float": 3.14,
                 "some_bool": false,
                 "some_text": "henlo",
                 "some_int": 42.0,
                 "generated_int": 294.0,
                 "some_default": 4.0},
                {"id": 123.0,
                 "other_id": 28.0,
                 "some_float": 7.14,
                 "some_bool": true,
                 "some_text": "moo",
                 "some_int": 777.0,
                 "generated_int": 5439.0,
                 "some_default": 40.0},
            ],
        }),
    );
}

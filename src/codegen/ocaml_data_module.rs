use std::{path::PathBuf, str::FromStr};
use std::fmt::Write;

use convert_case::Casing;

use crate::{codegen::write_file_check_if_different, checker::{logic::AllData, types::{DBType, KeyType, DataTable, DataColumn}}};

pub enum OCamlCodegenOutput {
    MkDir {
        path: String,
    },
    MkFile {
        path: String,
        content: Vec<u8>,
        overwrite_if_exists: bool,
    }
}

pub fn generate_ocaml_data_module_sources(data: &AllData, path: &str) -> Vec<OCamlCodegenOutput> {
    let mut res = Vec::new();

    let path_buf = PathBuf::from_str(path).unwrap();
    res.push(OCamlCodegenOutput::MkDir { path: path.to_string() });
    res.push(OCamlCodegenOutput::MkFile {
        path: path_buf.join("dune-project").to_str().unwrap().to_string(),
        content: dune_project().as_bytes().to_vec(),
        overwrite_if_exists: false,
    });
    let bin_dir = path_buf.join("bin");
    res.push(OCamlCodegenOutput::MkDir { path: bin_dir.to_str().unwrap().to_string() });
    res.push(OCamlCodegenOutput::MkFile {
        path: bin_dir.join("dune").to_str().unwrap().to_string(),
        content: dune_file().as_bytes().to_vec(),
        overwrite_if_exists: false,
    });
    res.push(OCamlCodegenOutput::MkFile {
        path: bin_dir.join("main.ml").to_str().unwrap().to_string(),
        content: main_file().as_bytes().to_vec(),
        overwrite_if_exists: true,
    });
    let context_files = generate_context_files(data);
    res.push(OCamlCodegenOutput::MkFile {
        path: bin_dir.join("context.ml").to_str().unwrap().to_string(),
        content: context_files.ml.into_bytes(),
        overwrite_if_exists: true,
    });
    res.push(OCamlCodegenOutput::MkFile {
        path: bin_dir.join("context.mli").to_str().unwrap().to_string(),
        content: context_files.mli.into_bytes(),
        overwrite_if_exists: true,
    });
    res.push(OCamlCodegenOutput::MkFile {
        path: bin_dir.join("db_types.ml").to_str().unwrap().to_string(),
        content: generate_db_types(data).into_bytes(),
        overwrite_if_exists: true,
    });
    res.push(OCamlCodegenOutput::MkFile {
        path: bin_dir.join("main.ml").to_str().unwrap().to_string(),
        content: generate_main().as_bytes().to_vec(),
        overwrite_if_exists: true,
    });
    res.push(OCamlCodegenOutput::MkFile {
        path: bin_dir.join("implementation.mli").to_str().unwrap().to_string(),
        content: implementation_mli().as_bytes().to_vec(),
        overwrite_if_exists: true,
    });
    res.push(OCamlCodegenOutput::MkFile {
        path: bin_dir.join("implementation.ml").to_str().unwrap().to_string(),
        content: implementation_ml_template(data).into_bytes(),
        overwrite_if_exists: false,
    });

    res
}

struct ContextFiles {
    ml: String,
    mli: String,
}

fn relevant_columns(t: &DataTable) -> impl std::iter::Iterator<Item = &DataColumn> {
    t.columns.iter().filter(|i| i.generate_expression.is_none())
}

fn implementation_ml_template(data: &AllData) -> String {
    let maybe_example =
        if data.tables.len() > 0 {
            let table = &data.tables[0];
            let args = relevant_columns(table).map(|c| {
                format!("~{}:{}", c.column_name.as_str(), edb_type_to_ocaml_example_value(c.data.column_type()))
            }).collect::<Vec<_>>().join(" ");
            format!(
                "  (* def_{} (mk_{} {}); *)",
                table.name.as_str(),
                table.name.as_str(),
                args
            )
        } else {
            "".to_string()
        };
    format!(r#"open Context
open! Db_types
open! Context

let define_data () =
  (* define data in tables here *)
{maybe_example}
  ()
"#)
}

fn implementation_mli() -> &'static str {
    r#"
val define_data: unit -> unit
"#
}

fn generate_main() -> &'static str {
    r#"
let () =
  Implementation.define_data ();
  Context.dump_state_to_stdout ()
"#
}

fn ocaml_data_emit_key() -> &'static str {
    "fVxVFfkSj2yJGb2ury32Z4SkNm9QDTnqb4FKeygXw1"
}

pub fn ocaml_data_emit_key_begin() -> String {
    format!("time to read boi {}", ocaml_data_emit_key())
}

pub fn ocaml_data_emit_key_end() -> String {
    format!("reading over boi {}", ocaml_data_emit_key())
}

fn db_type(data: &[&DataTable]) -> String {
    let mut res = String::new();

    res += "type database = {\n";
    for t in data {
        res += "  mutable ";
        res += t.name.as_str();
        res += ": Db_types.table_row_";
        res += t.name.as_str();
        res += " growing_array;\n";
    }
    res += "}\n";

    res
}

fn generate_context_files(data: &AllData) -> ContextFiles {
    let mut res = String::new();
    let mut mli = String::new();
    let tables_sorted = data.tables_sorted();
    let db_type = db_type(&tables_sorted);

    mli += mli_growing_array();
    res += lib_growing_array();

    res += &db_type;
    mli += &db_type;

    res += "type database_to_serialize = {\n";
    for t in &tables_sorted {
        res += "  ";
        res += t.name.as_str();
        res += ": Db_types.table_row_";
        res += t.name.as_str();
        res += " array;\n";
    }
    res += "} [@@deriving yojson]\n";

    // default row values for growing array
    for t in &tables_sorted {
        res += "let _default_table_row_";
        res += t.name.as_str();
        res += ": Db_types.table_row_";
        res += t.name.as_str();
        res += " = {\n";
        for c in relevant_columns(t) {
            res += "  ";
            res += c.column_name.as_str();
            res += " = ";
            res += edb_type_to_ocaml_default_value(c.data.column_type());
            res += ";\n";
        }
        res += "}\n";
        res += "\n";
    }

    res += "let database_create () : database = {\n";
    for t in &tables_sorted {
        res += "  ";
        res += t.name.as_str();
        res += " = grarr_create ~default_value:_default_table_row_";
        res += t.name.as_str();
        res += ";\n";
    }
    res += "}\n";
    res += "let global_db: database = database_create ()\n";
    res += "\n";

    res += "let merge_into_database ~(into: database) ~(from: database) =\n";
    res += "  (* We don't want to merge same dbs *)\n";
    res += "  assert (into == from |> not);\n";
    for t in &tables_sorted {
        res += "  iter from.";
        res += t.name.as_str();
        res += " (fun elem -> push into.";
        res += t.name.as_str();
        res += " elem);\n";
    }
    res += "  ()\n";

    res += r#"

let merge_to_global (from: database) =
  merge_into_database ~into:global_db ~from
"#;

    res += "let dump_state_to_stdout () =\n";
    res += "  let final_output: database_to_serialize = {\n";
    for t in &tables_sorted {
        res += "    ";
        res += t.name.as_str();
        res += " = to_array global_db.";
        res += t.name.as_str();
        res += ";\n";
    }
    res += "  } in\n";
    // we're copying stuff, so what, if this ever becomes a problem we'll use capn proto
    res += "  print_endline \"\";\n"; // just in case user printed something and didn't finish line
    writeln!(&mut res, "  print_endline \"{}\";", ocaml_data_emit_key_begin()).unwrap();
    res += "  database_to_serialize_to_yojson final_output |> Yojson.Safe.to_string |> print_endline;\n";
    writeln!(&mut res, "  print_endline \"{}\"", ocaml_data_emit_key_end()).unwrap();
    res += "\n";

    mli += r#"
val database_create: unit -> database
val dump_state_to_stdout: unit -> unit
val global_db: database
val merge_into_database: into:database -> from:database -> unit
val merge_to_global: database -> unit
"#;

    ContextFiles { ml: res, mli }
}

pub fn generate_db_types(data: &AllData) -> String {
    let mut res = String::new();
    let tables_sorted = data.tables_sorted();

    // primary keys
    for t in &tables_sorted {
        let pkeys = t.primary_keys_with_parents();
        if !pkeys.is_empty() {
            let pasc_name = t.name.as_str().to_case(convert_case::Case::Pascal);
            res += "type table_pkey_";
            res += t.name.as_str();
            res += " = TablePkey";
            res += &pasc_name;
            res += " of ";
            if pkeys.len() > 1 {
                res += "(";
                res += &pkeys.iter().map(|i| {
                    let col = &t.columns[*i];
                    edb_type_to_ocaml_type(col.data.column_type())
                }).collect::<Vec<_>>().join(" * ");
                res += ")";
            } else {
                assert_eq!(pkeys.len(), 1);
                res += edb_type_to_ocaml_type(t.columns[pkeys[0]].data.column_type());
            }
            res += "\n";
        }
    }

    // row types
    for t in &tables_sorted {
        res += "type table_row_";
        res += t.name.as_str();
        res += " = {\n";
        for c in relevant_columns(t) {
            res += "  ";
            res += c.column_name.as_str();
            res += ": ";
            res += edb_type_to_ocaml_type(c.data.column_type());
            res += ";\n";
        }
        res += "} [@@deriving yojson]\n";
        res += "\n";
    }

    // row constructors
    for t in &tables_sorted {
        res += "let mk_";
        res += t.name.as_str();
        res += "";

        // values without default column
        for c in relevant_columns(t) {
            if !c.data.has_default_value() {
                res += " ~(";
                res += c.column_name.as_str();
                res += ": ";
                res += edb_type_to_ocaml_type(c.data.column_type());
                res += ")";
            }
        }

        // values with default column
        for c in relevant_columns(t) {
            if c.data.has_default_value() {
                let def_value = c.data.default_value().unwrap();
                res += " ?(";
                res += c.column_name.as_str();
                res += " = ";
                if c.data.column_type() == DBType::Text {
                    let replaced = def_value.replace("\"", "\\\"");
                    res += "\"";
                    res += &replaced;
                    res += "\"";
                } else {
                    res += &def_value;
                }
                res += ")";
            }
        }

        res += " () : table_row_";
        res += t.name.as_str();
        res += " =\n";

        res += "  {\n";
        for c in relevant_columns(t) {
            res += "    ";
            res += c.column_name.as_str();
            res += ";\n";
        }
        res += "  }\n";
        res += "\n";
    }

    // get primary keys of tables function
    for t in &tables_sorted {
        let pkeys = t.primary_keys_with_parents();
        if !pkeys.is_empty() {
            let pasc_name = t.name.as_str().to_case(convert_case::Case::Pascal);
            res += "let pkey_of_";
            res += t.name.as_str();
            res += " (input: table_row_";
            res += t.name.as_str();
            res += ") : table_pkey_";
            res += t.name.as_str();
            res += " =\n";
            res += "  TablePkey";
            res += &pasc_name;
            res += " ";
            if pkeys.len() > 1 {
                res += "(";
                res += &pkeys.iter().map(|i| {
                    format!("input.{}", t.columns[*i].column_name.as_str())
                }).collect::<Vec<_>>().join(", ");
                res += ")";
            } else {
                assert_eq!(pkeys.len(), 1);
                res += "input.";
                res += t.columns[pkeys[0]].column_name.as_str();
            }
            res += "\n";
            res += "\n";
        }
    }

    // create child of parent
    for t in &tables_sorted {
        for ot in &tables_sorted {
            if t.name != ot.name {
                if let Some(pt) = ot.parent_table() {
                    if pt == t.name {
                        let parent_pkeys = t.primary_keys_with_parents();
                        let pasc_parent = t.name.as_str().to_case(convert_case::Case::Pascal);
                        res += "let mk_";
                        res += ot.name.as_str();
                        res += "_child_of_";
                        res += t.name.as_str();
                        res += " ~(parent: table_pkey_";
                        res += t.name.as_str();
                        res += ") ";

                        // values without default column
                        for c in relevant_columns(ot) {
                            if matches!(c.key_type, KeyType::ParentPrimary { .. }) {
                                continue;
                            }

                            if !c.data.has_default_value() {
                                res += "~(";
                                res += c.column_name.as_str();
                                res += ": ";
                                res += edb_type_to_ocaml_type(c.data.column_type());
                                res += ") ";
                            }
                        }

                        for c in relevant_columns(ot) {
                            if matches!(c.key_type, KeyType::ParentPrimary { .. }) {
                                continue;
                            }

                            if c.data.has_default_value() {
                                let def_value = c.data.default_value().unwrap();
                                res += "?(";
                                res += c.column_name.as_str();
                                res += " = ";
                                if c.data.column_type() == DBType::Text {
                                    let replaced = def_value.replace("\"", "\\\"");
                                    res += "\"";
                                    res += &replaced;
                                    res += "\"";
                                } else {
                                    res += &def_value;
                                }
                                res += ") ";
                            }
                        }

                        res += "() : table_row_";
                        res += ot.name.as_str();
                        res += " =\n";
                        res += "  let TablePkey";
                        res += &pasc_parent;
                        res += " ";
                        if parent_pkeys.len() > 1 {
                            res += "(";
                            res += &parent_pkeys.iter().map(|i| {
                                t.columns[*i].column_name.as_str()
                            }).collect::<Vec<_>>().join(", ");
                            res += ") = parent in\n";
                        } else {
                            assert_eq!(parent_pkeys.len(), 1);
                            res += t.columns[parent_pkeys[0]].column_name.as_str();
                            res += " = parent in\n";
                        }

                        res += "  {\n";
                        for c in relevant_columns(ot) {
                            res += "    ";
                            res += c.column_name.as_str();
                            res += ";\n";
                        }
                        res += "  }\n";
                    }
                }
            }
        }
    }

    res
}

fn edb_type_to_ocaml_type(col: DBType) -> &'static str {
    match col {
        DBType::Text => "string",
        DBType::Bool => "bool",
        DBType::Float => "float",
        DBType::Int => "int",
    }
}

fn edb_type_to_ocaml_default_value(col: DBType) -> &'static str {
    match col {
        DBType::Text => "\"\"",
        DBType::Bool => "false",
        DBType::Float => "0.0",
        DBType::Int => "0",
    }
}

fn edb_type_to_ocaml_example_value(col: DBType) -> &'static str {
    match col {
        DBType::Text => "\"foo\"",
        DBType::Bool => "true",
        DBType::Float => "1.7",
        DBType::Int => "777",
    }
}

pub fn flush_to_disk(out_dir: &PathBuf, files: &[OCamlCodegenOutput]) {
    for file in files {
        match file {
            OCamlCodegenOutput::MkDir { path } => {
                std::fs::create_dir_all(out_dir.join(path)).unwrap();
            }
            OCamlCodegenOutput::MkFile { path, content, overwrite_if_exists } => {
                let out_path = out_dir.join(path);
                if *overwrite_if_exists || !out_path.exists() {
                    write_file_check_if_different(
                        &out_path,
                        content
                    );
                }
            }
        }
    }
}

fn mli_growing_array() -> &'static str {
    r#"
type 'a growing_array

val grarr_create: default_value:'a -> 'a growing_array
val push: 'a growing_array -> 'a -> unit
val filter: 'a growing_array -> ('a -> bool) -> 'a growing_array
val iter: 'a growing_array -> ('a -> unit) -> unit
val map: 'a growing_array -> ('a -> 'a) -> 'a growing_array
val filter_map: 'a growing_array -> ('a -> 'a option) -> 'a growing_array
"#
}

fn lib_growing_array() -> &'static str {
    r#"
type 'a growing_array = {
    mutable arr: 'a array array;
    mutable level: int;
    mutable level_offset: int;
    mutable capacity: int;
    mutable size: int;
    default_value: 'a;
}

let grarr_initial_level = 16

let grarr_max_levels = 32

let compute_level_offset level =
    16 lsl (level - 1)

let grarr_create ~default_value =
    let empty_arr = [||] in
    let l1_arr = Array.make grarr_max_levels empty_arr in
    l1_arr.(0) <- Array.make grarr_initial_level default_value;
    let initial_level = 0 in
    {
        arr = l1_arr;
        level = initial_level;
        level_offset = compute_level_offset initial_level;
        capacity = compute_level_offset (initial_level + 1);
        size = 0;
        default_value = default_value;
    }

let push (arr: 'a growing_array) (to_push: 'a) =
    if arr.size < arr.capacity then (
        arr.arr.(arr.level).(arr.size - arr.level_offset) <- to_push;
        arr.size <- arr.size + 1;
    ) else (
        arr.level <- arr.level + 1;

        let new_level_offset = compute_level_offset arr.level in
        arr.level_offset <- new_level_offset;
        arr.capacity <- compute_level_offset (arr.level + 1);
        arr.arr.(arr.level) <- Array.make new_level_offset arr.default_value;

        arr.arr.(arr.level).(arr.size - arr.level_offset) <- to_push;
        arr.size <- arr.size + 1;
    )

let iter (arr: 'a growing_array) (func: 'a -> unit) =
    let keep_going = ref true in
    let level = ref 0 in
    while !keep_going do
        let level_offset = compute_level_offset !level in
        if level_offset < arr.size then (
            let go_to = min (Array.length arr.arr.(!level)) (arr.size - level_offset) - 1 in
            for i = 0 to go_to do
                func arr.arr.(!level).(i)
            done
        ) else (
            keep_going := false;
        );
        incr level;
    done

let filter (arr: 'a growing_array) (func: 'a -> bool): 'a growing_array =
  let new_arr = grarr_create ~default_value:arr.default_value in
  iter arr (fun i ->
      if func i then (
        push new_arr i
      )
    );
  new_arr

let filter_map (arr: 'a growing_array) (func: 'a -> 'a option): 'a growing_array =
  let new_arr = grarr_create ~default_value:arr.default_value in
  iter arr (fun i ->
      match func i with
      | Some res -> push new_arr res
      | None -> ()
    );
  new_arr

let map (arr: 'a growing_array) (func: 'a -> 'a): 'a growing_array =
  let new_arr = grarr_create ~default_value:arr.default_value in
  iter arr (fun i ->
      push new_arr (func i)
    );
  new_arr

let to_array (arr: 'a growing_array) : 'a array =
  let res = Array.make arr.size arr.default_value in
  let counter = ref 0 in
  iter arr (fun i ->
      res.(!counter) <- i;
      incr counter;
    );
  res
"#
}

fn main_file() -> &'static str {
    r#"
let () =
  print_endline "Hello world"
"#
}

fn dune_file() -> &'static str {
    r#"
(executable
 (public_name main)
 (name main)
 (preprocess (pps ppx_deriving_yojson))
 (libraries yojson)
 (modes (native exe)))
(env
  (_ (flags (:standard -w -33))))
"#
}

fn dune_project() -> &'static str {
    r#"(lang dune 2.1)

(name data-module)

(authors "Nobody")
(maintainers "Cares")
(license LICENSE)

(package
 (name data-module)
 (depends ocaml dune))
"#
}

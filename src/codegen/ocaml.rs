use convert_case::{Case, Casing};

use crate::checker::{logic::AllData, types::SerializationVector};

use super::{CodeGenerator, CodegenOutputFile};

pub struct OCamlCodegen {
    pub debug_dump_function: bool,
    pub edb_data_file_name: String,
    /// .ml and .mli are added to the end of this string
    pub db_source_file_name: String,
}

impl Default for OCamlCodegen {
    fn default() -> Self {
        Self {
            debug_dump_function: false,
            edb_data_file_name: "edb_data.bin".to_string(),
            db_source_file_name: "database".to_string(),
        }
    }
}

impl CodeGenerator for OCamlCodegen {
    fn generate(&self, data: &crate::checker::logic::AllData) -> super::CodegenOutputs {
        let mut impl_content = String::with_capacity(1024);
        let comp = OcamlCodegenCompute::new(data, self);

        impl_content += r#"(* DB bytes *)
let data_blob = [%blob "edb_data.bin"]
"#;
        impl_content += "\n";

        impl_content += ocaml_mini_deserialization_library();

        impl_content += "(* Table row pointer types *)\n";
        for trow_pointer in &comp.table_pointer_types {
            impl_content += trow_pointer;
            impl_content += "\n";
        }
        impl_content += "\n";

        impl_content += "(* Table row types *)\n";
        for trow_pointer in &comp.table_structs {
            impl_content += trow_pointer;
            impl_content += "\n";
        }
        impl_content += "\n";

        impl_content += "(* Table definitions *)\n";
        for trow_pointer in &comp.table_definitions {
            impl_content += trow_pointer;
            impl_content += "\n";
        }
        impl_content += "\n";

        impl_content += "(* Database definition *)\n";
        impl_content += &comp.database_definition;

        impl_content += "(* Deserialization function *)\n";
        impl_content += &comp.deserialization_function;

        impl_content += "(* Database loading *)\n";
        impl_content += "let db: database = deserialize ()\n";

        if self.debug_dump_function {
            impl_content += "\n";
            impl_content += "(* Dump function *)\n";
            impl_content += &comp.debug_dump_function;
        }

        let mut mli_content = String::with_capacity(1024);
        mli_content += "(* Table row pointer types *)\n";
        for trow_pointer in &comp.table_pointer_types_decl {
            mli_content += trow_pointer;
            mli_content += "\n";
        }
        mli_content += "\n";

        mli_content += "(* Table row types *)\n";
        for trow_pointer in &comp.table_structs {
            mli_content += trow_pointer;
            mli_content += "\n";
        }
        mli_content += "\n";

        mli_content += "(* Table definitions *)\n";
        for trow_pointer in &comp.table_definitions {
            mli_content += trow_pointer;
            mli_content += "\n";
        }
        mli_content += "\n";

        mli_content += "(* Database definition *)\n";
        mli_content += &comp.database_definition;

        mli_content += "(* Database constant *)\n";
        mli_content += "val db: database\n";

        if self.debug_dump_function {
            mli_content += "\n";
            mli_content += "(* Dump function *)\n";
            mli_content += "val dump_to_stdout: database -> unit\n";
        }

        let impl_src = CodegenOutputFile {
            filename: format!("{}.ml", self.db_source_file_name),
            content: impl_content.into_bytes(),
        };

        let mli_src = CodegenOutputFile {
            filename: format!("{}.mli", self.db_source_file_name),
            content: mli_content.into_bytes(),
        };

        let data_src = CodegenOutputFile {
            filename: self.edb_data_file_name.clone(),
            content: comp.uncompressed_data_bytes.clone(),
        };

        super::CodegenOutputs {
            uncompressed_edb_data: comp.uncompressed_data_bytes,
            files: vec![impl_src, mli_src, data_src],
        }
    }
}
struct OcamlCodegenCompute {
    table_pointer_types: Vec<String>,
    table_pointer_types_decl: Vec<String>,
    table_structs: Vec<String>,
    table_definitions: Vec<String>,
    database_definition: String,
    deserialization_function: String,
    debug_dump_function: String,
    uncompressed_data_bytes: Vec<u8>,
}

impl OcamlCodegenCompute {
    fn new(data: &AllData, opt: &OCamlCodegen) -> OcamlCodegenCompute {
        let serialization_vectors = data.serialization_vectors();
        let table_pointer_types_decl = table_pointer_types(data, false, opt.debug_dump_function);
        let table_pointer_types = table_pointer_types(data, true, opt.debug_dump_function);
        let table_structs = table_structs(data, opt.debug_dump_function, &serialization_vectors);
        let table_definitions = table_definitions(data, &serialization_vectors);
        let database_definition = database_definition(data);
        let deserialization_function = deserialization_function(data, &serialization_vectors);
        let debug_dump_function = debug_dump_function(data);
        // TODO: having issues with ocaml lz4 libraries, enable compression
        let uncompressed_data_bytes = super::dump_as_bytes(&serialization_vectors);
        OcamlCodegenCompute {
            table_pointer_types,
            table_pointer_types_decl,
            table_structs,
            table_definitions,
            database_definition,
            debug_dump_function,
            deserialization_function,
            uncompressed_data_bytes,
        }
    }
}

fn ocaml_mini_deserialization_library() -> &'static str {
    r#"
(* ensure we're in 64 bit territory *)
let () = assert ((Int.max_int |> Int64.of_int) > (Int32.max_int |> Int64.of_int32))

let fetch_i64_number (buffer: string) (cursor: int ref) =
  let c = !cursor in
  cursor := c + 8;
  String.get_int64_le buffer c

let fetch_bool (buffer: string) (cursor: int ref) =
  let c = !cursor in
  incr cursor;
  match String.get buffer c |> Char.code with
  | 0 -> false
  | _ -> true

let fetch_f64_float (buffer: string) (cursor: int ref) =
  let i64 = fetch_i64_number buffer cursor in
  Int64.float_of_bits i64

let fetch_string (buffer: string) (cursor: int ref) =
  let string_len = fetch_i64_number buffer cursor |> Int64.to_int in
  let string_start = !cursor in
  cursor := string_start + string_len;
  String.sub buffer string_start string_len

let fetch_vector_generic ~init_element ~push_function (buffer: string) (cursor: int ref) =
  let size = fetch_i64_number buffer cursor |> Int64.to_int in
  let res = Array.make size init_element in
  for i = 0 to size - 1 do
    res.(i) <- push_function buffer cursor;
  done;
  res

let fetch_i64_vector =
  fetch_vector_generic
    ~init_element:0
    ~push_function:(fun buffer cursor ->
        fetch_i64_number buffer cursor |> Int64.to_int
      )

let fetch_f64_vector =
  fetch_vector_generic
    ~init_element:0.0
    ~push_function:fetch_f64_float

let fetch_bool_vector =
  fetch_vector_generic
    ~init_element:false
    ~push_function:fetch_bool

let fetch_string_vector =
  fetch_vector_generic
    ~init_element:""
    ~push_function:fetch_string

let fetch_i64_nested_vector =
  fetch_vector_generic
    ~init_element:[||]
    ~push_function:fetch_i64_vector

"#
}

fn table_pointer_types(
    data: &AllData,
    with_implementation: bool,
    with_yojson: bool,
) -> Vec<String> {
    let mut res = Vec::with_capacity(data.tables.len());
    for t in data.tables_sorted() {
        let mut output = String::new();
        output += &format!("type table_row_pointer_{}", t.name.as_str());
        if with_implementation {
            let tname_pasc_case = t.name.as_str().to_case(Case::Pascal);
            output += " = TableRowPointer";
            output += &tname_pasc_case;
            output += " of int";

            if with_yojson {
                output += " [@@deriving yojson]";
            }
        }

        res.push(output);
    }
    res
}

fn table_structs(
    data: &AllData,
    with_yojson: bool,
    ser_vecs: &Vec<SerializationVector>,
) -> Vec<String> {
    let mut res = Vec::with_capacity(data.tables.len());
    for t in data.tables_sorted() {
        let mut output = String::new();
        output += &format!("type table_row_{} = {{\n", t.name.as_str());

        for sv in ser_vecs {
            if sv.table_name() == t.name.as_str() {
                let (cname, ctype) = match sv {
                    crate::checker::types::SerializationVector::Strings(sv) => {
                        (&sv.column_name, "string".to_string())
                    }
                    crate::checker::types::SerializationVector::Ints(sv) => {
                        (&sv.column_name, "int".to_string())
                    }
                    crate::checker::types::SerializationVector::Floats(sv) => {
                        (&sv.column_name, "float".to_string())
                    }
                    crate::checker::types::SerializationVector::Bools(sv) => {
                        (&sv.column_name, "bool".to_string())
                    }
                    crate::checker::types::SerializationVector::Fkeys { sv, foreign_table } => (
                        &sv.column_name,
                        format!("table_row_pointer_{}", foreign_table),
                    ),
                    crate::checker::types::SerializationVector::FkeysOneToMany {
                        sv,
                        foreign_table,
                    } => (
                        &sv.column_name,
                        format!("table_row_pointer_{} list", foreign_table),
                    ),
                };

                output += "  ";
                output += cname;
                output += ": ";
                output += &ctype;
                output += ";\n";
            }
        }

        output += "}";
        if with_yojson {
            output += " [@@deriving yojson]";
        }
        res.push(output);
    }
    res
}

fn table_definitions(data: &AllData, ser_vecs: &Vec<SerializationVector>) -> Vec<String> {
    let mut res = Vec::with_capacity(data.tables.len());
    for t in data.tables_sorted() {
        let mut output = String::new();
        output += &format!("type table_definition_{} = {{\n", t.name.as_str());
        output += "  length: int;\n";
        output += &format!(
            "  iter: (table_row_pointer_{} -> unit) -> unit;\n",
            t.name.as_str()
        );
        output += &format!(
            "  row: table_row_pointer_{} -> table_row_{};\n",
            t.name.as_str(),
            t.name.as_str()
        );

        for sv in ser_vecs {
            if sv.table_name() == t.name.as_str() {
                let (cname, ctype) = match sv {
                    crate::checker::types::SerializationVector::Strings(sv) => (
                        &sv.column_name,
                        format!("table_row_pointer_{} -> string", t.name.as_str()),
                    ),
                    crate::checker::types::SerializationVector::Ints(sv) => (
                        &sv.column_name,
                        format!("table_row_pointer_{} -> int", t.name.as_str()),
                    ),
                    crate::checker::types::SerializationVector::Floats(sv) => (
                        &sv.column_name,
                        format!("table_row_pointer_{} -> float", t.name.as_str()),
                    ),
                    crate::checker::types::SerializationVector::Bools(sv) => (
                        &sv.column_name,
                        format!("table_row_pointer_{} -> bool", t.name.as_str()),
                    ),
                    crate::checker::types::SerializationVector::Fkeys { sv, foreign_table } => (
                        &sv.column_name,
                        format!(
                            "table_row_pointer_{} -> table_row_pointer_{}",
                            t.name.as_str(),
                            foreign_table
                        ),
                    ),
                    crate::checker::types::SerializationVector::FkeysOneToMany {
                        sv,
                        foreign_table,
                    } => (
                        &sv.column_name,
                        format!(
                            "table_row_pointer_{} -> table_row_pointer_{} list",
                            t.name.as_str(),
                            foreign_table
                        ),
                    ),
                };

                output += "  c_";
                output += cname;
                output += ": ";
                output += &ctype;
                output += ";\n";
            }
        }

        output += "}";
        res.push(output);
    }
    res
}

fn database_definition(data: &AllData) -> String {
    let mut res = String::new();

    res += "type database = {\n";
    for t in data.tables_sorted() {
        let table_struct = format!("table_definition_{}", t.name.as_str());
        res += "  ";
        res += t.name.as_str();
        res += ": ";
        res += &table_struct;
        res += ";\n";
    }
    res += "}\n";

    res
}

fn deserialization_function(data: &AllData, vecs: &Vec<SerializationVector>) -> String {
    let mut output = String::new();

    output += "let deserialize () : database =\n";
    output += "  let buffer = data_blob in\n";
    output += "  let cursor = ref 0 in\n";
    output += "\n";

    struct ColumnVar<'a> {
        cvar: String,
        row_var: String,
        raw_column_type: String,
        column_fetch_expr: String,
        last_for_table: bool,
        table_name: &'a str,
    }

    // let mut table_cvars: Vec<(&DataTable, Vec<ColumnVar>)> = Vec::with_capacity(data.tables.len());
    let mut column_vars: Vec<ColumnVar> = Vec::new();
    for sv in vecs {
        let cv = match sv {
            crate::checker::types::SerializationVector::Strings(v) => {
                // let cvar = format!("{}_{}", t.name.as_str(), c.column_name.as_str());
                ColumnVar {
                    cvar: format!("{}_{}", v.table_name, v.column_name),
                    row_var: v.column_name.to_string(),
                    raw_column_type: "string array".to_string(),
                    column_fetch_expr: "fetch_string_vector buffer cursor".to_string(),
                    last_for_table: v.last_for_table,
                    table_name: v.table_name,
                }
            }
            crate::checker::types::SerializationVector::Ints(v) => ColumnVar {
                cvar: format!("{}_{}", v.table_name, v.column_name),
                row_var: v.column_name.to_string(),
                raw_column_type: "int array".to_string(),
                column_fetch_expr: "fetch_i64_vector buffer cursor".to_string(),
                last_for_table: v.last_for_table,
                table_name: v.table_name,
            },
            crate::checker::types::SerializationVector::Floats(v) => ColumnVar {
                cvar: format!("{}_{}", v.table_name, v.column_name),
                row_var: v.column_name.to_string(),
                raw_column_type: "float array".to_string(),
                column_fetch_expr: "fetch_f64_vector buffer cursor".to_string(),
                last_for_table: v.last_for_table,
                table_name: v.table_name,
            },
            crate::checker::types::SerializationVector::Bools(v) => ColumnVar {
                cvar: format!("{}_{}", v.table_name, v.column_name),
                row_var: v.column_name.to_string(),
                raw_column_type: "bool array".to_string(),
                column_fetch_expr: "fetch_bool_vector buffer cursor".to_string(),
                last_for_table: v.last_for_table,
                table_name: v.table_name,
            },
            crate::checker::types::SerializationVector::Fkeys { sv, foreign_table } => {
                let cvar = format!("{}_{}", sv.table_name, sv.column_name);
                let fkey_pascal = foreign_table.to_case(Case::Pascal);
                ColumnVar {
                    cvar,
                    row_var: sv.column_name.to_string(),
                    raw_column_type: format!("table_row_pointer_{} array", foreign_table),
                    column_fetch_expr: format!("fetch_i64_vector buffer cursor |> Array.map (fun ptr -> TableRowPointer{} ptr)", fkey_pascal),
                    last_for_table: sv.last_for_table,
                    table_name: sv.table_name,
                }
            }
            crate::checker::types::SerializationVector::FkeysOneToMany { sv, foreign_table } => {
                let cvar = format!("{}_{}", sv.table_name, sv.column_name);
                let fkey_pascal = foreign_table.to_case(Case::Pascal);
                ColumnVar {
                    cvar,
                    row_var: sv.column_name.to_string(),
                    raw_column_type: format!("table_row_pointer_{} list array", foreign_table),
                    column_fetch_expr: format!("fetch_i64_nested_vector buffer cursor |> Array.map (fun children -> Array.map (fun c -> TableRowPointer{} c) children |> Array.to_list)", fkey_pascal),
                    last_for_table: sv.last_for_table,
                    table_name: sv.table_name,
                }
            }
        };

        output.push_str("  ");
        output.push_str("let ");
        output.push_str(&cv.cvar);
        output.push_str(": ");
        output.push_str(&cv.raw_column_type);
        output.push_str(" = ");
        output.push_str(&cv.column_fetch_expr);
        output.push_str(" in\n");

        let last_for_table = cv.last_for_table;
        column_vars.push(cv);
        if last_for_table {
            let last_var = column_vars.last().unwrap();
            output.push('\n');

            let tlen_var = format!("{}_len", last_var.table_name);
            let tlen_expr = format!("  let {} = Array.length {} in\n", tlen_var, last_var.cvar);
            output.push_str(&tlen_expr);
            for cv in &column_vars {
                if cv.table_name == last_var.table_name && !cv.last_for_table {
                    output.push_str("  assert (");
                    output.push_str(&tlen_var);
                    output.push_str(" = Array.length ");
                    output.push_str(&cv.cvar);
                    output.push_str(");\n");
                }
            }

            output.push('\n');
        }
    }

    for t in data.tables_sorted() {
        let tname_pascal = t.name.as_str().to_case(Case::Pascal);
        // generate row ids
        let row_ids_vname = format!("row_ids_{}", t.name.as_str());
        let first_var_name = column_vars
            .iter()
            .find(|i| i.table_name == t.name.as_str())
            .unwrap();
        output.push_str("  ");
        output.push_str("let ");
        output.push_str(&row_ids_vname);
        output.push_str(": table_row_pointer_");
        output.push_str(t.name.as_str());
        output.push_str(" array = Array.mapi (fun idx _ -> TableRowPointer");
        output.push_str(&tname_pascal);
        output.push_str(" idx) ");
        output.push_str(&first_var_name.cvar);
        output.push_str(" in\n");

        // generate rows
        let rows_vname = format!("rows_{}", t.name.as_str());
        output.push_str("  ");
        output.push_str("let ");
        output.push_str(&rows_vname);
        output.push_str(": table_row_");
        output.push_str(t.name.as_str());
        output.push_str(" array = Array.map (fun (TableRowPointer");
        output.push_str(&tname_pascal);
        output.push_str(" row) -> {\n");

        for column in &column_vars {
            if column.table_name == t.name.as_str() {
                output.push_str("        ");
                output.push_str(&column.row_var);
                output.push_str(" = ");
                output.push_str(&column.cvar);
                output.push_str(".(row);\n");
            }
        }

        output.push_str("      }) ");
        output.push_str(&row_ids_vname);
        output.push_str("  in\n");

        output.push('\n');

        // generate table definition
        output.push_str("  let ");
        output.push_str(t.name.as_str());
        output.push_str(": table_definition_");
        output.push_str(t.name.as_str());
        output.push_str(" = {\n");

        // length
        output.push_str("    length = ");
        output.push_str(t.name.as_str());
        output.push_str("_len;\n");

        // iter function
        output.push_str("    iter = (fun f -> Array.iter f row_ids_");
        output.push_str(t.name.as_str());
        output.push_str(");\n");

        // get row function
        output.push_str(&format!(
            "    row = (fun (TableRowPointer{} ptr) -> rows_",
            tname_pascal
        ));
        output.push_str(t.name.as_str());
        output.push_str(".(ptr));\n");

        for column in &column_vars {
            if column.table_name == t.name.as_str() {
                output.push_str("    c_");
                output.push_str(&column.row_var);
                output.push_str(&format!(
                    " = (fun (TableRowPointer{} ptr) -> ",
                    tname_pascal
                ));
                output.push_str(&column.cvar);
                output.push_str(".(ptr));\n");
            }
        }

        output.push_str("  } in\n");
        output.push('\n');
    }

    output += "  assert (String.length buffer = !cursor);\n";
    output += "\n";

    output.push_str("  {\n");
    for t in data.tables_sorted() {
        output.push_str("    ");
        output.push_str(t.name.as_str());
        output.push_str(";\n");
    }
    output.push_str("  }\n");
    output.push('\n');

    output
}

fn debug_dump_function(data: &AllData) -> String {
    let mut output = String::new();

    output.push_str("let dump_to_stdout (db: database) : unit =\n");
    for t in data.tables_sorted() {
        let t = t.name.as_str();
        output.push_str(&format!(
            "  print_endline \"TABLE: {} <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\";\n",
            t
        ));
        output.push_str(&format!("  db.{}.iter (fun i -> db.{}.row i |> table_row_{}_to_yojson |> Yojson.Safe.to_string |> print_endline);\n", t, t, t));
    }
    output.push_str("  ()\n");

    output
}

#[cfg(test)]
fn init_dune_project(dir: &std::path::PathBuf) -> std::path::PathBuf {
    let dune_project_contents = r#"(lang dune 2.1)

(name test)

(authors "Author Name")
(maintainers "Maintainer Name")
(license LICENSE)

(package
 (name test)
 (depends ocaml dune))
"#;

    let dune_file_contents = r#"
(executable
 (public_name test)
 (name main)
 (preprocess (pps ppx_blob ppx_deriving_yojson))
 (preprocessor_deps (file edb_data.bin))
 (libraries yojson checkseum)
 (modes (native exe)))
"#;

    let main_ml_contents = r#"
let () =
  Database.dump_to_stdout Database.db
"#;
    let src_dir = dir.join("bin");
    std::fs::create_dir(&src_dir).unwrap();

    let cargo_toml = dir.join("dune-project");
    std::fs::write(cargo_toml, dune_project_contents).unwrap();
    let main_rs = src_dir.join("main.ml");
    std::fs::write(main_rs, main_ml_contents).unwrap();
    let dune_file = src_dir.join("dune");
    std::fs::write(dune_file, dune_file_contents).unwrap();

    src_dir
}

#[cfg(test)]
fn assert_ocaml_db_compiled_dump_equals(source: &str, output_dump: &str) {
    use std::process::{Command, Stdio};

    use crate::db_parser::{self, InputSource};

    let tmp_dir = crate::checker::tests::common::random_test_dir();
    let src_dir = init_dune_project(&tmp_dir);
    let inputs = &mut [InputSource {
        path: "test".to_string(),
        contents: Some(source.to_string()),
        source_dir: None,
        line_comments: Vec::new(),
    }];

    let sources = db_parser::parse_sources(inputs.as_mut_slice()).unwrap();
    let data = AllData::new(sources).unwrap();

    let mut gen = OCamlCodegen::default();
    gen.debug_dump_function = true;
    let codegen_outputs = gen.generate(&data);
    codegen_outputs.dump_to_dir(src_dir.to_str().unwrap());

    let output = Command::new("dune")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .current_dir(&tmp_dir)
        .arg("exec")
        .arg("test")
        .output()
        .unwrap();

    assert!(output.status.success());

    let out_res = String::from_utf8(output.stdout).unwrap();
    pretty_assertions::assert_eq!(out_res, output_dump);
}

#[test]
#[ignore]
fn test_ocaml_codegen_integration() {
    let source = r#"
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

DATA enum_child_a(name, inner_name_a) {
  warm, barely warm;
  warm, medium warm;
}

DATA enum_child_b(name, inner_name_b) {
  warm, barely degrees;
  warm, medium degrees;
}

DATA EXCLUSIVE some_enum {
  warm;
  hot;
}
"#;
    let output_dump = r#"TABLE: enum_child_a <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
{"inner_name_a":"barely warm","parent":["TableRowPointerSomeEnum",0]}
{"inner_name_a":"medium warm","parent":["TableRowPointerSomeEnum",0]}
TABLE: enum_child_b <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
{"inner_name_b":"barely degrees","parent":["TableRowPointerSomeEnum",0]}
{"inner_name_b":"medium degrees","parent":["TableRowPointerSomeEnum",0]}
TABLE: some_enum <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
{"name":"warm","children_enum_child_a":[["TableRowPointerEnumChildA",0],["TableRowPointerEnumChildA",1]],"children_enum_child_b":[["TableRowPointerEnumChildB",0],["TableRowPointerEnumChildB",1]],"referrers_thic_boi__fk":[["TableRowPointerThicBoi",0]]}
{"name":"hot","children_enum_child_a":[],"children_enum_child_b":[],"referrers_thic_boi__fk":[["TableRowPointerThicBoi",1],["TableRowPointerThicBoi",2]]}
TABLE: thic_boi <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
{"id":1,"name":"hey ho","b":true,"f":1.23,"fk":["TableRowPointerSomeEnum",0]}
{"id":2,"name":"here she goes","b":false,"f":3.21,"fk":["TableRowPointerSomeEnum",1]}
{"id":3,"name":"either blah","b":true,"f":5.43,"fk":["TableRowPointerSomeEnum",1]}
"#;

    assert_ocaml_db_compiled_dump_equals(source, output_dump);
}

#[cfg(test)]
use std::{process::{Command, Stdio}};

use convert_case::{Casing, Case};

#[cfg(test)]
use crate::db_parser::{self, InputSource};

use crate::checker::{logic::AllData, types::SerializationVector};

use super::{CodeGenerator, CodegenOutputFile};


pub struct RustCodegen {
    pub debug_dump_function: bool,
    pub edb_data_file_name: String,
    pub db_source_file_name: String,
    // for testing, undocumented
    pub expose_deserialization_function: bool,
}

impl Default for RustCodegen {
    fn default() -> Self {
        Self {
            debug_dump_function: false,
            edb_data_file_name: "edb_data.bin".to_string(),
            db_source_file_name: "database.rs".to_string(),
            expose_deserialization_function: false,
        }
    }
}

impl CodeGenerator for RustCodegen {
    fn generate(&self, data: &crate::checker::logic::AllData) -> super::CodegenOutputs {
        let mut content = String::new();
        let comp = RustCodegenCompute::new(data, &self);

        content += r#"// Test db content
const DB_BYTES: &[u8] = include_bytes!("edb_data.bin");
lazy_static!{
    pub static ref DB: Database = Database::deserialize_compressed(DB_BYTES).unwrap();
}
"#;
        content += "\n";

        content += "// Table row pointer types\n";
        for trow_pointer in &comp.table_pointer_types {
            content += trow_pointer;
            content += "\n";
            content += "\n";
        }
        content += "\n";

        content += "// Table struct types\n";
        for tstruct in &comp.table_structs {
            content += tstruct;
            content += "\n";
            content += "\n";
        }
        content += "\n";

        content += "// Table definitions\n";
        for tdef in &comp.table_definitions {
            content += tdef;
            content += "\n";
            content += "\n";
        }
        content += "\n";

        content += "// Database definition\n";
        content += &comp.database_definition;
        content += "\n";

        content += "// Database implementation\n";
        content += &comp.database_impl;
        content += "\n";

        content += "// Table definition implementations\n";
        for tdef in &comp.table_definition_impls {
            content += tdef;
            content += "\n";
            content += "\n";
        }

        let output_src = CodegenOutputFile {
            filename: self.db_source_file_name.clone(),
            content: content.into_bytes(),
        };

        let data_src = CodegenOutputFile {
            filename: self.edb_data_file_name.clone(),
            content: comp.data_bytes.clone(),
        };

        super::CodegenOutputs {
            uncompressed_edb_data: comp.uncompressed_data_bytes,
            files: vec![
                output_src,
                data_src,
            ],
        }
    }
}

struct RustCodegenCompute {
    table_pointer_types: Vec<String>,
    table_structs: Vec<String>,
    table_definitions: Vec<String>,
    table_definition_impls: Vec<String>,
    database_definition: String,
    database_impl: String,
    data_bytes: Vec<u8>,
    uncompressed_data_bytes: Vec<u8>,
}

impl RustCodegenCompute {
    fn new(data: &AllData, opt: &RustCodegen) -> RustCodegenCompute {
        let vecs = data.serialization_vectors();
        let table_pointer_types = table_pointer_types(data);
        let table_structs = table_structs(data, &vecs);
        let table_definitions = table_definitions(data, &vecs);
        let database_definition = database_definition(data);
        let database_impl = database_impl(data, opt, &vecs);
        let table_definition_impls = table_definition_impls(data, &vecs);
        let (data_bytes, uncompressed_data_bytes) = super::dump_as_bytes_lz4_checksum_xxh(&vecs);
        RustCodegenCompute {
            table_pointer_types,
            table_structs,
            table_definitions,
            table_definition_impls,
            database_definition,
            database_impl,
            data_bytes,
            uncompressed_data_bytes,
        }
    }
}

fn table_pointer_types(data: &AllData) -> Vec<String> {
    let mut res = Vec::with_capacity(data.tables.len());
    for t in data.tables_sorted() {
        let tname_pasc_case = t.name.as_str().to_case(Case::Pascal);
        let mut output = String::new();
        output += "#[derive(Copy, Clone, Debug, serde::Deserialize, Eq, PartialEq, ::std::hash::Hash)]\n";
        output += &format!("pub struct TableRowPointer{}(usize);", tname_pasc_case);
        res.push(output);
    }
    res
}

fn table_structs(data: &AllData, vecs: &Vec<SerializationVector>) -> Vec<String> {
    let mut res = Vec::with_capacity(data.tables.len());
    for t in data.tables_sorted() {
        let tname_pasc_case = t.name.as_str().to_case(Case::Pascal);
        let mut output = String::new();
        output += "#[derive(Debug)]\n";
        output += &format!("pub struct TableRow{} {{\n", tname_pasc_case);

        for sv in vecs {
            if sv.table_name() == t.name.as_str() {
                let (cname, ctype) = match sv {
                    SerializationVector::StringsColumn(sv) => {
                        (&sv.column_name, "::std::string::String".to_string())
                    },
                    SerializationVector::IntsColumn(sv) => {
                        (&sv.column_name, "i64".to_string())
                    },
                    SerializationVector::FloatsColumn(sv) => {
                        (&sv.column_name, "f64".to_string())
                    },
                    SerializationVector::BoolsColumn(sv) => {
                        (&sv.column_name, "bool".to_string())
                    },
                    SerializationVector::FkeysColumn { sv, foreign_table } => {
                        (&sv.column_name, format!("TableRowPointer{}", foreign_table.as_str().to_case(Case::Pascal)))
                    },
                    SerializationVector::FkeysOneToManyColumn { sv, foreign_table } => {
                        (&sv.column_name, format!("Vec<TableRowPointer{}>", foreign_table.as_str().to_case(Case::Pascal)))
                    },
                };

                output += "    pub ";
                output += cname;
                output += ": ";
                output += &ctype;
                output += ",\n";
            }
        }

        output += "}";
        res.push(output);
    }
    res
}

fn table_definitions(data: &AllData, vecs: &Vec<SerializationVector>) -> Vec<String> {
    let mut res = Vec::with_capacity(data.tables.len());
    for t in data.tables_sorted() {
        let tname_pasc_case = t.name.as_str().to_case(Case::Pascal);
        let mut output = String::new();
        output += &format!("pub struct TableDefinition{} {{\n", tname_pasc_case);
        output += &format!("    rows: Vec<TableRow{}>,\n", tname_pasc_case);

        for sv in vecs {
            if sv.table_name() == t.name.as_str() {
                let (cname, ctype) = match sv {
                    SerializationVector::StringsColumn(sv) => {
                        (&sv.column_name, "Vec<::std::string::String>".to_string())
                    },
                    SerializationVector::IntsColumn(sv) => {
                        (&sv.column_name, "Vec<i64>".to_string())
                    },
                    SerializationVector::FloatsColumn(sv) => {
                        (&sv.column_name, "Vec<f64>".to_string())
                    },
                    SerializationVector::BoolsColumn(sv) => {
                        (&sv.column_name, "Vec<bool>".to_string())
                    },
                    SerializationVector::FkeysColumn { sv, foreign_table } => {
                        (&sv.column_name, format!("Vec<TableRowPointer{}>", foreign_table.as_str().to_case(Case::Pascal)))
                    },
                    SerializationVector::FkeysOneToManyColumn { sv, foreign_table } => {
                        (&sv.column_name, format!("Vec<Vec<TableRowPointer{}>>", foreign_table.as_str().to_case(Case::Pascal)))
                    },
                };

                output += "    c_";
                output += cname;
                output += ": ";
                output += &ctype;
                output += ",\n";
            }
        }

        output += "}";
        res.push(output);
    }
    res
}

fn database_definition(data: &AllData) -> String {
    let mut res = String::new();

    res += "pub struct Database {\n";
    for t in data.tables_sorted() {
        let tname_pasc_case = t.name.as_str().to_case(Case::Pascal);
        let table_struct = format!("TableDefinition{}", tname_pasc_case);
        res += "    ";
        res += t.name.as_str();
        res += ": ";
        res += &table_struct;
        res += ",\n";
    }
    res += "}\n";

    res
}

fn database_impl(data: &AllData, opt: &RustCodegen, vecs: &Vec<SerializationVector>) -> String {
    let mut res = String::new();

    res += "impl Database {\n";

    // database accessors
    for t in data.tables_sorted() {
        let tname_pasc_case = t.name.as_str().to_case(Case::Pascal);
        let table_struct = format!("TableDefinition{}", tname_pasc_case);
        res += "    pub fn ";
        res += t.name.as_str();
        res += "(&self) -> &";
        res += &table_struct;
        res += " {\n";
        res += "        &self.";
        res += t.name.as_str();
        res += "\n";
        res += "    }\n";
        res += "\n";
    }

    // database deserialization function
    database_deserialization_function(&mut res, data, vecs, opt.expose_deserialization_function);
    if opt.debug_dump_function {
        database_dump_function(&mut res, data);
    }

    res += "}\n";

    res
}

fn database_dump_function(output: &mut String, data: &AllData) {
    output.push_str("    pub fn debug_dump_stdout(&self) {\n");
    for t in data.tables_sorted() {
        output.push_str("        ");
        output.push_str("println!(\"TABLE: ");
        output.push_str(t.name.as_str());
        output.push_str(" <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\");\n");

        output.push_str("        ");
        output.push_str("for row in self.");
        output.push_str(t.name.as_str());
        output.push_str(".rows_iter() {\n");
        output.push_str("            println!(\"{:?}\", self.");
        output.push_str(t.name.as_str());
        output.push_str("().row(row));\n");
        output.push_str("        }\n");
    }
    output.push_str("    }\n");
    output.push_str("\n");
}

fn database_deserialization_function(output: &mut String, data: &AllData, vecs: &Vec<SerializationVector>, expose: bool) {
    output.push_str("    fn deserialize_compressed(compressed: &[u8]) -> Result<Database, Box<dyn ::std::error::Error>> {\n");
    output.push_str("        let hash_size = ::std::mem::size_of::<u64>();\n");
    output.push_str("        assert!(compressed.len() > hash_size);\n");
    output.push_str("        let compressed_end = compressed.len() - hash_size;\n");
    output.push_str("        let compressed_slice = &compressed[0..compressed_end];\n");
    output.push_str("        let hash_slice = &compressed[compressed_end..];\n");
    output.push_str("        let encoded_hash = ::bincode::deserialize::<u64>(hash_slice).unwrap();\n");
    output.push_str("        let computed_hash = ::xxhash_rust::xxh3::xxh3_64(compressed_slice);\n");
    output.push_str("        if encoded_hash != computed_hash { panic!(\"EdenDB data is corrupted, checksum mismatch.\") }\n");
    output.push_str("        let input = ::lz4_flex::decompress_size_prepended(compressed_slice).unwrap();\n");
    output.push_str("        Self::deserialize(input.as_slice())\n");
    output.push_str("    }\n");
    output.push_str("\n");
    output.push_str("    ");
    if expose { output.push_str("pub ") };
    output.push_str("fn deserialize(input: &[u8]) -> Result<Database, Box<dyn ::std::error::Error>> {\n");
    output.push_str("        let mut cursor = ::std::io::Cursor::new(input);\n");
    output.push_str("\n");

    struct ColumnVar<'a> {
        cvar: String,
        row_var: String,
        raw_column_type: String,
        last_for_table: bool,
        table_name: &'a str,
        should_clone: bool,
    }

    let mut column_vars: Vec<ColumnVar> = Vec::new();
    for sv in vecs {
        let cv = match sv {
            crate::checker::types::SerializationVector::StringsColumn(v) => {
                // let cvar = format!("{}_{}", t.name.as_str(), c.column_name.as_str());
                ColumnVar {
                    cvar: format!("{}_{}", v.table_name, v.column_name),
                    row_var: v.column_name.to_string(),
                    raw_column_type: "Vec<::std::string::String>".to_string(),
                    last_for_table: v.last_for_table,
                    table_name: v.table_name,
                    should_clone: true,
                }
            },
            crate::checker::types::SerializationVector::IntsColumn(v) => {
                ColumnVar {
                    cvar: format!("{}_{}", v.table_name, v.column_name),
                    row_var: v.column_name.to_string(),
                    raw_column_type: "Vec<i64>".to_string(),
                    last_for_table: v.last_for_table,
                    table_name: v.table_name,
                    should_clone: false,
                }
            },
            crate::checker::types::SerializationVector::FloatsColumn(v) => {
                ColumnVar {
                    cvar: format!("{}_{}", v.table_name, v.column_name),
                    row_var: v.column_name.to_string(),
                    raw_column_type: "Vec<f64>".to_string(),
                    last_for_table: v.last_for_table,
                    table_name: v.table_name,
                    should_clone: false,
                }
            },
            crate::checker::types::SerializationVector::BoolsColumn(v) => {
                ColumnVar {
                    cvar: format!("{}_{}", v.table_name, v.column_name),
                    row_var: v.column_name.to_string(),
                    raw_column_type: "Vec<bool>".to_string(),
                    last_for_table: v.last_for_table,
                    table_name: v.table_name,
                    should_clone: false,
                }
            },
            crate::checker::types::SerializationVector::FkeysColumn { sv, foreign_table } => {
                let cvar = format!("{}_{}", sv.table_name, sv.column_name);
                let fkey_pascal = foreign_table.to_case(Case::Pascal);
                ColumnVar {
                    cvar,
                    row_var: sv.column_name.to_string(),
                    raw_column_type: format!("Vec<TableRowPointer{}>", fkey_pascal),
                    last_for_table: sv.last_for_table,
                    table_name: sv.table_name,
                    should_clone: false,
                }
            },
            crate::checker::types::SerializationVector::FkeysOneToManyColumn { sv, foreign_table } => {
                let cvar = format!("{}_{}", sv.table_name, sv.column_name);
                let fkey_pascal = foreign_table.to_case(Case::Pascal);
                ColumnVar {
                    cvar,
                    row_var: sv.column_name.to_string(),
                    raw_column_type: format!("Vec<Vec<TableRowPointer{}>>", fkey_pascal),
                    last_for_table: sv.last_for_table,
                    table_name: sv.table_name,
                    should_clone: true,
                }
            },
        };

        output.push_str("        ");
        output.push_str("let ");
        output.push_str(&cv.cvar);
        output.push_str(": ");
        output.push_str(&cv.raw_column_type);
        output.push_str(" = ::bincode::deserialize_from(&mut cursor)?;\n");
        let last_for_table = cv.last_for_table;
        column_vars.push(cv);

        if last_for_table {
            let last_var = column_vars.last().unwrap();
            output.push_str("\n");

            let tlen_var = format!("{}_len", last_var.table_name);
            // table length var
            output.push_str("        ");
            output.push_str("let ");
            output.push_str(&tlen_var);
            output.push_str(" = ");
            output.push_str(&last_var.cvar);
            output.push_str(".len();\n");
            output.push_str("\n");


            for i in &column_vars {
                if i.table_name == last_var.table_name && last_var.cvar != i.cvar {
                    output.push_str("        ");
                    output.push_str("assert_eq!(");
                    output.push_str(&tlen_var);
                    output.push_str(", ");
                    output.push_str(&i.cvar);
                    output.push_str(".len());\n");
                }
            }

            output.push_str("\n");

            // generate rows
            let trow_pascal = last_var.table_name.to_case(Case::Pascal);
            let rows_vname = format!("rows_{}", last_var.table_name);
            output.push_str("        ");
            output.push_str("let mut ");
            output.push_str(&rows_vname);
            output.push_str(": Vec<TableRow");
            output.push_str(&trow_pascal);
            output.push_str("> = Vec::with_capacity(");
            output.push_str(&tlen_var);
            output.push_str(");\n");

            output.push_str("        ");
            output.push_str("for row in 0..");
            output.push_str(&tlen_var);
            output.push_str(" {\n");
            output.push_str("            ");
            output.push_str(&rows_vname);
            output.push_str(".push(TableRow");
            output.push_str(&trow_pascal);
            output.push_str(" {\n");

            for column in &column_vars {
                if column.table_name == last_var.table_name {
                    output.push_str("                ");
                    output.push_str(&column.row_var);
                    output.push_str(": ");
                    output.push_str(&column.cvar);
                    output.push_str("[row]");
                    if column.should_clone {
                        output.push_str(".clone()")
                    }
                    // clone if string
                    output.push_str(",\n");
                }
            }

            output.push_str("            });\n");
            output.push_str("        }\n");

            output.push_str("\n");
        }
    }

    output.push_str("\n");
    output.push_str("        assert_eq!(cursor.position() as usize, input.len());\n");
    output.push_str("\n");
    output.push_str("        Ok(Database {\n");

    for t in data.tables_sorted() {
        let tname_pascal = t.name.as_str().to_case(Case::Pascal);
        output.push_str("            ");
        output.push_str(t.name.as_str());
        output.push_str(": TableDefinition");
        output.push_str(&tname_pascal);
        output.push_str(" {\n");

        output.push_str("                ");
        output.push_str("rows: rows_");
        output.push_str(t.name.as_str());
        output.push_str(",\n");

        for v in &column_vars {
            if v.table_name == t.name.as_str() {
                output.push_str("                ");
                output.push_str("c_");
                output.push_str(&v.row_var);
                output.push_str(": ");
                output.push_str(&v.cvar);
                output.push_str(",\n");
            }
        }

        output.push_str("            },\n");
    }

    output.push_str("        })\n");

    output.push_str("    }\n");

}

fn table_definition_impls(data: &AllData, vecs: &Vec<SerializationVector>) -> Vec<String> {
    let mut res = Vec::with_capacity(data.tables.len());
    for t in data.tables_sorted() {
        let tname_pasc_case = t.name.as_str().to_case(Case::Pascal);
        let mut output = String::new();
        let trow_def = format!("TableRow{}", tname_pasc_case);
        let trow_ptr = format!("TableRowPointer{}", tname_pasc_case);

        output += &format!("impl TableDefinition{} {{\n", tname_pasc_case);

        // Example: table len
        // pub fn len(&self) -> usize {
        //     self.rows.len()
        // }
        output += "    pub fn len(&self) -> usize {\n";
        output += "        self.rows.len()\n";
        output += "    }\n";
        output += "\n";

        // Example: rows iterator
        // pub fn rows_iter(&self) -> impl std::iter::Iterator<Item = TableRowPointerThicBoi> {
        //     (0..self.rows.len()).map(|idx| {
        //         TableRowPointerThicBoi(idx)
        //     })
        // }

        output += "    pub fn rows_iter(&self) -> impl ::std::iter::Iterator<Item = ";
        output += &trow_ptr;
        output += "> {\n";

        output += "        (0..self.rows.len()).map(|idx| {\n";
        output += "            "; output += &trow_ptr; output += "(idx)\n";
        output += "        })\n";
        output += "    }\n";
        output += "\n";

        // Example: get full row reference by pointer
        // pub fn row(&self, ptr: TableRowPointerThicBoi) -> &TableRowThicBoi {
        //     &self.rows[ptr.0]
        // }

        output += "    pub fn row(&self, ptr: "; output += &trow_ptr; output += ") -> &"; output += &trow_def; output += " {\n";
        output += "        &self.rows[ptr.0]\n";
        output += "    }\n";
        output += "\n";


        struct ColumnVar {
            row_var: String,
            raw_column_type: String,
            return_ref: bool,
        }

        for sv in vecs {
            if sv.table_name() == t.name.as_str() {
                let cv = match sv {
                    crate::checker::types::SerializationVector::StringsColumn(v) => {
                        ColumnVar {
                            row_var: v.column_name.to_string(),
                            raw_column_type: "&::std::string::String".to_string(),
                            return_ref: true,
                        }
                    },
                    crate::checker::types::SerializationVector::IntsColumn(v) => {
                        ColumnVar {
                            row_var: v.column_name.to_string(),
                            raw_column_type: "i64".to_string(),
                            return_ref: false,
                        }
                    },
                    crate::checker::types::SerializationVector::FloatsColumn(v) => {
                        ColumnVar {
                            row_var: v.column_name.to_string(),
                            raw_column_type: "f64".to_string(),
                            return_ref: false,
                        }
                    },
                    crate::checker::types::SerializationVector::BoolsColumn(v) => {
                        ColumnVar {
                            row_var: v.column_name.to_string(),
                            raw_column_type: "bool".to_string(),
                            return_ref: false,
                        }
                    },
                    crate::checker::types::SerializationVector::FkeysColumn { sv, foreign_table } => {
                        let fkey_pascal = foreign_table.to_case(Case::Pascal);
                        ColumnVar {
                            row_var: sv.column_name.to_string(),
                            raw_column_type: format!("TableRowPointer{}", fkey_pascal),
                            return_ref: false,
                        }
                    },
                    crate::checker::types::SerializationVector::FkeysOneToManyColumn { sv, foreign_table } => {
                        let fkey_pascal = foreign_table.to_case(Case::Pascal);
                        ColumnVar {
                            row_var: sv.column_name.to_string(),
                            raw_column_type: format!("&[TableRowPointer{}]", fkey_pascal),
                            return_ref: true,
                        }
                    },
                };

                output += "    pub fn c_";
                output += &cv.row_var;
                output += "(&self, ptr: ";
                output += &trow_ptr;
                output += ") -> ";
                output += &cv.raw_column_type;
                output += " {\n";
                output += "        ";
                if cv.return_ref { output += "&" }
                output += "self.c_";
                output += cv.row_var.as_str();
                output += "[ptr.0]\n";
                output += "    }\n";
                output += "\n";
            }
        }

        output += "}";
        res.push(output);
    }
    res
}

#[cfg(test)]
fn init_cargo_project(dir: &std::path::PathBuf) -> std::path::PathBuf {
    let cargo_toml_contents = r#"
[package]
name = "tst"
version = "0.1.0"
edition = "2021"

# See more keys and their definitions at https://doc.rust-lang.org/cargo/reference/manifest.html

[dependencies]
bincode = "1.3.3"
serde = { version = "1.0.140", features = ["derive"] }
lazy_static = "1.4.0"
lz4_flex = { version = "0.9.3", default-features = false, features = ["checked-decode"] }
xxhash-rust = { version = "0.8.5", features = ["xxh3"] }
"#;
    let main_rs_contents = r#"
#[macro_use]
extern crate lazy_static;

mod database;

fn main() {
    database::DB.debug_dump_stdout();
}
"#;
    let src_dir = dir.join("src");
    std::fs::create_dir(&src_dir).unwrap();
    let cargo_toml = dir.join("Cargo.toml");
    std::fs::write(&cargo_toml, cargo_toml_contents).unwrap();
    let main_rs = src_dir.join("main.rs");
    std::fs::write(&main_rs, main_rs_contents).unwrap();

    src_dir
}

#[cfg(test)]
fn assert_rust_db_compiled_dump_equals(source: &str, output_dump: &str) {
    let tmp_dir = crate::checker::tests::common::random_test_dir();
    let src_dir = init_cargo_project(&tmp_dir);
    let inputs = &mut [InputSource {
        path: "test".to_string(),
        contents: Some(source.to_string()),
        source_dir: None,
    }];

    let sources = db_parser::parse_sources(inputs.as_mut_slice()).unwrap();
    let data = AllData::new(sources).unwrap();

    let mut gen = RustCodegen::default();
    gen.debug_dump_function = true;
    let codegen_outputs = gen.generate(&data);
    codegen_outputs.dump_to_dir(src_dir.to_str().unwrap());

    let output =
        Command::new("cargo")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .current_dir(&tmp_dir)
            .arg("run")
            .output()
            .unwrap();

    assert!(output.status.success());


    let out_res = String::from_utf8(output.stdout.clone()).unwrap();
    pretty_assertions::assert_eq!(out_res, output_dump);
}

#[test]
#[ignore]
fn test_rust_codegen_integration() {
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
    let output_dump =
r#"TABLE: enum_child_a <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
TableRowEnumChildA { inner_name_a: "barely warm", parent: TableRowPointerSomeEnum(0) }
TableRowEnumChildA { inner_name_a: "medium warm", parent: TableRowPointerSomeEnum(0) }
TABLE: enum_child_b <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
TableRowEnumChildB { inner_name_b: "barely degrees", parent: TableRowPointerSomeEnum(0) }
TableRowEnumChildB { inner_name_b: "medium degrees", parent: TableRowPointerSomeEnum(0) }
TABLE: some_enum <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
TableRowSomeEnum { name: "warm", children_enum_child_a: [TableRowPointerEnumChildA(0), TableRowPointerEnumChildA(1)], children_enum_child_b: [TableRowPointerEnumChildB(0), TableRowPointerEnumChildB(1)], referrers_thic_boi__fk: [TableRowPointerThicBoi(0)] }
TableRowSomeEnum { name: "hot", children_enum_child_a: [], children_enum_child_b: [], referrers_thic_boi__fk: [TableRowPointerThicBoi(1), TableRowPointerThicBoi(2)] }
TABLE: thic_boi <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
TableRowThicBoi { id: 1, name: "hey ho", b: true, f: 1.23, fk: TableRowPointerSomeEnum(0) }
TableRowThicBoi { id: 2, name: "here she goes", b: false, f: 3.21, fk: TableRowPointerSomeEnum(1) }
TableRowThicBoi { id: 3, name: "either blah", b: true, f: 5.43, fk: TableRowPointerSomeEnum(1) }
"#;

    assert_rust_db_compiled_dump_equals(source, output_dump);
}
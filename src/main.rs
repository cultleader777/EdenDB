use std::{mem::size_of, path::PathBuf};

use checker::logic::AllData;
use codegen::CodeGenerator;
use db_parser::InputSource;

#[macro_use]
extern crate lazy_static;

mod checker;
mod cli;
mod codegen;
mod db_parser;

fn main() {
    // we serialize/deserialize usize, 64 bit platform assumed
    assert_eq!(8, size_of::<usize>());

    let args = cli::get_args();

    let mut inputs: Vec<_> = args
        .inputs
        .iter()
        .map(|i| InputSource {
            path: i.clone(),
            contents: None,
            source_dir: None,
            line_comments: Vec::new(),
        })
        .collect();

    let sources = db_parser::parse_sources_with_external(&mut inputs[0..1]);
    if let Err(e) = sources.as_ref() {
        eprint!("{e}");
        std::process::exit(1);
    }
    let mut sources = sources.unwrap();

    if let Some(source_dump_file) = &args.dump_source_file {
        match db_parser::serialize_source_outputs(&sources) {
            Ok(bytes) => {
                crate::codegen::write_file_check_if_different(&PathBuf::from(source_dump_file), &bytes);
            }
            Err(err) => {
                eprint!("{err}");
                std::process::exit(1);
            }
        }
    }

    let rest_inputs = &mut inputs[1..];
    if let Err(err) = sources.parse_into_external(rest_inputs) {
        eprint!("{err}");
        std::process::exit(1);
    }

    if let Some(replacements_file) = &args.replacements_file {
        match std::fs::read(&replacements_file) {
            Ok(file) => {
                let replacements: db_parser::Replacements = serde_json::from_slice(&file).expect("Cannot parse replacements file");
                sources.set_value_replacements(replacements);
            }
            Err(e) => {
                eprintln!("Cannot read replacements json file at {}: {}", replacements_file, e);
                std::process::exit(1);
            }
        }
    }

    let sqlite_needed = args.sqlite_output_file.is_some();

    let data = AllData::new_with_flags(sources, sqlite_needed);
    if let Err(e) = data.as_ref() {
        err_print("validation error", &e);
        std::process::exit(1);
    }
    let data = data.unwrap();

    if let Some(rt) = &args.rust_output_directory {
        let cgen = codegen::rust::RustCodegen {
            expose_deserialization_function: std::env::var("EDB_EXPOSE_DESER").is_ok(),
            ..Default::default()
        };
        let gen_src = cgen.generate(&data);
        gen_src.dump_to_dir(rt.as_str());
    }

    if let Some(oc) = &args.ocaml_output_directory {
        let cgen = codegen::ocaml::OCamlCodegen::default();
        let gen_src = cgen.generate(&data);
        gen_src.dump_to_dir(oc.as_str());
    }

    if let Some(sqlite) = &args.sqlite_output_file {
        let db = data.sqlite_db.ro.lock().unwrap();
        let mut backup = rusqlite::Connection::open(sqlite).unwrap();
        let b = rusqlite::backup::Backup::new(&db, &mut backup).unwrap();
        b.run_to_completion(9999999, std::time::Duration::from_secs(0), None)
            .unwrap();
    }
}

fn err_print(prefix: &'static str, e: &dyn std::error::Error) {
    let out = format!("{}: {:#?}", prefix, e);
    let repl = out.replace("\\n", "\n").replace("\\\"", "\"");
    eprintln!("{}", repl);
}

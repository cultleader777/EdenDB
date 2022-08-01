use std::mem::size_of;

use checker::logic::AllData;
use codegen::CodeGenerator;
use db_parser::InputSource;

#[macro_use]
extern crate lazy_static;

mod db_parser;
mod checker;
mod cli;
mod codegen;

fn main() {
    // we serialize/deserialize usize, 64 bit platform assumed
    assert_eq!(8, size_of::<usize>());

    let args = cli::get_args();

    let mut inputs: Vec<_> = args.inputs
        .iter()
        .map(|i| { InputSource { path: i.clone(), contents: None, source_dir: None } })
        .collect();

    let sources = db_parser::parse_sources_with_external(inputs.as_mut_slice());
    if let Err(e) = sources.as_ref() {
        err_print("syntax parsing error", e.as_ref());
        std::process::exit(1);
    }
    let sources = sources.unwrap();

    let data = AllData::new(sources);
    if let Err(e) = data.as_ref() {
        err_print("validation error", &e);
        std::process::exit(1);
    }
    let data = data.unwrap();

    for rt in &args.rust_output_directory {
        let cgen = codegen::rust::RustCodegen::default();
        let gen_src = cgen.generate(&data);
        gen_src.dump_to_dir(rt.as_str());
    }

    for oc in &args.ocaml_output_directory {
        let cgen = codegen::ocaml::OCamlCodegen::default();
        let gen_src = cgen.generate(&data);
        gen_src.dump_to_dir(oc.as_str());
    }
}

fn err_print(prefix: &'static str, e: &dyn std::error::Error) {
    let out = format!("{}: {:#?}", prefix, e);
    let repl = out.replace("\\n", "\n").replace("\\\"", "\"");
    eprintln!("{}", repl);
}
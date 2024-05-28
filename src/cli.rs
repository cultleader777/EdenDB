use clap::Parser;

#[derive(Parser)] // requires `derive` feature
#[clap(author, version, about, long_about = None)]
pub struct Cli {
    /// Rust output directory
    #[clap(long)]
    pub rust_output_directory: Option<String>,

    /// OCaml output directory
    #[clap(long)]
    pub ocaml_output_directory: Option<String>,

    /// Sqlite dump output file
    #[clap(long)]
    pub sqlite_output_file: Option<String>,

    /// Replacements json file to replace data in sources
    #[clap(long)]
    pub replacements_file: Option<String>,

    /// Input sources to compile and check
    #[clap(required = true, min_values(1))]
    pub inputs: Vec<String>,

    /// Dump source serialization to file after first file parse
    /// You will not get why this is needed, this is specific feature for Eden platform
    #[clap(long)]
    pub dump_source_file: Option<String>,
}

pub fn get_args() -> Cli {
    Cli::parse()
}

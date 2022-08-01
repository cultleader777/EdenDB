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

    /// Input sources to compile and check
    #[clap(required = true, min_values(1))]
    pub inputs: Vec<String>,
}

pub fn get_args() -> Cli {
    Cli::parse()
}
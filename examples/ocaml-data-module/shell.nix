let
  # Pinned nixpkgs, deterministic. Last updated: 2/12/21.
  pkgs = import (fetchTarball("https://github.com/NixOS/nixpkgs/archive/ce6aa13369b667ac2542593170993504932eb836.tar.gz")) {};

in pkgs.mkShell {
  buildInputs = [
    pkgs.cargo
    pkgs.rustc
    pkgs.rustfmt
    pkgs.luajit
    pkgs.binutils

    pkgs.ocaml
    pkgs.ocamlPackages.findlib
    pkgs.ocamlPackages.core
    pkgs.ocamlPackages.utop
    pkgs.ocamlPackages.ocaml_sqlite3
    pkgs.ocamlPackages.data-encoding
    pkgs.ocamlPackages.ppx_blob
    pkgs.ocamlPackages.yojson
    pkgs.ocamlPackages.ppx_deriving_yojson
    pkgs.ocamlPackages.checkseum
  ];

  # Certain Rust tools won't work without this
  # This can also be fixed by using oxalica/rust-overlay and specifying the rust-src extension
  # See https://discourse.nixos.org/t/rust-src-not-found-and-other-misadventures-of-developing-rust-on-nixos/11570/3?u=samuela. for more details.
  RUST_SRC_PATH = "${pkgs.rust.packages.stable.rustPlatform.rustLibSrc}";
}

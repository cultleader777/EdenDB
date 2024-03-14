let
  # Nixpkgs version 23.11, includes promtool (23.05 doesn't)
  pkgs = import (fetchTarball("https://github.com/NixOS/nixpkgs/archive/057f9aecfb71c4437d2b27d3323df7f93c010b7e.tar.gz")) {};

in pkgs.mkShell {
  buildInputs = with pkgs; [
    ocaml
    dune_3
    ocamlPackages.findlib
    ocamlPackages.merlin
    ocamlPackages.ocp-indent
    ocamlPackages.ocaml-lsp
    ocamlPackages.utop
    ocamlPackages.ocamlformat
    ocamlPackages.yojson
    ocamlPackages.ppx_deriving_yojson
  ];
}

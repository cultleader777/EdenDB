let
  # Pinned nixpkgs, deterministic. Last updated: 2/12/21.
  pkgs = import (fetchTarball("https://github.com/NixOS/nixpkgs/archive/ce6aa13369b667ac2542593170993504932eb836.tar.gz")) {};

in pkgs.mkShell {
  buildInputs = [
    pkgs.binutils

    pkgs.ocaml
    pkgs.ocamlPackages.findlib
    pkgs.ocamlPackages.ppx_blob
    pkgs.ocamlPackages.yojson
    pkgs.ocamlPackages.ppx_deriving_yojson
    pkgs.ocamlPackages.checkseum
  ];
}

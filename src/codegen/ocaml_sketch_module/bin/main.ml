open! Database
open Sketch_db

let () =
  (* deserialization speed 812MB/s when removing assertions *)
  Sketch_db.db.thic_boi.iter (fun tb ->
      Printf.printf "%s\n" (db.thic_boi.c_name tb)
    );
  Printf.printf "%d\n" Sketch_db.db.thic_boi.len

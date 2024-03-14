open Context
open Db_types

let define_data () =
  (* define data in tables here *)
  (* def_server (mk_server ~hostname:"foo" ~ram_mb:777); *)
  def_server (mk_server ~hostname:"dookie" ~ram_mb:1024 ());
  ()

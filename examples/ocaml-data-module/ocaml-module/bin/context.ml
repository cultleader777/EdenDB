
type 'a growing_array = {
    mutable arr: 'a array array;
    mutable level: int;
    mutable level_offset: int;
    mutable capacity: int;
    mutable size: int;
    default_value: 'a;
}

let grarr_initial_level = 16

let grarr_max_levels = 32

let compute_level_offset level =
    16 lsl (level - 1)

let create ~default_value =
    let empty_arr = [||] in
    let l1_arr = Array.make grarr_max_levels empty_arr in
    l1_arr.(0) <- Array.make grarr_initial_level default_value;
    let initial_level = 0 in
    {
        arr = l1_arr;
        level = initial_level;
        level_offset = compute_level_offset initial_level;
        capacity = compute_level_offset (initial_level + 1);
        size = 0;
        default_value = default_value;
    }

let append (arr: 'a growing_array) (to_push: 'a) =
    if arr.size < arr.capacity then (
        arr.arr.(arr.level).(arr.size - arr.level_offset) <- to_push;
        arr.size <- arr.size + 1;
    ) else (
        arr.level <- arr.level + 1;

        let new_level_offset = compute_level_offset arr.level in
        arr.level_offset <- new_level_offset;
        arr.capacity <- compute_level_offset (arr.level + 1);
        arr.arr.(arr.level) <- Array.make new_level_offset arr.default_value;

        arr.arr.(arr.level).(arr.size - arr.level_offset) <- to_push;
        arr.size <- arr.size + 1;
    )

let iter (arr: 'a growing_array) (func: 'a -> unit) =
    let keep_going = ref true in
    let level = ref 0 in
    while !keep_going do
        let level_offset = compute_level_offset !level in
        if level_offset < arr.size then (
            let go_to = min (Array.length arr.arr.(!level)) (arr.size - level_offset) - 1 in
            for i = 0 to go_to do
                func arr.arr.(!level).(i)
            done
        ) else (
            keep_going := false;
        );
        incr level;
    done

let to_array (arr: 'a growing_array) : 'a array =
  let res = Array.make arr.size arr.default_value in
  let counter = ref 0 in
  iter arr (fun i ->
      res.(!counter) <- i;
      incr counter;
    );
  res


type database = {
  disk_manufacturer: Db_types.table_row_disk_manufacturer growing_array;
  disks: Db_types.table_row_disks growing_array;
  server: Db_types.table_row_server growing_array;
}
type database_to_serialize = {
  disk_manufacturer: Db_types.table_row_disk_manufacturer array;
  disks: Db_types.table_row_disks array;
  server: Db_types.table_row_server array;
} [@@deriving yojson]
let _default_table_row_disk_manufacturer: Db_types.table_row_disk_manufacturer = {
  model = "";
}

let _default_table_row_disks: Db_types.table_row_disks = {
  hostname = "";
  disk_id = "";
  size_bytes = 0;
  make = "";
}

let _default_table_row_server: Db_types.table_row_server = {
  hostname = "";
  ram_mb = 0;
}

let the_db: database = {
  disk_manufacturer = create ~default_value:_default_table_row_disk_manufacturer;
  disks = create ~default_value:_default_table_row_disks;
  server = create ~default_value:_default_table_row_server;
}

let def_disk_manufacturer (input: Db_types.table_row_disk_manufacturer)=
  append the_db.disk_manufacturer input

let def_disks (input: Db_types.table_row_disks)=
  append the_db.disks input

let def_server (input: Db_types.table_row_server)=
  append the_db.server input

let dump_state_to_stdout () =
  let final_output: database_to_serialize = {
    disk_manufacturer = to_array the_db.disk_manufacturer;
    disks = to_array the_db.disks;
    server = to_array the_db.server;
  } in
  print_endline "";
  print_endline "time to read boi fVxVFfkSj2yJGb2ury32Z4SkNm9QDTnqb4FKeygXw1";
  database_to_serialize_to_yojson final_output |> Yojson.Safe.to_string |> print_endline;
  print_endline "reading over boi fVxVFfkSj2yJGb2ury32Z4SkNm9QDTnqb4FKeygXw1"


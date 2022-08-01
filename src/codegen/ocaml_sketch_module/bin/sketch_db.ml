let () = assert ((Int.max_int |> Int64.of_int) > (Int32.max_int |> Int64.of_int32))

let fetch_i64_number (buffer: string) (cursor: int ref) =
  let c = !cursor in
  cursor := c + 8;
  String.get_int64_le buffer c

let fetch_bool (buffer: string) (cursor: int ref) =
  let c = !cursor in
  incr cursor;
  match String.get buffer c |> Char.code with
  | 0 -> false
  | _ -> true

let fetch_f64_float (buffer: string) (cursor: int ref) =
  let i64 = fetch_i64_number buffer cursor in
  Int64.float_of_bits i64

let fetch_string (buffer: string) (cursor: int ref) =
  let string_len = fetch_i64_number buffer cursor |> Int64.to_int in
  let string_start = !cursor in
  cursor := string_start + string_len;
  String.sub buffer string_start string_len

let fetch_vector_generic ~init_element ~push_function (buffer: string) (cursor: int ref) =
  let size = fetch_i64_number buffer cursor |> Int64.to_int in
  let res = Array.make size init_element in
  for i = 0 to size - 1 do
    res.(i) <- push_function buffer cursor;
  done;
  res

let fetch_i64_vector =
  fetch_vector_generic
    ~init_element:0
    ~push_function:(fun buffer cursor ->
        fetch_i64_number buffer cursor |> Int64.to_int
      )

let fetch_f64_vector =
  fetch_vector_generic
    ~init_element:0.0
    ~push_function:fetch_f64_float

let fetch_bool_vector =
  fetch_vector_generic
    ~init_element:false
    ~push_function:fetch_bool

let fetch_string_vector =
  fetch_vector_generic
    ~init_element:""
    ~push_function:fetch_string

let fetch_i64_nested_vector =
  fetch_vector_generic
    ~init_element:[||]
    ~push_function:fetch_i64_vector

type table_row_pointer_thic_boi = TableRowPointerThicBoi of int
type table_row_pointer_some_enum = TableRowPointerSomeEnum of int
type table_row_pointer_enum_child_a = TableRowPointerEnumChildA of int
type table_row_pointer_enum_child_b = TableRowPointerEnumChildB of int

type table_row_thic_boi = {
  id: int;
  name: string;
  b: bool;
  f: float;
  fk: table_row_pointer_some_enum;
}

type table_row_some_enum = {
  name: string;
  children_enum_child_a: table_row_pointer_enum_child_a list; (* arrays are mutable, would use those for perf *)
  children_enum_child_b: table_row_pointer_enum_child_b list;
}

type table_row_enum_child_a = {
  inner_name_a: string;
  parent: table_row_pointer_some_enum;
}

type table_row_enum_child_b = {
  inner_name_b: string;
  parent: table_row_pointer_some_enum;
}

type table_definition_thic_boi = {
  iter: (table_row_pointer_thic_boi -> unit) -> unit;
  row: table_row_pointer_thic_boi -> table_row_thic_boi;
  len: int;
  c_id: table_row_pointer_thic_boi -> int;
  c_name: table_row_pointer_thic_boi -> string;
  c_b: table_row_pointer_thic_boi -> bool;
  c_f: table_row_pointer_thic_boi -> float;
  c_fk: table_row_pointer_thic_boi -> table_row_pointer_some_enum;
}

type table_definition_some_enum = {
  iter: (table_row_pointer_some_enum -> unit) -> unit;
  row: table_row_pointer_some_enum -> table_row_some_enum;
  len: int;
  c_name: table_row_pointer_some_enum -> string;
  c_children_enum_child_a: table_row_pointer_some_enum -> table_row_pointer_enum_child_a list;
  c_children_enum_child_b: table_row_pointer_some_enum -> table_row_pointer_enum_child_b list;
}

type table_definition_enum_child_a = {
  iter: (table_row_pointer_enum_child_a -> unit) -> unit;
  row: table_row_pointer_enum_child_a -> table_row_enum_child_a;
  len: int;
  c_inner_name_a: table_row_pointer_enum_child_a -> string;
  c_parent: table_row_pointer_enum_child_a -> table_row_pointer_some_enum;
}

type table_definition_enum_child_b = {
  iter: (table_row_pointer_enum_child_b -> unit) -> unit;
  row: table_row_pointer_enum_child_b -> table_row_enum_child_b;
  len: int;
  c_inner_name_b: table_row_pointer_enum_child_b -> string;
  c_parent: table_row_pointer_enum_child_b -> table_row_pointer_some_enum;
}

type database = {
  thic_boi: table_definition_thic_boi;
  some_enum: table_definition_some_enum;
  enum_child_a: table_definition_enum_child_a;
  enum_child_b: table_definition_enum_child_b;
}

let test_buffer: string = [
3; 0; 0; 0; 0; 0; 0; 0; 1; 0; 0; 0; 0; 0; 0; 0; 2; 0; 0; 0; 0; 0; 0; 0; 3; 0; 0; 0; 0; 0; 0; 0; 3; 0; 0; 0; 0; 0; 0; 0; 6; 0; 0; 0; 0; 0; 0; 0; 104; 101; 121; 32; 104; 111; 13; 0; 0; 0; 0; 0; 0; 0; 104; 101; 114; 101; 32; 115; 104; 101; 32; 103; 111; 101; 115; 11; 0; 0; 0; 0; 0; 0; 0; 101; 105; 116; 104; 101; 114; 32; 98; 108; 97; 104; 3; 0; 0; 0; 0; 0; 0; 0; 1; 0; 1; 3; 0; 0; 0; 0; 0; 0; 0; 174; 71; 225; 122; 20; 174; 243; 63; 174; 71; 225; 122; 20; 174; 9; 64; 184; 30; 133; 235; 81; 184; 21; 64; 3; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 1; 0; 0; 0; 0; 0; 0; 0; 1; 0; 0; 0; 0; 0; 0; 0; 2; 0; 0; 0; 0; 0; 0; 0; 4; 0; 0; 0; 0; 0; 0; 0; 119; 97; 114; 109; 3; 0; 0; 0; 0; 0; 0; 0; 104; 111; 116; 2; 0; 0; 0; 0; 0; 0; 0; 1; 0; 0; 0; 0; 0; 0; 0; 1; 0; 0; 0; 0; 0; 0; 0; 1; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 2; 0; 0; 0; 0; 0; 0; 0; 1; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 1; 0; 0; 0; 0; 0; 0; 0; 1; 0; 0; 0; 0; 0; 0; 0; 2; 0; 0; 0; 0; 0; 0; 0; 11; 0; 0; 0; 0; 0; 0; 0; 98; 97; 114; 101; 108; 121; 32; 119; 97; 114; 109; 11; 0; 0; 0; 0; 0; 0; 0; 109; 101; 100; 105; 117; 109; 32; 119; 97; 114; 109; 2; 0; 0; 0; 0; 0; 0; 0; 1; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 2; 0; 0; 0; 0; 0; 0; 0; 14; 0; 0; 0; 0; 0; 0; 0; 98; 97; 114; 101; 108; 121; 32; 100; 101; 103; 114; 101; 101; 115; 14; 0; 0; 0; 0; 0; 0; 0; 109; 101; 100; 105; 117; 109; 32; 100; 101; 103; 114; 101; 101; 115; 2; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 1; 0; 0; 0; 0; 0; 0; 0
] |> List.map Char.chr |> List.to_seq |> String.of_seq

let deserialize () : database =
  let buffer = test_buffer in
  let cursor = ref 0 in

  let hash_size = 4 in
  let computed_hash = Checkseum.Crc32.digest_string buffer 0 (String.length buffer - hash_size) Checkseum.Crc32.default |> Optint.to_int32 in

  let thic_boi_id = fetch_i64_vector buffer cursor in
  assert (thic_boi_id = [|1; 2; 3|]);
  let thic_boi_name = fetch_string_vector buffer cursor in
  assert (thic_boi_name = [|"hey ho"; "here she goes"; "either blah"|]);
  let thic_boi_b = fetch_bool_vector buffer cursor in
  assert (thic_boi_b = [|true; false; true|]);
  let thic_boi_f = fetch_f64_vector buffer cursor in
  assert (thic_boi_f = [|1.23; 3.21; 5.43|]);
  let thic_boi_fk = fetch_i64_vector buffer cursor in
  assert (thic_boi_fk = [|0; 1; 1|]);
  let thic_boi_fk = Array.map (fun ptr -> TableRowPointerSomeEnum ptr) thic_boi_fk in

  let thic_boi_len = Array.length thic_boi_id in
  assert (thic_boi_len = Array.length thic_boi_name);
  assert (thic_boi_len = Array.length thic_boi_b);
  assert (thic_boi_len = Array.length thic_boi_f);
  assert (thic_boi_len = Array.length thic_boi_fk);

  let some_enum_name = fetch_string_vector buffer cursor in
  assert (some_enum_name = [|"warm"; "hot"|]);
  let some_enum_children_enum_child_a = fetch_i64_nested_vector buffer cursor in
  assert (some_enum_children_enum_child_a = [|
      [|1|];
      [|0|];
    |]);
  let some_enum_children_enum_child_a = Array.map (fun children -> Array.to_list children |> List.map (fun ptr -> TableRowPointerEnumChildA ptr)) some_enum_children_enum_child_a in
  let some_enum_children_enum_child_b = fetch_i64_nested_vector buffer cursor in
  assert (some_enum_children_enum_child_b = [|
      [|0|];
      [|1|];
    |]);
  let some_enum_children_enum_child_b = Array.map (fun children -> Array.to_list children |> List.map (fun ptr -> TableRowPointerEnumChildB ptr)) some_enum_children_enum_child_b in
  let some_enum_len = Array.length some_enum_name in
  assert (some_enum_len = Array.length some_enum_name);
  assert (some_enum_len = Array.length some_enum_children_enum_child_a);
  assert (some_enum_len = Array.length some_enum_children_enum_child_b);

  let enum_child_a_inner_name_a = fetch_string_vector buffer cursor in
  assert (enum_child_a_inner_name_a = [|"barely warm"; "medium warm"|]);
  let enum_child_a_parent = fetch_i64_vector buffer cursor in
  assert (enum_child_a_parent = [|1; 0|]);
  let enum_child_a_parent = Array.map (fun ptr -> TableRowPointerSomeEnum ptr) enum_child_a_parent in
  let enum_child_a_len = Array.length enum_child_a_inner_name_a in
  assert (enum_child_a_len = Array.length enum_child_a_inner_name_a);
  assert (enum_child_a_len = Array.length enum_child_a_parent);

  let enum_child_b_inner_name_b = fetch_string_vector buffer cursor in
  assert (enum_child_b_inner_name_b = [|"barely degrees"; "medium degrees"|]);
  let enum_child_b_parent = fetch_i64_vector buffer cursor in
  assert (enum_child_b_parent = [|0; 1|]);
  let enum_child_b_parent = Array.map (fun ptr -> TableRowPointerSomeEnum ptr) enum_child_b_parent in
  let enum_child_b_len = Array.length enum_child_b_inner_name_b in
  assert (enum_child_b_len = Array.length enum_child_b_inner_name_b);
  assert (enum_child_b_len = Array.length enum_child_b_parent);

  assert (!cursor = String.length buffer);

  let thic_boi_rowids: table_row_pointer_thic_boi array = Array.mapi (fun idx _ -> TableRowPointerThicBoi idx) thic_boi_id in
  let thic_boi_rows: table_row_thic_boi array = Array.map (fun (TableRowPointerThicBoi ptr) -> {
        id = thic_boi_id.(ptr);
        name = thic_boi_name.(ptr);
        b = thic_boi_b.(ptr);
        f = thic_boi_f.(ptr);
        fk = thic_boi_fk.(ptr);
      }) thic_boi_rowids in

  let some_enum_rowids: table_row_pointer_some_enum array = Array.mapi (fun idx _ -> TableRowPointerSomeEnum idx) some_enum_name in
  let some_enum_rows: table_row_some_enum array = Array.map (fun (TableRowPointerSomeEnum ptr) -> {
        name = some_enum_name.(ptr);
        children_enum_child_a = some_enum_children_enum_child_a.(ptr);
        children_enum_child_b = some_enum_children_enum_child_b.(ptr);
      }) some_enum_rowids in

  let enum_child_a_rowids: table_row_pointer_enum_child_a array = Array.mapi (fun idx _ -> TableRowPointerEnumChildA idx) enum_child_a_inner_name_a in
  let enum_child_a_rows: table_row_enum_child_a array = Array.map (fun (TableRowPointerEnumChildA ptr) -> {
        inner_name_a = enum_child_a_inner_name_a.(ptr);
        parent = enum_child_a_parent.(ptr);
      }) enum_child_a_rowids in

  let enum_child_b_rowids: table_row_pointer_enum_child_b array = Array.mapi (fun idx _ -> TableRowPointerEnumChildB idx) enum_child_b_inner_name_b in
  let enum_child_b_rows: table_row_enum_child_b array = Array.map (fun (TableRowPointerEnumChildB ptr) -> {
        inner_name_b = enum_child_b_inner_name_b.(ptr);
        parent = enum_child_a_parent.(ptr);
      }) enum_child_b_rowids in


  let thic_boi: table_definition_thic_boi = {
    iter = (fun f -> Array.iter f thic_boi_rowids);
    row = (fun (TableRowPointerThicBoi ptr) -> thic_boi_rows.(ptr));
    len = thic_boi_len;
    c_id = (fun (TableRowPointerThicBoi ptr) -> thic_boi_id.(ptr));
    c_name = (fun (TableRowPointerThicBoi ptr) -> thic_boi_name.(ptr));
    c_b = (fun (TableRowPointerThicBoi ptr) -> thic_boi_b.(ptr));
    c_f = (fun (TableRowPointerThicBoi ptr) -> thic_boi_f.(ptr));
    c_fk = (fun (TableRowPointerThicBoi ptr) -> thic_boi_fk.(ptr));
  } in

  let some_enum: table_definition_some_enum = {
    iter = (fun f -> Array.iter f some_enum_rowids);
    row = (fun (TableRowPointerSomeEnum ptr) -> some_enum_rows.(ptr));
    len = some_enum_len;
    c_name = (fun (TableRowPointerSomeEnum ptr) -> some_enum_name.(ptr));
    c_children_enum_child_a = (fun (TableRowPointerSomeEnum ptr) -> some_enum_children_enum_child_a.(ptr));
    c_children_enum_child_b = (fun (TableRowPointerSomeEnum ptr) -> some_enum_children_enum_child_b.(ptr));
  } in

  let enum_child_a: table_definition_enum_child_a = {
    iter = (fun f -> Array.iter f enum_child_a_rowids);
    row = (fun (TableRowPointerEnumChildA ptr) -> enum_child_a_rows.(ptr));
    len = enum_child_a_len;
    c_inner_name_a = (fun (TableRowPointerEnumChildA ptr) -> enum_child_a_inner_name_a.(ptr));
    c_parent = (fun (TableRowPointerEnumChildA ptr) -> enum_child_a_parent.(ptr));
  } in

  let enum_child_b: table_definition_enum_child_b = {
    iter = (fun f -> Array.iter f enum_child_b_rowids);
    row = (fun (TableRowPointerEnumChildB ptr) -> enum_child_b_rows.(ptr));
    len = enum_child_b_len;
    c_inner_name_b = (fun (TableRowPointerEnumChildB ptr) -> enum_child_a_inner_name_a.(ptr));
    c_parent = (fun (TableRowPointerEnumChildB ptr) -> enum_child_b_parent.(ptr));
  } in

  {
    thic_boi;
    some_enum;
    enum_child_a;
    enum_child_b;
  }

let db: database =
  deserialize ()

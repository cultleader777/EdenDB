
type table_row_pointer_thic_boi
type table_row_pointer_some_enum
type table_row_pointer_enum_child_a
type table_row_pointer_enum_child_b

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

val db : database

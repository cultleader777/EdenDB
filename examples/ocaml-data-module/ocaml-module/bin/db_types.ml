type table_pkey_disk_manufacturer = TablePkeyDiskManufacturer of string
type table_pkey_disks = TablePkeyDisks of (string * string)
type table_pkey_server = TablePkeyServer of string
type table_row_disk_manufacturer = {
  model: string;
} [@@deriving yojson]

type table_row_disks = {
  hostname: string;
  disk_id: string;
  size_bytes: int;
  make: string;
} [@@deriving yojson]

type table_row_server = {
  hostname: string;
  ram_mb: int;
} [@@deriving yojson]

let mk_disk_manufacturer ~(model: string) () : table_row_disk_manufacturer =
  {
    model;
  }

let mk_disks ~(hostname: string) ~(disk_id: string) ~(size_bytes: int) ~(make: string) () : table_row_disks =
  {
    hostname;
    disk_id;
    size_bytes;
    make;
  }

let mk_server ~(hostname: string) ~(ram_mb: int) () : table_row_server =
  {
    hostname;
    ram_mb;
  }

let pkey_of_disk_manufacturer (input: table_row_disk_manufacturer) : table_pkey_disk_manufacturer =
  TablePkeyDiskManufacturer input.model

let pkey_of_disks (input: table_row_disks) : table_pkey_disks =
  TablePkeyDisks (input.hostname, input.disk_id)

let pkey_of_server (input: table_row_server) : table_pkey_server =
  TablePkeyServer input.hostname

let mk_disks_child_of_server ~(parent: table_pkey_server) ~(disk_id: string) ~(size_bytes: int) ~(make: string) () : table_row_disks =
  let TablePkeyServer hostname = parent in
  {
    hostname;
    disk_id;
    size_bytes;
    make;
  }

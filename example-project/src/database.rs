// Test db content
const DB_BYTES: &[u8] = include_bytes!("edb_data.bin");
lazy_static! {
    pub static ref DB: Database = Database::deserialize(DB_BYTES).unwrap();
}

// Table row pointer types
#[derive(Copy, Clone, Debug, serde::Deserialize, PartialEq)]
pub struct TableRowPointerServer(usize);

#[derive(Copy, Clone, Debug, serde::Deserialize, PartialEq)]
pub struct TableRowPointerDiskManufacturer(usize);

#[derive(Copy, Clone, Debug, serde::Deserialize, PartialEq)]
pub struct TableRowPointerDisks(usize);

// Table struct types
#[derive(Debug)]
pub struct TableRowServer {
    pub hostname: ::std::string::String,
    pub ram_mb: i64,
    pub children_disks: Vec<TableRowPointerDisks>,
}

#[derive(Debug)]
pub struct TableRowDiskManufacturer {
    pub model: ::std::string::String,
    pub referrers_disks__make: Vec<TableRowPointerDisks>,
}

#[derive(Debug)]
pub struct TableRowDisks {
    pub disk_id: ::std::string::String,
    pub size_bytes: i64,
    pub size_mb: i64,
    pub make: TableRowPointerDiskManufacturer,
    pub parent: TableRowPointerServer,
}

// Table definitions
pub struct TableDefinitionServer {
    rows: Vec<TableRowServer>,
    c_hostname: Vec<::std::string::String>,
    c_ram_mb: Vec<i64>,
    c_children_disks: Vec<Vec<TableRowPointerDisks>>,
}

pub struct TableDefinitionDiskManufacturer {
    rows: Vec<TableRowDiskManufacturer>,
    c_model: Vec<::std::string::String>,
    c_referrers_disks__make: Vec<Vec<TableRowPointerDisks>>,
}

pub struct TableDefinitionDisks {
    rows: Vec<TableRowDisks>,
    c_disk_id: Vec<::std::string::String>,
    c_size_bytes: Vec<i64>,
    c_size_mb: Vec<i64>,
    c_make: Vec<TableRowPointerDiskManufacturer>,
    c_parent: Vec<TableRowPointerServer>,
}

// Database definition
pub struct Database {
    server: TableDefinitionServer,
    disk_manufacturer: TableDefinitionDiskManufacturer,
    disks: TableDefinitionDisks,
}

// Database implementation
impl Database {
    pub fn server(&self) -> &TableDefinitionServer {
        &self.server
    }

    pub fn disk_manufacturer(&self) -> &TableDefinitionDiskManufacturer {
        &self.disk_manufacturer
    }

    pub fn disks(&self) -> &TableDefinitionDisks {
        &self.disks
    }

    pub fn deserialize(compressed: &[u8]) -> Result<Database, Box<dyn::std::error::Error>> {
        let hash_size = ::std::mem::size_of::<u64>();
        assert!(compressed.len() > hash_size);
        let compressed_end = compressed.len() - hash_size;
        let compressed_slice = &compressed[0..compressed_end];
        let hash_slice = &compressed[compressed_end..];
        let encoded_hash = ::bincode::deserialize::<u64>(hash_slice).unwrap();
        let computed_hash = ::xxhash_rust::xxh3::xxh3_64(compressed_slice);
        if encoded_hash != computed_hash {
            panic!("EdenDB data is corrupted, checksum mismatch.")
        }
        let input = ::lz4_flex::decompress_size_prepended(compressed_slice).unwrap();
        let mut cursor = ::std::io::Cursor::new(input.as_slice());

        let server_hostname: Vec<::std::string::String> = ::bincode::deserialize_from(&mut cursor)?;
        let server_ram_mb: Vec<i64> = ::bincode::deserialize_from(&mut cursor)?;
        let server_children_disks: Vec<Vec<TableRowPointerDisks>> =
            ::bincode::deserialize_from(&mut cursor)?;

        let server_len = server_children_disks.len();

        assert_eq!(server_len, server_hostname.len());
        assert_eq!(server_len, server_ram_mb.len());

        let mut rows_server: Vec<TableRowServer> = Vec::with_capacity(server_len);
        for row in 0..server_len {
            rows_server.push(TableRowServer {
                hostname: server_hostname[row].clone(),
                ram_mb: server_ram_mb[row],
                children_disks: server_children_disks[row].clone(),
            });
        }

        let disk_manufacturer_model: Vec<::std::string::String> =
            ::bincode::deserialize_from(&mut cursor)?;
        let disk_manufacturer_referrers_disks__make: Vec<Vec<TableRowPointerDisks>> =
            ::bincode::deserialize_from(&mut cursor)?;

        let disk_manufacturer_len = disk_manufacturer_referrers_disks__make.len();

        assert_eq!(disk_manufacturer_len, disk_manufacturer_model.len());

        let mut rows_disk_manufacturer: Vec<TableRowDiskManufacturer> =
            Vec::with_capacity(disk_manufacturer_len);
        for row in 0..disk_manufacturer_len {
            rows_disk_manufacturer.push(TableRowDiskManufacturer {
                model: disk_manufacturer_model[row].clone(),
                referrers_disks__make: disk_manufacturer_referrers_disks__make[row].clone(),
            });
        }

        let disks_disk_id: Vec<::std::string::String> = ::bincode::deserialize_from(&mut cursor)?;
        let disks_size_bytes: Vec<i64> = ::bincode::deserialize_from(&mut cursor)?;
        let disks_size_mb: Vec<i64> = ::bincode::deserialize_from(&mut cursor)?;
        let disks_make: Vec<TableRowPointerDiskManufacturer> =
            ::bincode::deserialize_from(&mut cursor)?;
        let disks_parent: Vec<TableRowPointerServer> = ::bincode::deserialize_from(&mut cursor)?;

        let disks_len = disks_parent.len();

        assert_eq!(disks_len, disks_disk_id.len());
        assert_eq!(disks_len, disks_size_bytes.len());
        assert_eq!(disks_len, disks_size_mb.len());
        assert_eq!(disks_len, disks_make.len());

        let mut rows_disks: Vec<TableRowDisks> = Vec::with_capacity(disks_len);
        for row in 0..disks_len {
            rows_disks.push(TableRowDisks {
                disk_id: disks_disk_id[row].clone(),
                size_bytes: disks_size_bytes[row],
                size_mb: disks_size_mb[row],
                make: disks_make[row],
                parent: disks_parent[row],
            });
        }

        Ok(Database {
            server: TableDefinitionServer {
                rows: rows_server,
                c_hostname: server_hostname,
                c_ram_mb: server_ram_mb,
                c_children_disks: server_children_disks,
            },
            disk_manufacturer: TableDefinitionDiskManufacturer {
                rows: rows_disk_manufacturer,
                c_model: disk_manufacturer_model,
                c_referrers_disks__make: disk_manufacturer_referrers_disks__make,
            },
            disks: TableDefinitionDisks {
                rows: rows_disks,
                c_disk_id: disks_disk_id,
                c_size_bytes: disks_size_bytes,
                c_size_mb: disks_size_mb,
                c_make: disks_make,
                c_parent: disks_parent,
            },
        })
    }
}

// Table definition implementations
impl TableDefinitionServer {
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn rows_iter(&self) -> impl ::std::iter::Iterator<Item = TableRowPointerServer> {
        (0..self.rows.len()).map(|idx| TableRowPointerServer(idx))
    }

    pub fn row(&self, ptr: TableRowPointerServer) -> &TableRowServer {
        &self.rows[ptr.0]
    }

    pub fn c_hostname(&self, ptr: TableRowPointerServer) -> &::std::string::String {
        &self.c_hostname[ptr.0]
    }

    pub fn c_ram_mb(&self, ptr: TableRowPointerServer) -> i64 {
        self.c_ram_mb[ptr.0]
    }

    pub fn c_children_disks(&self, ptr: TableRowPointerServer) -> &[TableRowPointerDisks] {
        &self.c_children_disks[ptr.0]
    }
}

impl TableDefinitionDiskManufacturer {
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn rows_iter(&self) -> impl ::std::iter::Iterator<Item = TableRowPointerDiskManufacturer> {
        (0..self.rows.len()).map(|idx| TableRowPointerDiskManufacturer(idx))
    }

    pub fn row(&self, ptr: TableRowPointerDiskManufacturer) -> &TableRowDiskManufacturer {
        &self.rows[ptr.0]
    }

    pub fn c_model(&self, ptr: TableRowPointerDiskManufacturer) -> &::std::string::String {
        &self.c_model[ptr.0]
    }
}

impl TableDefinitionDisks {
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn rows_iter(&self) -> impl ::std::iter::Iterator<Item = TableRowPointerDisks> {
        (0..self.rows.len()).map(|idx| TableRowPointerDisks(idx))
    }

    pub fn row(&self, ptr: TableRowPointerDisks) -> &TableRowDisks {
        &self.rows[ptr.0]
    }

    pub fn c_disk_id(&self, ptr: TableRowPointerDisks) -> &::std::string::String {
        &self.c_disk_id[ptr.0]
    }

    pub fn c_size_bytes(&self, ptr: TableRowPointerDisks) -> i64 {
        self.c_size_bytes[ptr.0]
    }

    pub fn c_size_mb(&self, ptr: TableRowPointerDisks) -> i64 {
        self.c_size_mb[ptr.0]
    }

    pub fn c_make(&self, ptr: TableRowPointerDisks) -> TableRowPointerDiskManufacturer {
        self.c_make[ptr.0]
    }

    pub fn c_parent(&self, ptr: TableRowPointerDisks) -> TableRowPointerServer {
        self.c_parent[ptr.0]
    }
}

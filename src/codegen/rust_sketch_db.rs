use std::io::Cursor;

use serde::Deserialize;

#[derive(Clone, Copy, PartialEq, Debug, Deserialize)]
pub struct TableRowPointerThicBoi(usize);

impl TableRowPointerThicBoi {
    pub fn row_id(&self) -> usize { self.0 }
}

#[derive(Clone, Copy, PartialEq, Debug, Deserialize)]
pub struct TableRowPointerSomeEnum(usize);

impl TableRowPointerSomeEnum {
    pub fn row_id(&self) -> usize { self.0 }
}

// Table struct types
// allow accessing all fields
// we never allow mutation of these at compile time
// safe to read
#[derive(PartialEq, Debug)]
pub struct TableRowThicBoi {
    pub id: i64,
    pub text: String,
    pub var: TableRowPointerSomeEnum,
}

#[derive(PartialEq, Debug)]
pub struct TableRowSomeEnum {
    pub name: String,
}


// Table definitions
// Do not allow accessing row slices but by our iterators
// Maybe we'll ever need indexes but column store should be always much faster
#[derive(PartialEq, Debug)]
pub struct TableDefinitionThicBoi {
    rows: Vec<TableRowThicBoi>,
    c_id: Vec<i64>,
    c_text: Vec<String>,
    c_var: Vec<TableRowPointerSomeEnum>,
}

#[derive(PartialEq, Debug)]
pub struct TableDefinitionSomeEnum {
    rows: Vec<TableRowSomeEnum>,
    c_name: Vec<String>,
}


impl TableDefinitionThicBoi {
    // all rows iterator
    pub fn rows_iter(&self) -> impl std::iter::Iterator<Item = TableRowPointerThicBoi> {
        (0..self.rows.len()).map(|idx| {
            TableRowPointerThicBoi(idx)
        })
    }

    // one for each table
    pub fn row(&self, ptr: TableRowPointerThicBoi) -> &TableRowThicBoi {
        &self.rows[ptr.0]
    }

    // one per every column
    pub fn c_id(&self, ptr: TableRowPointerThicBoi) -> i64 {
        self.c_id[ptr.0]
    }

    // one per every column
    pub fn c_text(&self, ptr: TableRowPointerThicBoi) -> &String {
        &self.c_text[ptr.0]
    }
}

impl TableDefinitionSomeEnum {
    // all rows iterator
    pub fn rows_iter(&self) -> impl std::iter::Iterator<Item = TableRowPointerSomeEnum> {
        (0..self.rows.len()).map(|idx| {
            TableRowPointerSomeEnum(idx)
        })
    }

    // one for each table
    pub fn row(&self, ptr: TableRowPointerSomeEnum) -> &TableRowSomeEnum {
        &self.rows[ptr.0]
    }

    // one per every column
    pub fn c_text(&self, ptr: TableRowPointerSomeEnum) -> &String {
        &self.c_name[ptr.0]
    }
}

#[derive(PartialEq, Debug)]
pub struct Database {
    thic_boi: TableDefinitionThicBoi,
    some_enum: TableDefinitionSomeEnum,
}

const TEST_DB_BYTES: &[u8] = &[3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 97, 1, 0, 0, 0, 0, 0, 0, 0, 98, 1, 0, 0, 0, 0, 0, 0, 0, 99, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 65, 1, 0, 0, 0, 0, 0, 0, 0, 66];

lazy_static!{
    pub static ref TEST_DB: Database = Database::read_from_bytes(TEST_DB_BYTES).unwrap();
}

impl Database {
    fn read_from_bytes(input: &[u8]) -> Result<Database, Box<dyn std::error::Error>> {
        let mut cursor = Cursor::new(input);

        // thic_boi
        let thic_boi_c_id: Vec<i64> = bincode::deserialize_from(&mut cursor)?;
        let thic_boi_c_text: Vec<String> = bincode::deserialize_from(&mut cursor)?;
        let thic_boi_c_var: Vec<TableRowPointerSomeEnum> = bincode::deserialize_from(&mut cursor)?;
        let thic_boi_len = thic_boi_c_id.len();
        assert_eq!(thic_boi_len, thic_boi_c_text.len());

        let mut thic_boi_rows = Vec::with_capacity(thic_boi_len);
        for row in 0..thic_boi_len {
            thic_boi_rows.push(TableRowThicBoi {
                id: thic_boi_c_id[row],
                text: thic_boi_c_text[row].clone(),
                var: thic_boi_c_var[row],
            });
        }

        // some_enum
        let some_enum_c_name: Vec<String> = bincode::deserialize_from(&mut cursor)?;
        let some_enum_len = some_enum_c_name.len();

        let mut some_enum_rows = Vec::with_capacity(some_enum_len);
        for row in 0..some_enum_len {
            some_enum_rows.push(TableRowSomeEnum {
                name: some_enum_c_name[row].clone(),
            });
        }

        Ok(Database {
            thic_boi: TableDefinitionThicBoi {
                c_id: thic_boi_c_id,
                c_text: thic_boi_c_text,
                c_var: thic_boi_c_var,
                rows: thic_boi_rows,
            },
            some_enum: TableDefinitionSomeEnum {
                c_name: some_enum_c_name,
                rows: some_enum_rows,
            },
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(1024);
        bincode::serialize_into(&mut output, self.thic_boi.c_id.as_slice()).unwrap();
        bincode::serialize_into(&mut output, self.thic_boi.c_text.as_slice()).unwrap();
        // serialize as usize to check compatibility
        let usize_vec = self.thic_boi.c_var.iter().map(|i| i.row_id()).collect::<Vec<_>>();
        bincode::serialize_into(&mut output, usize_vec.as_slice()).unwrap();

        bincode::serialize_into(&mut output, self.some_enum.c_name.as_slice()).unwrap();

        output
    }
}

fn test_db() -> Database {
    // thic_boi table
    let id_vec = vec![
        1,
        2,
        3,
    ];
    let text_vec = [
        "a",
        "b",
        "c",
    ].iter().map(|i| i.to_string()).collect::<Vec<_>>();

    let var_vec = [
        0,
        1,
        0,
    ].iter().map(|i| TableRowPointerSomeEnum(*i)).collect::<Vec<_>>();

    let mut rows_vec: Vec<TableRowThicBoi> = Vec::with_capacity(id_vec.len());
    for idx in 0..id_vec.len() {
        rows_vec.push(TableRowThicBoi { id: id_vec[idx], text: text_vec[idx].clone(), var: var_vec[idx] })
    }

    let thic_boi = TableDefinitionThicBoi {
        c_text: text_vec,
        c_id: id_vec,
        c_var: var_vec,
        rows: rows_vec,
    };

    // some_enum table
    let name_vec = [
        "A",
        "B",
    ].iter().map(|i| i.to_string()).collect::<Vec<_>>();

    let mut rows_vec: Vec<TableRowSomeEnum> = Vec::with_capacity(name_vec.len());
    for idx in 0..name_vec.len() {
        rows_vec.push(TableRowSomeEnum { name: name_vec[idx].clone() })
    }

    let some_enum = TableDefinitionSomeEnum {
        c_name: name_vec,
        rows: rows_vec,
    };

    // db init
    Database {
        thic_boi,
        some_enum,
    }
}

#[test]
fn test_sketch_db_filters() {
    let test_db = test_db();
    let tdef = &test_db.thic_boi;

    // this allows us to use ultra fast
    // column vector array filtration
    // or row based storage when more convenient
    let out_v =
        tdef.rows_iter().filter(|i| {
            tdef.row(*i).id > 1
        }).filter(|i| {
            tdef.c_id(*i) < 3 && !tdef.c_text(*i).is_empty()
        }).collect::<Vec<_>>();

    assert_eq!(out_v.len(), 1);
    assert_eq!(out_v[0].row_id(), 1);
    assert_eq!(tdef.row(out_v[0]), &TableRowThicBoi { id: 2, text: "b".to_string(), var: TableRowPointerSomeEnum(1) });
}

#[test]
fn test_db_serialization() {
    let test_db = test_db();
    let bytes = test_db.serialize_to_bytes();
    // renew if changed
    assert_eq!(bytes.as_slice(), TEST_DB_BYTES);
    let deser_db = Database::read_from_bytes(bytes.as_slice()).unwrap();
    let static_ser = TEST_DB.serialize_to_bytes();

    assert_eq!(test_db, deser_db);
    assert_eq!(bytes, static_ser);
}
use std::path::PathBuf;

use crate::checker::{logic::AllData, types::SerializationVector};

pub mod ocaml;
pub mod ocaml_data_module;
pub mod rust;

pub struct CodegenOutputFile {
    filename: String,
    content: Vec<u8>,
}

pub struct CodegenOutputs {
    #[allow(dead_code)]
    uncompressed_edb_data: Vec<u8>,
    files: Vec<CodegenOutputFile>,
}

/// Don't overwrite file if the same
pub fn write_file_check_if_different(path: &PathBuf, content: &[u8]) {
    match std::fs::read(&path) {
        Ok(existing_bytes) => {
            // write if not changed.
            // help build tools not to rebuild if not needed
            let current = xxhash_rust::xxh3::xxh3_64(existing_bytes.as_slice());
            let to_write = xxhash_rust::xxh3::xxh3_64(content);
            if current != to_write {
                std::fs::write(&path, content).unwrap();
            }
        }
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                std::fs::write(&path, content).unwrap();
            } else {
                panic!("{:?}", e)
            }
        }
    }
}

impl CodegenOutputs {
    pub fn dump_to_dir(&self, output_dir: &str) {
        let dir_path = PathBuf::from(output_dir);
        for i in &self.files {
            let fpath = dir_path.join(i.filename.as_str());
            write_file_check_if_different(&fpath, &i.content);
        }
    }
}

pub trait CodeGenerator {
    fn generate(&self, data: &AllData) -> CodegenOutputs;
}

/// Returns raw uncompressed binary data
pub fn dump_as_bytes(vecs: &Vec<SerializationVector>) -> Vec<u8> {
    let mut output = Vec::with_capacity(1024);

    for sv in vecs {
        match sv {
            crate::checker::types::SerializationVector::Strings(v) => {
                bincode::serialize_into(&mut output, v.v.as_slice()).unwrap();
            }
            crate::checker::types::SerializationVector::Ints(v) => {
                bincode::serialize_into(&mut output, v.v.as_slice()).unwrap();
            }
            crate::checker::types::SerializationVector::Floats(v) => {
                bincode::serialize_into(&mut output, v.v.as_slice()).unwrap();
            }
            crate::checker::types::SerializationVector::Bools(v) => {
                bincode::serialize_into(&mut output, v.v.as_slice()).unwrap();
            }
            crate::checker::types::SerializationVector::Fkeys { sv, .. } => {
                bincode::serialize_into(&mut output, sv.v.as_slice()).unwrap();
            }
            crate::checker::types::SerializationVector::FkeysOneToMany { sv, .. } => {
                bincode::serialize_into(&mut output, sv.v.as_slice()).unwrap();
            }
        }
    }

    output
}

/// output binary format:
/// | lz4 compressed data | checksum (xxhash 8 bytes) |
/// Data is checksummed after compression.
/// Returns uncompressed data and uncompressed data (to check assumptions in tests)
fn dump_as_bytes_lz4_checksum_xxh(vecs: &Vec<SerializationVector>) -> (Vec<u8>, Vec<u8>) {
    let uncompressed = dump_as_bytes(vecs);
    let mut compressed = lz4_flex::compress_prepend_size(uncompressed.as_slice());
    let hash = xxhash_rust::xxh3::xxh3_64(&compressed);
    bincode::serialize_into(&mut compressed, &hash).unwrap();
    (compressed, uncompressed)
}

#[cfg(test)]
fn assert_eden_db_binary_dump_equals(source: &str, expected_dump: &[u8]) {
    use crate::db_parser::{self, InputSource};

    let inputs = &mut [InputSource {
        path: "test".to_string(),
        contents: Some(source.to_string()),
        source_dir: None,
        line_comments: Vec::new(),
    }];

    let gen = rust::RustCodegen::default();
    let sources = db_parser::parse_sources(inputs.as_mut_slice()).unwrap();
    let data = AllData::new(sources).unwrap();
    let codegen_outputs = gen.generate(&data);

    pretty_assertions::assert_eq!(codegen_outputs.uncompressed_edb_data, expected_dump)
}

#[test]
fn test_binary_dump_assumptions() {
    let source = r#"
TABLE thic_boi {
  id INT,
  name TEXT,
  b BOOL,
  f FLOAT,
  fk REF some_enum,
}

TABLE some_enum {
  name TEXT PRIMARY KEY,
}

TABLE enum_child_a {
  inner_name_a TEXT PRIMARY KEY CHILD OF some_enum,
}

TABLE enum_child_b {
  inner_name_b TEXT PRIMARY KEY CHILD OF some_enum,
}

DATA thic_boi {
  1, hey ho, true, 1.23, warm;
  2, here she goes, false, 3.21, hot;
  3, either blah, true, 5.43, hot;
}

DATA enum_child_a(name, inner_name_a) {
  hot, barely warm;
  warm, medium warm;
}

DATA enum_child_b(name, inner_name_b) {
  warm, barely degrees;
  hot, medium degrees;
}

DATA EXCLUSIVE some_enum {
  warm;
  hot;
}
"#;
    let fb_123 = 1.23_f64.to_le_bytes();
    let fb_321 = 3.21_f64.to_le_bytes();
    let fb_543 = 5.43_f64.to_le_bytes();

    let expected_dump: &[u8] = &[
        // enum_child_a table
        //  inner_name_a column size
        2, 0, 0, 0, 0, 0, 0, 0, //  inner_name_a column elements
        //   barely warm
        11, 0, 0, 0, 0, 0, 0, 0, b'b', b'a', b'r', b'e', b'l', b'y', b' ', b'w', b'a', b'r', b'm',
        //   medium warm
        11, 0, 0, 0, 0, 0, 0, 0, b'm', b'e', b'd', b'i', b'u', b'm', b' ', b'w', b'a', b'r', b'm',
        //  parent column size
        2, 0, 0, 0, 0, 0, 0, 0, //  parent column elements
        1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        // enum_child_b table
        //  inner_name_b column size
        2, 0, 0, 0, 0, 0, 0, 0, //   barely degrees
        14, 0, 0, 0, 0, 0, 0, 0, b'b', b'a', b'r', b'e', b'l', b'y', b' ', b'd', b'e', b'g', b'r',
        b'e', b'e', b's', //   medium degrees
        14, 0, 0, 0, 0, 0, 0, 0, b'm', b'e', b'd', b'i', b'u', b'm', b' ', b'd', b'e', b'g', b'r',
        b'e', b'e', b's', // parent column size
        2, 0, 0, 0, 0, 0, 0, 0, // parent column elements
        0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
        // some_enum table
        //  name column size
        2, 0, 0, 0, 0, 0, 0, 0, //  name column elements
        //   warm
        4, 0, 0, 0, 0, 0, 0, 0, b'w', b'a', b'r', b'm', //   hot
        3, 0, 0, 0, 0, 0, 0, 0, b'h', b'o', b't',
        //  children_enum_child_a elements
        //   children vector count, same as table row count
        2, 0, 0, 0, 0, 0, 0, 0, //   first vector children count
        1, 0, 0, 0, 0, 0, 0, 0, //   children values
        1, 0, 0, 0, 0, 0, 0, 0, //   second vector children count
        1, 0, 0, 0, 0, 0, 0, 0, //   children values
        0, 0, 0, 0, 0, 0, 0, 0,
        //  children_enum_child_b elements
        //   children vector count, same as table row count
        2, 0, 0, 0, 0, 0, 0, 0, //   first vector children count
        1, 0, 0, 0, 0, 0, 0, 0, //   children values
        0, 0, 0, 0, 0, 0, 0, 0, //   second vector children count
        1, 0, 0, 0, 0, 0, 0, 0, //   children values
        1, 0, 0, 0, 0, 0, 0, 0, //  referrer_thic_boi__fk column size
        2, 0, 0, 0, 0, 0, 0, 0, //  referrer_thic_boi__fk column first row element count
        1, 0, 0, 0, 0, 0, 0, 0, //  referrer_thic_boi__fk column first row elements
        0, 0, 0, 0, 0, 0, 0, 0, //  referrer_thic_boi__fk column second row element count
        2, 0, 0, 0, 0, 0, 0, 0, //  referrer_thic_boi__fk column second row elements
        1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
        // thic_boi table
        //  size, 8 bytes, little endian
        //  id column size
        3, 0, 0, 0, 0, 0, 0, 0, //  id column elements
        1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        //  name column size
        3, 0, 0, 0, 0, 0, 0, 0, //  name column elements
        //   hey ho, size + bytes
        6, 0, 0, 0, 0, 0, 0, 0, b'h', b'e', b'y', b' ', b'h', b'o',
        //   here she goes, size + bytes
        13, 0, 0, 0, 0, 0, 0, 0, b'h', b'e', b'r', b'e', b' ', b's', b'h', b'e', b' ', b'g', b'o',
        b'e', b's', //   either blah, size + bytes
        11, 0, 0, 0, 0, 0, 0, 0, b'e', b'i', b't', b'h', b'e', b'r', b' ', b'b', b'l', b'a', b'h',
        //  b column size
        3, 0, 0, 0, 0, 0, 0, 0, //  b elements
        1, 0, 1, //  f column size
        3, 0, 0, 0, 0, 0, 0, 0, //  f elements
        fb_123[0], fb_123[1], fb_123[2], fb_123[3], fb_123[4], fb_123[5], fb_123[6], fb_123[7],
        fb_321[0], fb_321[1], fb_321[2], fb_321[3], fb_321[4], fb_321[5], fb_321[6], fb_321[7],
        fb_543[0], fb_543[1], fb_543[2], fb_543[3], fb_543[4], fb_543[5], fb_543[6], fb_543[7],
        //  fk column size
        3, 0, 0, 0, 0, 0, 0, 0, //  fk column elements
        0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
    ];

    assert_eden_db_binary_dump_equals(source, expected_dump);
}

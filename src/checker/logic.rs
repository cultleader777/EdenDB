#![allow(clippy::needless_range_loop)]

use std::{
    collections::{HashMap, HashSet, BTreeMap},
    sync::Mutex, path::PathBuf, str::FromStr,
};

use mlua::Function;
use once_cell::sync::Lazy;
use rand::Rng;
use regex::Regex;
use rusqlite::ffi::{sqlite3_column_origin_name, sqlite3_column_table_name};

use crate::{
    checker::types::{
        ColumnVector, ColumnVectorGeneric, ContextualInsertStackItem, DBType, KeyType,
        NestedInsertionMode,
    },
    db_parser::{
        SourceOutputs, TableColumn, TableData, TableDataSegment, TableDataStruct,
        TableDataStructField, TableDataStructFields, TableDefinition, ValueWithPos, valid_unquoted_data_char,
    }, codegen::write_file_check_if_different,
};

use super::{
    errors::DatabaseValidationError,
    types::{
        DBIdentifier, DataColumn, DataTable, ForeignKey, SerializationVector, SerializedVector,
        UniqConstraint,
    },
};

pub(crate) struct SqliteDBs {
    pub(crate) rw: Mutex<rusqlite::Connection>,
    pub(crate) ro: Mutex<rusqlite::Connection>,
}

#[derive(Hash, PartialEq, Eq)]
pub struct ForeignKeyRelationship {
    pub referred_table: DBIdentifier,
    pub referee_table: DBIdentifier,
    pub referee_column: DBIdentifier,
}

#[derive(Hash, PartialEq, Eq)]
pub struct ForeignKeyToForeignChildRelationship {
    pub referred_table: DBIdentifier,
    pub referee_table: DBIdentifier,
    pub referee_column: DBIdentifier,
}

#[derive(Hash, PartialEq, Eq)]
pub struct ForeignKeyToNativeChildRelationship {
    pub referred_table: DBIdentifier,
    pub referee_table: DBIdentifier,
    pub referee_column: DBIdentifier,
}

#[derive(Hash, PartialEq, Eq)]
pub struct ParentKeyRelationship {
    pub parent_table: DBIdentifier,
    pub child_table: DBIdentifier,
}

pub struct ForeignKeyRelationshipData {
    pub foreign_keys_data: Vec<usize>,
    pub reverse_referrees_data: Vec<Vec<usize>>,
}

pub struct ForeignKeyToForeignChildRelationshipData {
    pub refereed_columns_by_key: Vec<DBIdentifier>,
    pub common_parent_keys: Vec<DBIdentifier>,
}

pub struct ForeignKeyToNativeChildRelationshipData {
    pub refereed_columns_by_key: Vec<DBIdentifier>,
    pub common_keys: Vec<DBIdentifier>,
}

pub struct ParentKeyRelationshipData {
    pub parents_for_children_index: Vec<usize>,
    pub children_for_parents_index: Vec<Vec<usize>>,
}

pub struct SingleRowReplacement {
    pub values: BTreeMap<String, String>,
    pub use_count: std::cell::RefCell<usize>,
}

#[derive(Clone)]
pub struct ScheduledValueReplacementInSource {
    pub source_file_idx: i32,
    pub offset_start: usize,
    pub offset_end: usize,
    pub value_to_replace_with: String,
}

pub struct AllData {
    pub(crate) tables: Vec<DataTable>,
    pub(crate) foreign_keys_map: HashMap<ForeignKeyRelationship, ForeignKeyRelationshipData>,
    pub(crate) foreign_to_foreign_child_keys_map:
        HashMap<ForeignKeyToForeignChildRelationship, ForeignKeyToForeignChildRelationshipData>,
    pub(crate) foreign_to_native_child_keys_map:
        HashMap<ForeignKeyToNativeChildRelationship, ForeignKeyToNativeChildRelationshipData>,
    pub(crate) parent_child_keys_map: HashMap<ParentKeyRelationship, ParentKeyRelationshipData>,
    pub(crate) table_replacements: HashMap<usize, HashMap<String, SingleRowReplacement>>,
    pub(crate) source_replacements: Vec<ScheduledValueReplacementInSource>,
    pub(crate) lua_runtime: Lazy<Mutex<mlua::Lua>>,
    pub(crate) sqlite_db: Lazy<SqliteDBs>,
    #[cfg(feature = "datalog")]
    pub(crate) datalog_db: Lazy<Mutex<asdi::Program>>,
}

impl AllData {
    fn init_all_data() -> AllData {
        AllData {
            tables: vec![],
            foreign_keys_map: HashMap::new(),
            foreign_to_foreign_child_keys_map: HashMap::new(),
            foreign_to_native_child_keys_map: HashMap::new(),
            parent_child_keys_map: HashMap::new(),
            table_replacements: HashMap::new(),
            source_replacements: Vec::new(),
            lua_runtime: Lazy::new(|| Mutex::new(mlua::Lua::new())),
            sqlite_db: Lazy::new(|| {
                let this_counter = rand::thread_rng().gen::<usize>();
                let this_db_name = format!("file:edendb_{this_counter}?mode=memory&cache=shared");

                let conn = rusqlite::Connection::open(this_db_name.as_str()).unwrap();
                let rw = Mutex::new(conn);

                let conn = rusqlite::Connection::open(this_db_name.as_str()).unwrap();
                let _ = conn
                    .execute("PRAGMA query_only = true;", rusqlite::params![])
                    .unwrap();
                let ro = Mutex::new(conn);

                SqliteDBs { rw, ro }
            }),
            #[cfg(feature = "datalog")]
            datalog_db: Lazy::new(|| {
                let mut features = asdi::features::FeatureSet::default();
                features.add_support_for(&asdi::features::FEATURE_COMPARISONS);
                features.add_support_for(&asdi::features::FEATURE_CONSTRAINTS);
                features.add_support_for(&asdi::features::FEATURE_NEGATION);
                Mutex::new(asdi::Program::new_with_features(features))
            }),
        }
    }

    #[cfg(test)]
    pub fn new(outputs: SourceOutputs) -> Result<AllData, DatabaseValidationError> {
        Self::new_with_flags(outputs, false)
    }

    pub fn new_with_flags(
        outputs: SourceOutputs,
        sqlite_needed: bool,
    ) -> Result<AllData, DatabaseValidationError> {
        let mut res = AllData::init_all_data();

        maybe_load_lua_runtime(&mut res, &outputs)?;

        crunch_tables_metadata(&mut res, &outputs)?;
        check_exclusive_data_violations(outputs.table_data_segments())?;

        // insert all data with replacements if they exist
        check_replacements(&mut res, &outputs)?;
        insert_main_data(&mut res, &outputs)?;
        insert_extra_data(&mut res, outputs.table_data_segments())?;
        maybe_insert_lua_data(&mut res, &outputs)?;
        check_unused_replacements(&mut res)?;
        compute_generated_columns(&mut res)?;
        maybe_insert_sqlite_data(&mut res, &outputs, sqlite_needed)?;
        compute_materialized_views(&mut res)?;
        validate_data(&mut res)?;

        run_sqlite_proofs(&mut res, &outputs)?;

        maybe_prepare_datalog_data(&mut res, &outputs)?;
        #[cfg(feature = "datalog")]
        run_datalog_proofs(&mut res, &outputs)?;

        // after all checks have passed process replacements if they exist
        process_source_replacements(&mut res, &outputs);

        Ok(res)
    }

    #[cfg(test)]
    pub fn data_as_json(&self) -> serde_json::Value {
        use serde_json::Number;
        use serde_json::Value;

        let mut tables = serde_json::Map::default();

        for t in &self.tables {
            let mut rows = Vec::with_capacity(t.len());

            for ridx in 0..t.len() {
                let mut row_value = serde_json::Map::default();

                for col in &t.columns {
                    let cn = col.column_name.as_str().to_string();
                    match &col.data {
                        ColumnVector::Strings(v) => {
                            let i = row_value.insert(cn, Value::String(v.v[ridx].clone()));
                            assert!(i.is_none());
                        }
                        ColumnVector::Ints(v) => {
                            let i = row_value.insert(
                                cn,
                                Value::Number(Number::from_f64(v.v[ridx] as f64).unwrap()),
                            );
                            assert!(i.is_none());
                        }
                        ColumnVector::Floats(v) => {
                            let i = row_value
                                .insert(cn, Value::Number(Number::from_f64(v.v[ridx]).unwrap()));
                            assert!(i.is_none());
                        }
                        ColumnVector::Bools(v) => {
                            let i = row_value.insert(cn, Value::Bool(v.v[ridx]));
                            assert!(i.is_none());
                        }
                    }
                }

                rows.push(Value::Object(row_value));
            }

            tables.insert(t.name.as_str().to_string(), Value::Array(rows));
        }

        Value::Object(tables)
    }

    pub fn find_table_named_idx(&self, dbi: &DBIdentifier) -> Vec<usize> {
        let mut res = Vec::with_capacity(1);
        for (idx, i) in self.tables.iter().enumerate() {
            if i.name.as_str() == dbi.as_str() {
                res.push(idx);
            }
        }
        res
    }

    pub fn children_tables(&self, parent_table: &DataTable) -> Vec<&DataTable> {
        let mut res = Vec::new();

        for maybe_child in &self.tables {
            if maybe_child.name != parent_table.name {
                if let Some(parent) = maybe_child.parent_table() {
                    if parent == parent_table.name {
                        res.push(maybe_child)
                    }
                }
            }
        }

        res
    }

    pub fn all_parent_tables(&self, child_table: &DataTable) -> Vec<DBIdentifier> {
        let mut res = Vec::new();

        let mut current_table = child_table;
        while let Some(parent) = current_table.parent_table() {
            res.push(parent.clone());

            let mut found = false;
            for t in &self.tables {
                if t.name == parent {
                    found = true;
                    current_table = t;
                    break;
                }
            }
            assert!(found);
        }

        res.reverse();

        res
    }

    pub fn referee_columns(&self, referred_table: &DataTable) -> Vec<(&DataTable, &DataColumn)> {
        let mut res = Vec::new();

        for maybe_referrer in &self.tables {
            for col in &maybe_referrer.columns {
                if let Some(ForeignKey { foreign_table, .. }) = &col.maybe_foreign_key {
                    if foreign_table == &referred_table.name {
                        res.push((maybe_referrer, col));
                    }
                }
            }
        }

        res
    }

    pub fn serialization_vectors(&self) -> Vec<SerializationVector<'_>> {
        // approximate column size
        let mut res = Vec::with_capacity(self.tables.len() * 8);
        let mut table_refs = self.tables.iter().collect::<Vec<_>>();
        table_refs.sort_by_key(|i| i.name.as_str());

        for t in &table_refs {
            for c in &t.columns {
                if let Some(ForeignKey { foreign_table, .. }) = &c.maybe_foreign_key {
                    let fk_vec = self
                        .foreign_keys_map
                        .get(&ForeignKeyRelationship {
                            referred_table: foreign_table.clone(),
                            referee_table: t.name.clone(),
                            referee_column: c.column_name.clone(),
                        })
                        .unwrap();

                    res.push(SerializationVector::Fkeys {
                        sv: SerializedVector {
                            table_name: t.name.as_str(),
                            column_name: c.column_name.as_str().to_string(),
                            v: &fk_vec.foreign_keys_data,
                            last_for_table: false,
                        },
                        foreign_table: foreign_table.as_str().to_string(),
                    });
                } else {
                    match &c.key_type {
                        crate::checker::types::KeyType::ParentPrimary { .. } => {
                            // these are redundant and parents can be reached via .parent field
                        }
                        crate::checker::types::KeyType::NotAKey
                        | crate::checker::types::KeyType::Primary
                        | crate::checker::types::KeyType::ChildPrimary { .. } => match &c.data {
                            ColumnVector::Strings(v) => {
                                res.push(SerializationVector::Strings(SerializedVector {
                                    table_name: t.name.as_str(),
                                    column_name: c.column_name.as_str().to_string(),
                                    v: &v.v,
                                    last_for_table: false,
                                }));
                            }
                            ColumnVector::Ints(v) => {
                                res.push(SerializationVector::Ints(SerializedVector {
                                    table_name: t.name.as_str(),
                                    column_name: c.column_name.as_str().to_string(),
                                    v: &v.v,
                                    last_for_table: false,
                                }));
                            }
                            ColumnVector::Floats(v) => {
                                res.push(SerializationVector::Floats(SerializedVector {
                                    table_name: t.name.as_str(),
                                    column_name: c.column_name.as_str().to_string(),
                                    v: &v.v,
                                    last_for_table: false,
                                }));
                            }
                            ColumnVector::Bools(v) => {
                                res.push(SerializationVector::Bools(SerializedVector {
                                    table_name: t.name.as_str(),
                                    column_name: c.column_name.as_str().to_string(),
                                    v: &v.v,
                                    last_for_table: false,
                                }));
                            }
                        },
                    }
                }
            }

            if let Some(maybe_parent) = t.parent_table() {
                // parent keys are stored directly as pointers to other table
                let fk_vec = self
                    .parent_child_keys_map
                    .get(&ParentKeyRelationship {
                        parent_table: maybe_parent.clone(),
                        child_table: t.name.clone(),
                    })
                    .unwrap();

                res.push(SerializationVector::Fkeys {
                    sv: SerializedVector {
                        table_name: t.name.as_str(),
                        column_name: "parent".to_string(),
                        v: &fk_vec.parents_for_children_index,
                        last_for_table: false,
                    },
                    foreign_table: maybe_parent.as_str().to_string(),
                });
            }

            for child in self.children_tables(t) {
                let fk_vec = self
                    .parent_child_keys_map
                    .get(&ParentKeyRelationship {
                        parent_table: t.name.clone(),
                        child_table: child.name.clone(),
                    })
                    .unwrap();

                res.push(SerializationVector::FkeysOneToMany {
                    sv: SerializedVector {
                        table_name: t.name.as_str(),
                        column_name: format!("children_{}", child.name.as_str()),
                        v: &fk_vec.children_for_parents_index,
                        last_for_table: false,
                    },
                    foreign_table: child.name.as_str().to_string(),
                });
            }

            for (ref_tbl, ref_col) in self.referee_columns(t) {
                let fk_vec = self
                    .foreign_keys_map
                    .get(&ForeignKeyRelationship {
                        referred_table: t.name.clone(),
                        referee_table: ref_tbl.name.clone(),
                        referee_column: ref_col.column_name.clone(),
                    })
                    .unwrap();

                res.push(SerializationVector::FkeysOneToMany {
                    sv: SerializedVector {
                        table_name: t.name.as_str(),
                        column_name: format!(
                            "referrers_{}__{}",
                            ref_tbl.name.as_str(),
                            ref_col.column_name.as_str()
                        ),
                        v: &fk_vec.reverse_referrees_data,
                        last_for_table: false,
                    },
                    foreign_table: ref_tbl.name.as_str().to_string(),
                });
            }

            if let Some(l) = res.last_mut() {
                match l {
                    SerializationVector::Strings(v) => {
                        v.last_for_table = true;
                    }
                    SerializationVector::Ints(v) => {
                        v.last_for_table = true;
                    }
                    SerializationVector::Floats(v) => {
                        v.last_for_table = true;
                    }
                    SerializationVector::Bools(v) => {
                        v.last_for_table = true;
                    }
                    SerializationVector::Fkeys { sv, .. } => {
                        sv.last_for_table = true;
                    }
                    SerializationVector::FkeysOneToMany { sv, .. } => {
                        sv.last_for_table = true;
                    }
                }
            }
        }

        res
    }

    pub fn tables_sorted(&self) -> Vec<&DataTable> {
        let mut res = Vec::with_capacity(self.tables.len());
        for t in &self.tables {
            res.push(t);
        }
        res.sort_by_key(|t| t.name.as_str());
        res
    }
}

fn check_replacements(
    res: &mut AllData,
    outputs: &SourceOutputs,
) -> Result<(), DatabaseValidationError> {

    // check that all tables exist and that colums referred to exist
    for (table_name, values) in outputs.value_replacements() {
        let table_name = DBIdentifier::new(&table_name)?;
        let table = res.find_table_named_idx(&table_name);
        if table.is_empty() {
            return Err(DatabaseValidationError::ReplacementsTargetTableDoesntExist {
                table: table_name.as_str().to_string(),
            });
        }

        assert_eq!(table.len(), 1);
        let table_idx = table[0];
        let table = &res.tables[table_idx];

        if let Some(pkey) = table.primary_key_column() {
            match &pkey.key_type {
                KeyType::ChildPrimary { .. } => {} // ok
                KeyType::Primary => {} // ok
                _ => {
                    // now support only this easy case
                    return Err(DatabaseValidationError::ReplacementsIsSupportedOnlyByPrimaryKey {
                        table: table_name.as_str().to_string(),
                    });
                }
            }
        } else {
            return Err(DatabaseValidationError::ReplacementsTargetTableDoesntHavePrimaryKey {
                table: table_name.as_str().to_string(),
            });
        }

        let primary_keys_count = table.primary_keys_with_parents().len();
        let mut prim_key_map: HashSet<&String> = HashSet::new();

        // duplicate keys could be passed
        for row in values {
            if !prim_key_map.insert(&row.primary_key) {
                return Err(DatabaseValidationError::ReplacementsDuplicatePrimaryKeyDetected {
                    table: table_name.as_str().to_string(),
                    replacement_primary_key: row.primary_key.clone(),
                });
            }

            if row.primary_key.split("=>").count() != primary_keys_count {
                return Err(DatabaseValidationError::ReplacementsUnexpectedKeySegmentCount {
                    table: table_name.as_str().to_string(),
                    replacement_primary_key: row.primary_key.clone(),
                    segments: row.primary_key.split("=>").map(|i| i.to_string()).collect(),
                    expected_segments: primary_keys_count,
                    segment_separator: "=>".to_string(),
                });
            }

            // just check if it exists
            for (target_column, _) in &row.replacements {
                let found = table.columns.iter().find(|i| i.column_name.as_str() == target_column);
                if let Some(found) = found {
                    if found.generate_expression.is_some() {
                        return Err(DatabaseValidationError::ReplacementCannotBeProvidedForGeneratedColumn {
                            table: table_name.as_str().to_string(),
                            replacement_primary_key: row.primary_key.clone(),
                            generated_column: target_column.as_str().to_string(),
                        });
                    }

                    // disallow replacements of parent primary keys for possibly nested
                    // keys because data would be no longer part of same parent structure
                    if matches!(found.key_type, KeyType::ParentPrimary { .. }) {
                        return Err(DatabaseValidationError::ReplacementsCannotReplaceParentPrimaryKey {
                            table: table_name.as_str().to_string(),
                            replacement_primary_key: row.primary_key.clone(),
                            parent_primary_key_column: found.column_name.as_str().to_string(),
                        });
                    }
                } else {
                    return Err(DatabaseValidationError::ReplacementsColumnNotFound {
                        table: table_name.as_str().to_string(),
                        replacement_primary_key: row.primary_key.clone(),
                        column_not_found: target_column.as_str().to_string(),
                        available_columns: table.columns.iter().map(|i| i.column_name.as_str().to_string()).collect(),
                    });
                }
            }

            let e = res.table_replacements.entry(table_idx).or_default();
            let v = SingleRowReplacement {
                use_count: std::cell::RefCell::new(0),
                values: row.replacements.clone(),
            };
            assert!(e.insert(row.primary_key.clone(), v).is_none());
        }
    }

    Ok(())
}

fn process_source_replacements(res: &mut AllData, so: &SourceOutputs) {
    if res.source_replacements.is_empty() {
        return;
    }

    // 1. group by all replacements into source files
    // 2. sort all the replacements in every source file
    // 3. run the loop to execute the replacements to create new string
    // 4. readd comments to lines as if nothing happpened
    // 5. write to the disk

    let mut replacements_map: BTreeMap<i32, Vec<ScheduledValueReplacementInSource>> = BTreeMap::new();
    for repl in &res.source_replacements {
        let v = replacements_map.entry(repl.source_file_idx).or_default();
        v.push(repl.clone());
    }

    for v in replacements_map.values_mut() {
        v.sort_by_key(|i| i.offset_start);
    }

    for (source_idx, replacements) in &replacements_map {
        let source_idx: usize = (*source_idx).try_into().expect("Invalid replacements shouldn't exist?");
        let target_to_replace = &so.sources_db()[source_idx];
        let source = target_to_replace.contents.as_ref().unwrap();

        let mut output_source = String::with_capacity(source.len());
        let mut source_cursor: usize = 0;
        for repl in replacements {
            output_source += &source[source_cursor..repl.offset_start];
            let pre = if repl.offset_start > 0 { source.as_bytes().get(repl.offset_start - 1) } else { None };
            let post = source.as_bytes().get(repl.offset_end);
            let is_already_quoted = pre.as_deref() == Some(&b'"') && post.as_deref() == Some(&b'"');
            let should_be_quoted = !is_already_quoted && !repl.value_to_replace_with.chars().all(valid_unquoted_data_char);
            // TODO: nice error handling?
            assert!(!repl.value_to_replace_with.contains("\""));
            if should_be_quoted {
                output_source.push('"');
            }
            output_source += &repl.value_to_replace_with;
            if should_be_quoted {
                output_source.push('"');
            }
            source_cursor = repl.offset_end;
        }
        output_source += &source[source_cursor..];

        let mut with_comments: String = String::with_capacity(output_source.len());
        for (idx, line) in output_source.lines().enumerate() {
            with_comments += line;
            with_comments += &target_to_replace.line_comments[idx];
            with_comments += "\n";
        }

        println!("Writing modifications to file {}", &target_to_replace.path);
        let path = PathBuf::from_str(&target_to_replace.path).unwrap();
        write_file_check_if_different(&path, with_comments.as_bytes());
    }
}

fn check_unused_replacements(
    res: &mut AllData,
) -> Result<(), DatabaseValidationError> {

    for (table_idx, v) in &res.table_replacements {
        for (replacement_pkey, repl_value) in v {
            let use_count = repl_value.use_count.borrow();
            if *use_count == 0 {
                return Err(DatabaseValidationError::ReplacementNeverUsed {
                    table: res.tables[*table_idx].name.as_str().to_string(),
                    replacement_primary_key: replacement_pkey.clone(),
                    replacement_uses: *use_count,
                    replacement_columns: repl_value.values.keys().map(|i| i.clone()).collect(),
                    replacement_values: repl_value.values.values().map(|i| i.clone()).collect(),
                });
            }
        }
    }

    Ok(())
}

fn maybe_load_lua_runtime(
    res: &mut AllData,
    outputs: &SourceOutputs,
) -> Result<(), DatabaseValidationError> {
    let segments = outputs.lua_segments();
    if !segments.is_empty() {
        let lua = res.lua_runtime.lock().unwrap();

        lua.load(lua_internal_library())
            .exec()
            .expect("Standard lua runtime has bugs");

        for s in segments {
            if let Some(sd) = &s.source_dir {
                let lstr = lua.create_string(sd.as_bytes()).unwrap();
                lua.globals()
                    .set("SOURCE_DIR", mlua::Value::String(lstr))
                    .unwrap();
            }
            let c = lua
                .load(s.contents.as_ref().unwrap())
                .set_name(s.path.as_str())
                .unwrap();
            c.exec()
                .map_err(|e| DatabaseValidationError::LuaSourcesLoadError {
                    error: e.to_string(),
                    source_file: s.path.clone(),
                })?
        }

        lua.globals().raw_remove("SOURCE_DIR").unwrap();
    }

    Ok(())
}

fn lua_internal_library() -> &'static str {
    r#"

__do_not_refer_to_this_internal_value_in_your_code_dumbo__ = {}
function data(targetTable, newRow)
    local queue = __do_not_refer_to_this_internal_value_in_your_code_dumbo__

    if queue[targetTable] == nil then
        queue[targetTable] = {}
    end

    -- we simply accumulate values to insert in lua runtime and then process
    -- them in one go in rust
    table.insert(queue[targetTable], newRow)
end

"#
}

fn crunch_tables_metadata(
    res: &mut AllData,
    outputs: &SourceOutputs,
) -> Result<(), DatabaseValidationError> {
    init_all_declared_tables(res, outputs)?;
    validate_table_metadata_interconnections(res)?;
    process_detached_defaults(res, outputs)?;
    assert_uniq_constraints_columns(res, outputs)?;
    assert_table_column_order(res)?;
    assert_key_types_in_table(res)?;

    Ok(())
}

fn validate_data(res: &mut AllData) -> Result<(), DatabaseValidationError> {
    assert_row_vector_lengths_are_equal_for_all_tables(res);

    ensure_child_foreign_keys_are_restricted(res)?;
    ensure_no_nan_or_infinity_floats(res)?;
    ensure_primary_keys_unique_per_table_and_fkeys_exist(res)?;
    ensure_parent_primary_keys_exist_for_children(res)?;
    ensure_child_primary_keys_unique_per_table_and_fkeys_exist(res)?;
    ensure_uniq_constaints_are_not_violated(res)?;
    ensure_row_checks(res)?;

    Ok(())
}

fn assert_row_vector_lengths_are_equal_for_all_tables(res: &AllData) {
    // internal error, if this triggers we screwed up, not the client
    for t in &res.tables {
        let v: HashSet<_> = t.columns.iter().map(|i| i.data.len()).collect();
        assert_eq!(v.len(), 1);

        // no point in having table without columns
        assert!(!t.columns.is_empty());
    }
}

#[cfg(not(feature = "datalog"))]
fn maybe_prepare_datalog_data(
    _res: &mut AllData,
    so: &SourceOutputs,
) -> Result<(), DatabaseValidationError> {
    if !so.datalog_proofs().is_empty() {
        return Err(DatabaseValidationError::DatalogIsDisabled {
            explanation: "EdenDB was compiled without datalog support, please recompile EdenDB with 'datalog' feature if you need it.".to_string(),
        });
    }

    Ok(())
}

#[cfg(feature = "datalog")]
fn maybe_prepare_datalog_data(
    res: &mut AllData,
    so: &SourceOutputs,
) -> Result<(), DatabaseValidationError> {
    use asdi::edb::{Attribute, AttributeKind, Constant, Number};

    if so.datalog_proofs().is_empty() {
        return Ok(());
    }

    let mut db = res.datalog_db.lock().unwrap();

    // output data format in datalog:
    // t_<table_name>__<column_name>(<column_value>, <tuple_index>)
    //
    // for instance, for data
    // ```
    // TABLE person {
    //   name TEXT,
    // }
    //
    // DATA person {
    //   mclassen,
    //   doofus,
    // }
    // ```
    // following datalog rules:
    // ```
    // t_person__name(mclasen, 0).
    // t_person__name(doofus, 1).
    // ```

    for table in &res.tables {
        for column in &table.columns {
            let fact_name = format!("t_{}__{}", table.name.as_str(), column.column_name.as_str());
            let table_pred = db.predicates().fetch(fact_name.as_str()).unwrap();
            match &column.data {
                ColumnVector::Strings(v) => {
                    let fact = db
                        .add_new_intensional_relation(
                            table_pred.clone(),
                            vec![Attribute::string(), Attribute::integer()],
                        )
                        .unwrap();

                    for (idx, s) in v.v.iter().enumerate() {
                        fact.add_as_fact(vec![
                            Constant::String(s.clone()),
                            Constant::Number(Number::from_i64(idx as i64)),
                        ])
                        .unwrap();
                    }
                }
                ColumnVector::Ints(v) => {
                    let fact = db
                        .add_new_intensional_relation(
                            table_pred.clone(),
                            vec![Attribute::integer(), Attribute::integer()],
                        )
                        .unwrap();

                    for (idx, s) in v.v.iter().enumerate() {
                        fact.add_as_fact(vec![
                            Constant::Number(Number::from_i64(*s)),
                            Constant::Number(Number::from_i64(idx as i64)),
                        ])
                        .unwrap();
                    }
                }
                ColumnVector::Floats(v) => {
                    let fact = db
                        .add_new_intensional_relation(
                            table_pred.clone(),
                            vec![Attribute::typed(AttributeKind::Float), Attribute::integer()],
                        )
                        .unwrap();

                    for (idx, s) in v.v.iter().enumerate() {
                        fact.add_as_fact(vec![
                            Constant::Number(Number::from_f64(*s).unwrap()),
                            Constant::Number(Number::from_i64(idx as i64)),
                        ])
                        .unwrap();
                    }
                }
                ColumnVector::Bools(v) => {
                    let fact = db
                        .add_new_intensional_relation(
                            table_pred.clone(),
                            vec![
                                Attribute::typed(AttributeKind::Boolean),
                                Attribute::integer(),
                            ],
                        )
                        .unwrap();

                    for (idx, s) in v.v.iter().enumerate() {
                        fact.add_as_fact(vec![
                            Constant::Boolean(*s),
                            Constant::Number(Number::from_i64(idx as i64)),
                        ])
                        .unwrap();
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(feature = "datalog")]
fn run_datalog_proofs(
    res: &mut AllData,
    so: &SourceOutputs,
) -> Result<(), DatabaseValidationError> {
    use asdi::{
        idb::{query::Query, Atom, RuleForm},
        Labeled,
    };

    if so.datalog_proofs().is_empty() {
        return Ok(());
    }

    let mut db = res.datalog_db.lock().unwrap();

    for (idx, proof) in so.datalog_proofs().iter().enumerate() {
        let dbi = DBIdentifier::new(proof.output_table_name.as_str())?;
        let target_table = res.find_table_named_idx(&dbi);
        if target_table.is_empty() {
            return Err(DatabaseValidationError::DatalogProofTableNotFound {
                table_name: proof.output_table_name.as_str().to_string(),
                comment: proof.comment.clone(),
                proof_expression: proof.expression.clone(),
            });
        }
        assert_eq!(target_table.len(), 1);
        let target_table = target_table[0];

        let pre_extension_rules_count = db.rules_ordered().len();

        let offender_rule_header = "OUTPUT(Offender)";
        let outputs_count = proof
            .expression
            .match_indices(offender_rule_header)
            .collect::<Vec<_>>();

        if outputs_count.is_empty() {
            return Err(DatabaseValidationError::DatalogProofOutputRuleNotFound {
                error: format!("Datalog proof must contain output rule for offenders in the format like '{offender_rule_header} :- t_some_table__some_column(ColVal, Offender), ColVal = 7.'"),
                table_name: proof.output_table_name.as_str().to_string(),
                comment: proof.comment.clone(),
                proof_expression: proof.expression.clone(),
            });
        }

        if outputs_count.len() > 1 {
            return Err(DatabaseValidationError::DatalogProofTooManyOutputRules {
                error: format!(
                    "Only one '{offender_rule_header}' rule may exist in a datalog proof."
                ),
                table_name: proof.output_table_name.as_str().to_string(),
                comment: proof.comment.clone(),
                proof_expression: proof.expression.clone(),
            });
        }

        let offender_rule_label = format!("datalog_proof_{idx}");
        let offender_rule_full_header = format!("{offender_rule_label}(Offender)");
        let target_table_prefix_name = format!("t_{}__", proof.output_table_name);

        let replaced_rule = proof
            .expression
            .as_str()
            .replace(offender_rule_header, &offender_rule_full_header);

        asdi::parse::parser::extend_program(replaced_rule.as_str(), &mut db).map_err(|e| {
            DatabaseValidationError::DatalogProofQueryParseError {
                error: e.to_string(),
                table_name: proof.output_table_name.as_str().to_string(),
                comment: proof.comment.clone(),
                proof_expression: proof.expression.clone(),
            }
        })?;

        let post_extension_rules_count = db.rules_ordered().len();

        let rules_diff = post_extension_rules_count - pre_extension_rules_count;

        if rules_diff == 0 {
            return Err(DatabaseValidationError::DatalogProofNoRulesFound {
               error: "No datalog queries found in proof. There must exist one OUTPUT(Offender) rule in a proof.".to_string(),
               table_name: proof.output_table_name.as_str().to_string(),
               comment: proof.comment.clone(),
               proof_expression: proof.expression.clone(),
           });
        }

        let rules_post_parse = db.rules_ordered();
        let new_rule_slice =
            &rules_post_parse[pre_extension_rules_count..post_extension_rules_count];

        let mut the_query: Vec<asdi::idb::query::Query> = Vec::with_capacity(1);
        for nr in new_rule_slice {
            let head = nr.head().collect::<Vec<_>>();
            if head.len() == 1 {
                let label = head[0].label().to_string();
                if label == offender_rule_label {
                    assert!(the_query.is_empty());

                    let vars = head[0].variables().collect::<Vec<_>>();
                    if nr.form() != RuleForm::Pure
                        || vars.len() != 1
                        || vars[0].to_string() != "Offender"
                    {
                        // probably no one will ever get here but who knows?
                        return Err(DatabaseValidationError::DatalogProofBadOutputRuleFormat {
                            error: "There must be only one variable in output rule named Offender."
                                .to_string(),
                            table_name: proof.output_table_name.as_str().to_string(),
                            comment: proof.comment.clone(),
                            proof_expression: proof.expression.clone(),
                        });
                    }

                    // TODO: iterate body and find our wanted table in needed format

                    let mut found_expected_table = false;
                    for lit in nr.literals() {
                        match lit.as_ref() {
                            asdi::idb::LiteralInner::Relational(r) => {
                                let t_name = r.label().to_string();
                                if t_name.starts_with(&target_table_prefix_name) {
                                    // everything must be in form `t_<table_name>__<column_name>(Value, Index)
                                    let terms = r.terms();
                                    assert_eq!(terms.len(), 2);
                                    if terms[1].is_variable() {
                                        let vname = terms[1].as_variable().unwrap().to_string();
                                        if vname == "Offender" {
                                            found_expected_table = true;
                                        }
                                    }
                                }
                            }
                            asdi::idb::LiteralInner::Arithmetic(_) => {}
                        }
                    }

                    if !found_expected_table {
                        return Err(DatabaseValidationError::DatalogProofTableExpectedNotFoundInTheOutputQuery {
                            error: format!("Expected term like 't_{}__<column name>(_, Offender)' not found in the output query", proof.output_table_name),
                            table_name: proof.output_table_name.as_str().to_string(),
                            comment: proof.comment.clone(),
                            proof_expression: proof.expression.clone(),
                        });
                    }

                    let atom = Atom::new(
                        head[0].label_ref(),
                        vec![asdi::idb::Term::Variable(
                            db.variables().fetch("Offender").unwrap(),
                        )],
                    );
                    let query = Query::from(atom);
                    the_query.push(query.clone());
                }
            }
        }

        // at this point we either have found the query or we have buggy reporting to user
        assert_eq!(the_query.len(), 1);

        let evaluator = asdi::idb::eval::NaiveEvaluator {};

        let the_query = &the_query[0];
        let view = db.eval_query_with(the_query, evaluator).map_err(|e| {
            DatabaseValidationError::DatalogProofQueryingFailure {
                error: e.to_string(),
                table_name: proof.output_table_name.as_str().to_string(),
                comment: proof.comment.clone(),
                proof_expression: proof.expression.clone(),
            }
        })?;

        let mut offenders = vec![];
        match view {
            Some(view) => {
                let rows = asdi::Collection::iter(&view).collect::<Vec<_>>();
                for i in &rows {
                    assert_eq!(i.values().len(), 1);
                    match &i.values()[0] {
                        asdi::edb::Constant::Number(n) => {
                            let idx = *n.as_integer().unwrap();
                            let output = res.tables[target_table]
                                .row_as_pretty_json(idx as usize)
                                .unwrap();
                            offenders.push(output);
                        }
                        _ => {
                            panic!("Only number should appear here as data is generated.")
                        }
                    }
                }
            }
            None => {
                panic!(
                    "Should never be reached, we know this relation exists and parser validated."
                )
            }
        }

        if !offenders.is_empty() {
            offenders.sort();
            return Err(DatabaseValidationError::DatalogProofOffendersFound {
                table_name: proof.output_table_name.as_str().to_string(),
                comment: proof.comment.clone(),
                proof_expression: proof.expression.clone(),
                offending_columns: offenders,
            });
        }
    }

    Ok(())
}

fn compute_materialized_views(res: &mut AllData) -> Result<(), DatabaseValidationError> {
    let no_mat_views = !res.tables.iter().any(|i| i.mat_view_expression.is_some());
    if no_mat_views {
        return Ok(());
    }

    let conn_ro = res.sqlite_db.ro.lock().unwrap();
    let mut conn_rw = res.sqlite_db.rw.lock().unwrap();

    for mview in &mut res.tables {
        if let Some(expr) = &mview.mat_view_expression {
            let mut stmt = conn_ro.prepare(expr.as_str()).map_err(|e| {
                DatabaseValidationError::SqlMatViewStatementPrepareException {
                    table_name: mview.name.as_str().to_string(),
                    sql_expression: expr.clone(),
                    error: e.to_string(),
                }
            })?;

            let column_count = stmt.column_count();
            if column_count != mview.columns.len() {
                return Err(DatabaseValidationError::SqlMatViewWrongColumnCount {
                    table_name: mview.name.as_str().to_string(),
                    sql_expression: expr.clone(),
                    expected_columns: mview.columns.len(),
                    actual_columns: column_count,
                });
            }

            let mut rows = stmt.query(rusqlite::params![]).map_err(|e| {
                DatabaseValidationError::SqlMatViewStatementInitException {
                    table_name: mview.name.as_str().to_string(),
                    sql_expression: expr.clone(),
                    error: e.to_string(),
                }
            })?;

            let mut outputs: Vec<Vec<String>> = Vec::new();
            while let Some(output) = rows.next().map_err(|e| {
                DatabaseValidationError::SqlMatViewStatementQueryException {
                    table_name: mview.name.as_str().to_string(),
                    sql_expression: expr.clone(),
                    error: e.to_string(),
                }
            })? {
                let mut this_row = Vec::with_capacity(column_count);
                for column in 0..column_count {
                    let ctype = mview.columns[column].data.column_type();
                    match output.get_ref_unwrap(column) {
                        rusqlite::types::ValueRef::Null => {
                            return Err(DatabaseValidationError::SqlMatViewNullReturnsUnsupported {
                                table_name: mview.name.as_str().to_string(),
                                sql_expression: expr.clone(),
                                column_name: mview.columns[column].column_name.as_str().to_string(),
                                return_row_index: outputs.len() + 1,
                            })
                        }
                        rusqlite::types::ValueRef::Integer(i) => {
                            let accepted_types =
                                [DBType::Int, DBType::Float, DBType::Text, DBType::Bool];
                            if !accepted_types.contains(&ctype) {
                                return Err(
                                    DatabaseValidationError::SqlMatViewWrongColumnTypeReturned {
                                        table_name: mview.name.as_str().to_string(),
                                        sql_expression: expr.clone(),
                                        column_name: mview.columns[column]
                                            .column_name
                                            .as_str()
                                            .to_string(),
                                        return_row_index: outputs.len() + 1,
                                        actual_column_type: "INT".to_string(),
                                        expected_column_type: ctype,
                                    },
                                );
                            }
                            this_row.push(i.to_string())
                        }
                        rusqlite::types::ValueRef::Real(i) => {
                            let accepted_types = [DBType::Float, DBType::Text];
                            if !accepted_types.contains(&ctype) {
                                return Err(
                                    DatabaseValidationError::SqlMatViewWrongColumnTypeReturned {
                                        table_name: mview.name.as_str().to_string(),
                                        sql_expression: expr.clone(),
                                        column_name: mview.columns[column]
                                            .column_name
                                            .as_str()
                                            .to_string(),
                                        return_row_index: outputs.len() + 1,
                                        actual_column_type: "FLOAT".to_string(),
                                        expected_column_type: ctype,
                                    },
                                );
                            }
                            this_row.push(i.to_string())
                        }
                        rusqlite::types::ValueRef::Text(i) => {
                            let accepted_types = [DBType::Text];
                            if !accepted_types.contains(&ctype) {
                                return Err(
                                    DatabaseValidationError::SqlMatViewWrongColumnTypeReturned {
                                        table_name: mview.name.as_str().to_string(),
                                        sql_expression: expr.clone(),
                                        column_name: mview.columns[column]
                                            .column_name
                                            .as_str()
                                            .to_string(),
                                        return_row_index: outputs.len() + 1,
                                        actual_column_type: "TEXT".to_string(),
                                        expected_column_type: mview.columns[column]
                                            .data
                                            .column_type(),
                                    },
                                );
                            }
                            this_row.push(String::from_utf8(i.to_vec()).unwrap())
                        }
                        rusqlite::types::ValueRef::Blob(_) => {
                            panic!("Binary blobs of sqlite are not supported");
                        }
                    }
                }
                outputs.push(this_row);
            }

            let output_rows_count = outputs.len();
            outputs.sort();

            drop(rows);
            drop(stmt);

            // we assume mat view is empty now
            assert_eq!(mview.len(), 0);

            for column in 0..column_count {
                match &mut mview.columns[column].data {
                    ColumnVector::Strings(v) => {
                        v.v.reserve_exact(output_rows_count);
                        for row in 0..output_rows_count {
                            v.v.push(outputs[row][column].clone());
                        }
                    }
                    ColumnVector::Ints(v) => {
                        v.v.reserve_exact(output_rows_count);
                        for row in 0..output_rows_count {
                            v.v.push(outputs[row][column].parse::<i64>().unwrap());
                        }
                    }
                    ColumnVector::Floats(v) => {
                        v.v.reserve_exact(output_rows_count);
                        for row in 0..output_rows_count {
                            v.v.push(outputs[row][column].parse::<f64>().unwrap());
                        }
                    }
                    ColumnVector::Bools(v) => {
                        v.v.reserve_exact(output_rows_count);
                        for row in 0..output_rows_count {
                            // in sqlite 1 is true and 0 is false
                            let to_insert = match outputs[row][column].as_str() {
                                "1" => true,
                                "0" => false,
                                v => {
                                    panic!(
                                        "Unexpected sqlite value returned when wanting a bool: {}",
                                        v
                                    )
                                }
                            };
                            v.v.push(to_insert);
                        }
                    }
                }
            }

            // other mat views may depend on this mat view
            insert_sqlite_data(mview, &mut conn_rw)?;
        }
    }

    Ok(())
}

fn create_sqlite_table(
    table: &DataTable,
    conn: &rusqlite::Connection,
) -> Result<(), DatabaseValidationError> {
    let mut create_stmt = String::with_capacity(128);

    create_stmt += "CREATE TABLE ";
    create_stmt += table.name.as_str();
    create_stmt += " (\n";
    for (idx, column) in table.columns.iter().enumerate() {
        let is_last = idx == table.columns.len() - 1;
        create_stmt += "  ";
        create_stmt += column.column_name.as_str();
        create_stmt += "  ";
        create_stmt += column.sqlite_type_name();
        create_stmt += " NOT NULL";
        if !is_last {
            create_stmt += ","
        }
        create_stmt += "\n";
    }
    create_stmt += ") STRICT;\n";

    // it is our internal bug if faulures are here
    let _ = conn
        .execute(create_stmt.as_str(), rusqlite::params![])
        .unwrap();

    Ok(())
}

fn insert_sqlite_data(
    table: &DataTable,
    conn: &mut rusqlite::Connection,
) -> Result<(), DatabaseValidationError> {
    let mut insert_stmt = String::with_capacity(128);

    insert_stmt += "INSERT INTO ";
    insert_stmt += table.name.as_str();
    insert_stmt += " (";
    insert_stmt += table
        .columns
        .iter()
        .map(|i| i.column_name.as_str().to_string())
        .collect::<Vec<_>>()
        .join(", ")
        .as_str();
    insert_stmt += ")\nVALUES ";
    insert_stmt += "(";
    insert_stmt += table
        .columns
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ")
        .as_str();
    insert_stmt += ");";

    let tx = conn.transaction();
    assert!(tx.is_ok(), "sqlite internal error");
    let tx = tx.unwrap();
    let stmt = tx.prepare(&insert_stmt);
    assert!(stmt.is_ok(), "sqlite internal statement error");
    let mut stmt = stmt.unwrap();

    let mut dyn_columns = Vec::with_capacity(table.columns.len());
    for column in &table.columns {
        let mut mapped_col = Vec::with_capacity(table.len());
        match &column.data {
            ColumnVector::Strings(v) => {
                for i in &v.v {
                    mapped_col.push(i as &dyn rusqlite::ToSql);
                }
            }
            ColumnVector::Ints(v) => {
                for i in &v.v {
                    mapped_col.push(i as &dyn rusqlite::ToSql);
                }
            }
            ColumnVector::Floats(v) => {
                for i in &v.v {
                    mapped_col.push(i as &dyn rusqlite::ToSql);
                }
            }
            ColumnVector::Bools(v) => {
                for i in &v.v {
                    mapped_col.push(i as &dyn rusqlite::ToSql);
                }
            }
        }
        dyn_columns.push(mapped_col);
    }

    for row in 0..table.len() {
        let mut pvalues = Vec::with_capacity(table.columns.len());
        for column_idx in 0..table.columns.len() {
            pvalues.push(dyn_columns[column_idx][row]);
        }
        let res = stmt.execute(&*pvalues);
        assert!(
            res.is_ok(),
            "sqlite internal statement insert execution error"
        );
    }

    drop(stmt);
    let res = tx.commit();
    assert!(res.is_ok(), "sqlite internal statement commit error");

    Ok(())
}

fn create_sqlite_indexes(
    table: &DataTable,
    conn: &rusqlite::Connection,
) -> Result<(), DatabaseValidationError> {
    for column in &table.columns {
        let mut index_stmt = String::with_capacity(128);

        index_stmt += "CREATE INDEX ";
        index_stmt += table.name.as_str();
        index_stmt += "_index_";
        index_stmt += column.column_name.as_str();
        index_stmt += " ON ";
        index_stmt += table.name.as_str();
        index_stmt += " (";
        index_stmt += column.column_name.as_str();
        index_stmt += ");";
        // it is our internal bug if faulures are here
        let _ = conn
            .execute(index_stmt.as_str(), rusqlite::params![])
            .unwrap();
    }

    Ok(())
}

fn maybe_insert_sqlite_data(
    res: &mut AllData,
    so: &SourceOutputs,
    sqlite_needed: bool,
) -> Result<(), DatabaseValidationError> {
    let sqlite_needed = sqlite_needed
        || !so.sql_proofs().is_empty()
        || res.tables.iter().any(|i| i.mat_view_expression.is_some());

    if !sqlite_needed {
        return Ok(());
    }

    let mut conn = res.sqlite_db.rw.lock().unwrap();
    // create all tables
    for table in &res.tables {
        create_sqlite_table(table, &conn)?;
    }

    // insert all data
    for table in &res.tables {
        insert_sqlite_data(table, &mut conn)?;
    }

    // create all indexes
    for table in &res.tables {
        create_sqlite_indexes(table, &conn)?;
    }

    Ok(())
}

fn run_sqlite_proofs(res: &mut AllData, so: &SourceOutputs) -> Result<(), DatabaseValidationError> {
    if so.sql_proofs().is_empty() {
        return Ok(());
    }

    for proof in so.sql_proofs() {
        let dbi = DBIdentifier::new(proof.output_table_name.as_str())?;
        let tbl = res.find_table_named_idx(&dbi);

        if tbl.is_empty() {
            return Err(DatabaseValidationError::SqlProofTableNotFound {
                table_name: proof.output_table_name.clone(),
                proof_expression: proof.expression.clone(),
                comment: proof.comment.clone(),
            });
        }
    }

    let conn = res.sqlite_db.ro.lock().unwrap();

    for proof in so.sql_proofs() {
        let mut stmt = conn.prepare(proof.expression.as_str()).map_err(|e| {
            DatabaseValidationError::SqlProofQueryPlanningError {
                error: e.to_string(),
                table_name: proof.output_table_name.clone(),
                proof_expression: proof.expression.clone(),
                comment: proof.comment.clone(),
            }
        })?;

        let column_names = stmt.column_names();
        let column_count = column_names.len();
        if column_names.len() != 1 {
            return Err(
                DatabaseValidationError::SqlProofQueryErrorSingleRowIdColumnExpected {
                    error: format!("Required output column count is 1, got {column_count}"),
                    table_name: proof.output_table_name.clone(),
                    proof_expression: proof.expression.clone(),
                    comment: proof.comment.clone(),
                },
            );
        }

        if column_names[0] != "rowid" {
            return Err(
                DatabaseValidationError::SqlProofQueryErrorSingleRowIdColumnExpected {
                    error: format!(
                        "Required output column name must be rowid, got {}",
                        column_names[0]
                    ),
                    table_name: proof.output_table_name.clone(),
                    proof_expression: proof.expression.clone(),
                    comment: proof.comment.clone(),
                },
            );
        }

        // I kinda wish rusqlite exposed this :(
        let (query_tname, query_colname) = unsafe {
            let raw_stmt = stmt.raw_stmt();
            let raw_ptr = raw_stmt.ptr();
            let tname_raw = sqlite3_column_table_name(raw_ptr, 0);
            let cname_raw = sqlite3_column_origin_name(raw_ptr, 0);

            let tname = if tname_raw.is_null() {
                "NULL".to_string()
            } else {
                std::ffi::CStr::from_ptr(tname_raw)
                    .to_string_lossy()
                    .to_string()
            };
            let colname = if cname_raw.is_null() {
                "NULL".to_string()
            } else {
                std::ffi::CStr::from_ptr(cname_raw)
                    .to_string_lossy()
                    .to_string()
            };

            (tname, colname)
        };

        if query_tname != proof.output_table_name || query_colname != "rowid" {
            return Err(
                DatabaseValidationError::SqlProofQueryColumnOriginMismatchesExpected {
                    error: "Actual column origin table name or origin mistmaches expectations"
                        .to_string(),
                    expected_column_origin_table: proof.output_table_name.to_string(),
                    expected_column_origin_name: "rowid".to_string(),
                    actual_column_origin_table: query_tname,
                    actual_column_origin_name: query_colname,
                    proof_expression: proof.expression.to_string(),
                    comment: proof.comment.clone(),
                },
            );
        }

        let mut offenders_res =
            stmt.query([])
                .map_err(|e| DatabaseValidationError::SqlProofQueryError {
                    error: e.to_string(),
                    table_name: proof.output_table_name.clone(),
                    proof_expression: proof.expression.clone(),
                    comment: proof.comment.clone(),
                })?;

        let mut offenders: Vec<usize> = Vec::new();
        while let Some(offender) =
            offenders_res
                .next()
                .map_err(|e| DatabaseValidationError::SqlProofQueryError {
                    error: e.to_string(),
                    table_name: proof.output_table_name.clone(),
                    proof_expression: proof.expression.clone(),
                    comment: proof.comment.clone(),
                })?
        {
            let res = offender.get::<usize, usize>(0).unwrap();
            // rowid values are 1 based
            offenders.push(res - 1);
        }

        if !offenders.is_empty() {
            let dbi = DBIdentifier::new(proof.output_table_name.as_str())?;
            let tbl = &res.tables[res.find_table_named_idx(&dbi)[0]];
            let offenders_mapped = offenders
                .into_iter()
                .map(|o| tbl.row_as_pretty_json(o).unwrap())
                .collect::<Vec<_>>();

            return Err(DatabaseValidationError::SqlProofOffendersFound {
                table_name: proof.output_table_name.clone(),
                proof_expression: proof.expression.clone(),
                offending_columns: offenders_mapped,
                comment: proof.comment.clone(),
            });
        }
    }

    Ok(())
}

fn preprocess_lua_expression(inp_exp: &str) -> String {
    // if there are return keywords assume returns are explicit
    if inp_exp.starts_with("return ") || inp_exp.contains("return ") {
        return inp_exp.to_string();
    }

    // filter whitespace lines
    let mut lines = inp_exp
        .lines()
        .filter_map(|i| {
            let trimmed = i.trim();
            if !trimmed.is_empty() {
                Some(trimmed.to_string())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // final statement returns otherwise
    if !lines.is_empty() {
        let last_line = lines.len() - 1;
        lines[last_line] = format!("return {}", lines[last_line]);
        lines.join("\n")
    } else {
        inp_exp.to_string()
    }
}

fn compute_generated_columns(res: &mut AllData) -> Result<(), DatabaseValidationError> {
    for table in &mut res.tables {
        let gen_expr_count = table
            .columns
            .iter()
            .filter(|i| i.generate_expression.is_some())
            .count();
        if gen_expr_count == 0 {
            // avoid costly lua init if user doesn't use lua
            continue;
        }

        let lua = res.lua_runtime.lock().unwrap();

        let mut lua_data_vectors: Vec<(String, Vec<(mlua::Value, String)>)> =
            Vec::with_capacity(table.len());
        // first, push all lua non generated values
        for column in table.columns.iter_mut() {
            let map_lua_err =
                |e: mlua::Error| DatabaseValidationError::LuaColumnGenerationExpressionLoadError {
                    error: e.to_string(),
                    table_name: table.name.as_str().to_string(),
                    column_name: column.column_name.as_str().to_string(),
                    expression: "*loading to lua context*".to_string(),
                };

            if column.generate_expression.is_none() {
                let column_name = column.column_name.as_str().to_string();
                let mut values_vec: Vec<(mlua::Value, String)> =
                    Vec::with_capacity(column.data.len());
                match &column.data {
                    ColumnVector::Strings(v) => {
                        for i in &v.v {
                            let lua_str = lua.create_string(i.as_bytes()).map_err(map_lua_err)?;
                            values_vec.push((mlua::Value::String(lua_str), i.clone()));
                        }
                    }
                    ColumnVector::Ints(v) => {
                        for i in &v.v {
                            values_vec.push((mlua::Value::Integer(*i), format!("{i}")));
                        }
                    }
                    ColumnVector::Floats(v) => {
                        for i in &v.v {
                            values_vec.push((mlua::Value::Number(*i), format!("{i}")));
                        }
                    }
                    ColumnVector::Bools(v) => {
                        for i in &v.v {
                            values_vec.push((mlua::Value::Boolean(*i), format!("{i}")));
                        }
                    }
                }

                lua_data_vectors.push((column_name, values_vec));
            }
        }

        for column in table.columns.iter_mut() {
            match &column.generate_expression {
                Some(gen_expr) => {
                    let map_lua_err = |e: mlua::Error| {
                        DatabaseValidationError::LuaColumnGenerationExpressionLoadError {
                            error: e.to_string(),
                            table_name: table.name.as_str().to_string(),
                            column_name: column.column_name.as_str().to_string(),
                            expression: gen_expr.clone(),
                        }
                    };

                    let source_to_load = preprocess_lua_expression(gen_expr);
                    let lua_expr = lua
                        .load(&source_to_load)
                        .into_function()
                        .and_then(|f| lua.create_registry_value(f))
                        .map_err(map_lua_err)?;
                    let fv = lua
                        .registry_value::<mlua::Function>(&lua_expr)
                        .map_err(map_lua_err)?;

                    match &mut column.data {
                        ColumnVector::Strings(v) => {
                            let map_func = |inp_v: &mlua::Value| match inp_v {
                                mlua::Value::String(s) => Some(s.to_string_lossy().to_string()),
                                _ => None,
                            };

                            compute_lua_vector_value(
                                &lua,
                                &fv,
                                v,
                                &mut lua_data_vectors,
                                map_func,
                                table.name.as_str(),
                                column.column_name.as_str(),
                                gen_expr.as_str(),
                                "string",
                            )?;
                        }
                        ColumnVector::Ints(v) => {
                            let map_func = |inp_v: &mlua::Value| match inp_v {
                                mlua::Value::Integer(s) => Some(*s),
                                _ => None,
                            };

                            compute_lua_vector_value(
                                &lua,
                                &fv,
                                v,
                                &mut lua_data_vectors,
                                map_func,
                                table.name.as_str(),
                                column.column_name.as_str(),
                                gen_expr.as_str(),
                                "integer",
                            )?;
                        }
                        ColumnVector::Floats(v) => {
                            let map_func = |inp_v: &mlua::Value| match inp_v {
                                mlua::Value::Integer(s) => Some(*s as f64),
                                mlua::Value::Number(s) => Some(*s),
                                _ => None,
                            };

                            compute_lua_vector_value(
                                &lua,
                                &fv,
                                v,
                                &mut lua_data_vectors,
                                map_func,
                                table.name.as_str(),
                                column.column_name.as_str(),
                                gen_expr.as_str(),
                                "number",
                            )?;
                        }
                        ColumnVector::Bools(v) => {
                            let map_func = |inp_v: &mlua::Value| match inp_v {
                                mlua::Value::Boolean(s) => Some(*s),
                                _ => None,
                            };

                            compute_lua_vector_value(
                                &lua,
                                &fv,
                                v,
                                &mut lua_data_vectors,
                                map_func,
                                table.name.as_str(),
                                column.column_name.as_str(),
                                gen_expr.as_str(),
                                "boolean",
                            )?;
                        }
                    }
                }
                None => {}
            }
        }

        for c in &lua_data_vectors {
            lua.globals().set(c.0.as_str(), mlua::Nil).map_err(|e| {
                DatabaseValidationError::LuaColumnGenerationError {
                    table_name: table.name.as_str().to_string(),
                    expression: "*while cleaning set values*".to_string(),
                    error: e.to_string(),
                }
            })?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn compute_lua_vector_value<'lua, T: Clone + std::str::FromStr>(
    lua: &mlua::Lua,
    lua_func: &mlua::Function<'lua>,
    v: &mut ColumnVectorGeneric<T>,
    lua_data_vectors: &mut Vec<(String, Vec<(mlua::Value<'lua>, String)>)>,
    map_right_type: impl Fn(&mlua::Value<'lua>) -> Option<T>,
    table_name: &str,
    column_name: &str,
    expression: &str,
    expected_type: &str,
) -> Result<(), DatabaseValidationError> {
    let mut new_values: Vec<(mlua::Value, String)> = Vec::with_capacity(v.v.len());
    for row_no in 0..v.v.len() {
        for (v_name, the_vec) in lua_data_vectors.iter() {
            lua.globals()
                .set(v_name.as_str(), the_vec[row_no].0.clone())
                .map_err(|e: mlua::Error| {
                    DatabaseValidationError::LuaColumnGenerationExpressionLoadError {
                        error: e.to_string(),
                        table_name: table_name.to_string(),
                        column_name: column_name.to_string(),
                        expression: expression.to_string(),
                    }
                })?;
        }

        let output_value = lua_func
            .call::<mlua::Value, mlua::Value>(mlua::Value::Nil)
            .map_err(|e| {
                let (input_row_fields, input_row_values): (Vec<String>, Vec<String>) =
                    lua_data_vectors
                        .iter()
                        .map(|(c_name, c_value)| (c_name.clone(), c_value[row_no].1.clone()))
                        .unzip();
                DatabaseValidationError::LuaColumnGenerationExpressionComputeError {
                    table_name: table_name.to_string(),
                    column_name: column_name.to_string(),
                    input_row_fields,
                    input_row_values,
                    expression: expression.to_string(),
                    error: e.to_string(),
                }
            })?;

        let res = map_right_type(&output_value);
        match res {
            Some(new_value) => {
                let str_value_to_push = lua_value_to_string(&output_value);
                new_values.push((output_value, str_value_to_push));
                v.v[row_no] = new_value;
            }
            None => {
                let (input_row_fields, input_row_values): (Vec<String>, Vec<String>) =
                    lua_data_vectors
                        .iter()
                        .map(|(c_name, c_value)| (c_name.clone(), c_value[row_no].1.clone()))
                        .unzip();
                return Err(
                    DatabaseValidationError::LuaColumnGenerationExpressionComputeTypeMismatch {
                        table_name: table_name.to_string(),
                        column_name: column_name.to_string(),
                        input_row_fields,
                        input_row_values,
                        expression: expression.to_string(),
                        computed_value: lua_value_to_string(&output_value),
                        error: format!(
                            "Computed column expects lua expression to evaluate to type {}, got {}",
                            expected_type,
                            output_value.type_name()
                        ),
                    },
                );
            }
        }
    }

    lua_data_vectors.push((column_name.to_string(), new_values));

    Ok(())
}

fn ensure_child_foreign_keys_are_restricted(res: &AllData) -> Result<(), DatabaseValidationError> {
    let valid_fkey_case = Regex::new("^[a-zA-Z0-9_-]+$").unwrap();
    for table in &res.tables {
        for column in &table.columns {
            if column.is_snake_case_restricted {
                match &column.data {
                    ColumnVector::Strings(sv) => {
                        for i in &sv.v {
                            if !valid_fkey_case.is_match(i.as_str()) {
                                return Err(
                                    DatabaseValidationError::ForeignChildKeyTableStringMustBeAlphanumeric {
                                        referred_table: table.name.as_str().to_string(),
                                        offending_column: column.column_name.as_str().to_string(),
                                        offending_value: i.clone(),
                                    },
                                );
                            }
                        }
                    }
                    ColumnVector::Ints(iv) => {
                        for i in &iv.v {
                            if *i < 0 {
                                return Err(
                                    DatabaseValidationError::ForeignChildKeyTableIntegerKeyMustBeNonNegative {
                                        referred_table: table.name.as_str().to_string(),
                                        offending_column: column.column_name.as_str().to_string(),
                                        offending_value: *i,
                                    },
                                );
                            }
                        }
                    } // ints must have no minus sign in front of them
                    ColumnVector::Floats(_) => {
                        panic!("floats can never be foreign keys")
                    }
                    ColumnVector::Bools(_) => {
                        panic!("booleans can never be foreign keys")
                    }
                }
            }

            if let Some(ForeignKey {
                foreign_table,
                is_to_foreign_child_table,
                is_to_self_child_table,
                ..
            }) = &column.maybe_foreign_key
            {
                if *is_to_foreign_child_table {
                    let key = ForeignKeyToForeignChildRelationship {
                        referred_table: foreign_table.clone(),
                        referee_table: table.name.clone(),
                        referee_column: column.column_name.clone(),
                    };

                    let v = res
                        .foreign_to_foreign_child_keys_map
                        .get(&key)
                        .expect("We must have inserted this");
                    let expected_segments = v.refereed_columns_by_key.len();

                    // if this is special case that segments are skipped then
                    // it means we want to refer to the child

                    if let ColumnVector::Strings(sv) = &column.data {
                        for i in &sv.v {
                            let mut actual_segments = 0;
                            for spl in i.split("=>") {
                                actual_segments += 1;
                                if spl.trim().len() != spl.len() {
                                    return Err(
                                        DatabaseValidationError::ForeignChildKeyReferrerCannotHaveWhitespaceInSegments {
                                            referrer_table: table.name.as_str().to_string(),
                                            referrer_column: column.column_name.as_str().to_string(),
                                            referee_table: foreign_table.as_str().to_string(),
                                            offending_value: i.clone(),
                                        },
                                    );
                                }
                            }

                            if actual_segments != expected_segments {
                                return Err(
                                    DatabaseValidationError::ForeignChildKeyReferrerHasIncorrectSegmentsInCompositeKey {
                                        referrer_table: table.name.as_str().to_string(),
                                        referrer_column: column.column_name.as_str().to_string(),
                                        referee_table: foreign_table.as_str().to_string(),
                                        expected_segments,
                                        actual_segments,
                                        offending_value: i.clone(),
                                    },
                                );
                            }
                        }
                    } else {
                        panic!("Only strings can be composite keys");
                    }
                } else if *is_to_self_child_table {
                    let key = ForeignKeyToNativeChildRelationship {
                        referred_table: foreign_table.clone(),
                        referee_table: table.name.clone(),
                        referee_column: column.column_name.clone(),
                    };

                    let v = res
                        .foreign_to_native_child_keys_map
                        .get(&key)
                        .expect("We must have inserted this");
                    let expected_segments = v.refereed_columns_by_key.len();

                    if let ColumnVector::Strings(sv) = &column.data {
                        for i in &sv.v {
                            let mut actual_segments = 0;
                            for spl in i.split("=>") {
                                actual_segments += 1;
                                if spl.trim().len() != spl.len() {
                                    return Err(
                                        DatabaseValidationError::ForeignChildKeyReferrerCannotHaveWhitespaceInSegments {
                                            referrer_table: table.name.as_str().to_string(),
                                            referrer_column: column.column_name.as_str().to_string(),
                                            referee_table: foreign_table.as_str().to_string(),
                                            offending_value: i.clone(),
                                        },
                                    );
                                }
                            }

                            if actual_segments != expected_segments {
                                return Err(
                                    DatabaseValidationError::ForeignChildKeyReferrerHasIncorrectSegmentsInCompositeKey {
                                        referrer_table: table.name.as_str().to_string(),
                                        referrer_column: column.column_name.as_str().to_string(),
                                        referee_table: foreign_table.as_str().to_string(),
                                        expected_segments,
                                        actual_segments,
                                        offending_value: i.clone(),
                                    },
                                );
                            }
                        }
                    } else {
                        panic!("Only strings can be composite keys");
                    }
                }
            }
        }
    }

    Ok(())
}

fn ensure_no_nan_or_infinity_floats(res: &AllData) -> Result<(), DatabaseValidationError> {
    for table in &res.tables {
        for column in &table.columns {
            match &column.data {
                ColumnVector::Floats(v) => {
                    for (idx, f) in v.v.iter().enumerate() {
                        if f.is_infinite() || f.is_nan() {
                            return Err(
                                DatabaseValidationError::NanOrInfiniteFloatNumbersAreNotAllowed {
                                    table_name: table.name.as_str().to_string(),
                                    column_name: column.column_name.as_str().to_string(),
                                    column_value: f.to_string(),
                                    row_index: idx + 1,
                                },
                            );
                        }
                    }
                }
                ColumnVector::Strings(_) => {}
                ColumnVector::Ints(_) => {}
                ColumnVector::Bools(_) => {}
            }
        }
    }

    Ok(())
}

fn lua_value_to_string(lv: &mlua::Value) -> String {
    match lv {
        mlua::Value::Nil => "nil".to_string(),
        mlua::Value::Boolean(b) => format!("{b}"),
        mlua::Value::LightUserData(_) => "*light user data*".to_string(),
        mlua::Value::Integer(i) => format!("{i}"),
        mlua::Value::Number(n) => format!("{n}"),
        mlua::Value::String(s) => s.to_string_lossy().to_string(),
        mlua::Value::Table(_) => "*lua table*".to_string(),
        mlua::Value::Function(_) => "*lua function*".to_string(),
        mlua::Value::Thread(_) => "*lua thread*".to_string(),
        mlua::Value::UserData(_) => "*user data*".to_string(),
        mlua::Value::Error(e) => format!("lua error: {}", e),
    }
}

fn lua_value_to_string_descriptive(lv: &mlua::Value) -> String {
    match lv {
        mlua::Value::Nil => "nil".to_string(),
        mlua::Value::Boolean(b) => format!("{b}"),
        mlua::Value::LightUserData(_) => "*light user data*".to_string(),
        mlua::Value::Integer(i) => format!("integer {i}"),
        mlua::Value::Number(n) => format!("number {n}"),
        mlua::Value::String(s) => format!("string \"{}\"", s.to_string_lossy()),
        mlua::Value::Table(_) => "*lua table*".to_string(),
        mlua::Value::Function(_) => "*lua function*".to_string(),
        mlua::Value::Thread(_) => "*lua thread*".to_string(),
        mlua::Value::UserData(_) => "*user data*".to_string(),
        mlua::Value::Error(e) => format!("lua error: {}", e),
    }
}

fn ensure_row_checks(res: &AllData) -> Result<(), DatabaseValidationError> {
    for table in &res.tables {
        if table.row_checks.is_empty() {
            continue;
        }

        // only init lua runtime if we know we have checks
        let lua = res.lua_runtime.lock().unwrap();

        let column_names = table
            .columns
            .iter()
            .map(|i| i.column_name.as_str().to_string())
            .collect::<Vec<_>>();
        let mut exprs: Vec<(Function, &str)> = Vec::with_capacity(table.row_checks.len());
        for row_lua_checks in &table.row_checks {
            // TODO: workaround by adding return keyword, find more elegant solution?
            let map_lua_err =
                |e: mlua::Error| DatabaseValidationError::LuaCheckExpressionLoadError {
                    error: e.to_string(),
                    table_name: table.name.as_str().to_string(),
                    expression: row_lua_checks.expression.to_string(),
                };
            let source_to_load = preprocess_lua_expression(&row_lua_checks.expression);
            let lua_expr = lua
                .load(&source_to_load)
                .into_function()
                .and_then(|f| lua.create_registry_value(f))
                .map_err(map_lua_err)?;
            let fv = lua
                .registry_value::<mlua::Function>(&lua_expr)
                .map_err(map_lua_err)?;
            exprs.push((fv, row_lua_checks.expression.as_str()));
        }

        let map_lua_err = |e: mlua::Error| DatabaseValidationError::LuaCheckExpressionLoadError {
            error: e.to_string(),
            table_name: table.name.as_str().to_string(),
            expression: "*setting keys in lua context*".to_string(),
        };

        for row_no in 0..table.len() {
            let mut row_values = Vec::with_capacity(table.columns.len());
            for c in &table.columns {
                set_column_value_in_lua_runtime(&lua, c, row_no, &mut row_values)
                    .map_err(map_lua_err)?
            }

            for (fv, expression) in &exprs {
                let res = fv.call::<mlua::Value, mlua::Value>(mlua::Value::Nil);
                match res {
                    Ok(mlua::Value::Boolean(v)) => {
                        if !v {
                            return Err(DatabaseValidationError::LuaCheckEvaluationFailed {
                                table_name: table.name.as_str().to_string(),
                                expression: expression.to_string(),
                                row_values,
                                column_names,
                                error: "Expression check for the row didn't pass.".to_string(),
                            });
                        }
                    }
                    Ok(v) => {
                        return Err(
                            DatabaseValidationError::LuaCheckEvaluationErrorUnexpectedReturnType {
                                table_name: table.name.as_str().to_string(),
                                expression: expression.to_string(),
                                row_values,
                                column_names,
                                error: format!(
                                    "Unexpected expression return value, expected boolean, got {}",
                                    v.type_name()
                                ),
                            },
                        )
                    }
                    Err(e) => {
                        return Err(DatabaseValidationError::LuaCheckEvaluationError {
                            table_name: table.name.as_str().to_string(),
                            expression: expression.to_string(),
                            row_values,
                            column_names,
                            error: e.to_string(),
                        })
                    }
                }
            }
        }

        // cleanup after ourselves
        for c in &table.columns {
            lua.globals()
                .set(c.column_name.as_str(), mlua::Nil)
                .map_err(map_lua_err)?;
        }
    }

    Ok(())
}

fn set_column_value_in_lua_runtime(
    lua: &mlua::Lua,
    column: &DataColumn,
    row_no: usize,
    vdump_vec: &mut Vec<String>,
) -> Result<(), mlua::Error> {
    match &column.data {
        ColumnVector::Strings(v) => {
            vdump_vec.push(v.v[row_no].clone());
            let lua_str = lua.create_string(v.v[row_no].as_bytes())?;
            lua.globals().set(column.column_name.as_str(), lua_str)?;
        }
        ColumnVector::Ints(v) => {
            vdump_vec.push(v.v[row_no].to_string());
            lua.globals()
                .set(column.column_name.as_str(), v.v[row_no])?;
        }
        ColumnVector::Floats(v) => {
            vdump_vec.push(v.v[row_no].to_string());
            lua.globals()
                .set(column.column_name.as_str(), v.v[row_no])?;
        }
        ColumnVector::Bools(v) => {
            vdump_vec.push(v.v[row_no].to_string());
            lua.globals()
                .set(column.column_name.as_str(), v.v[row_no])?;
        }
    }

    Ok(())
}

fn ensure_child_primary_keys_unique_per_table_and_fkeys_exist(
    res: &mut AllData,
) -> Result<(), DatabaseValidationError> {
    for t in &res.tables {
        if let Some(pk) = t.primary_key_column() {
            if let KeyType::ChildPrimary { .. } = &pk.key_type {
                for fk_table in &res.tables {
                    if fk_table.name != t.name {
                        for fk_column in &fk_table.columns {
                            if let Some(ForeignKey {
                                foreign_table,
                                is_to_foreign_child_table,
                                is_to_self_child_table,
                                ..
                            }) = &fk_column.maybe_foreign_key
                            {
                                if foreign_table.as_str() == t.name.as_str() {
                                    if !is_to_foreign_child_table && !is_to_self_child_table {
                                        // bingo, found it, initialize the child vec if not yet done
                                        let mut parent_table_names = Vec::new();
                                        let mut parent_table_colums = Vec::new();
                                        let mut child_uniq_vecs: Vec<Vec<String>> = Vec::new();
                                        let mut child_vec_map: HashMap<
                                            Vec<String>,
                                            HashMap<String, usize>,
                                        > = HashMap::new();
                                        child_uniq_vecs.reserve_exact(t.len());

                                        for _ in 0..t.len() {
                                            child_uniq_vecs.push(Vec::new());
                                        }

                                        // iterate ancestors and as long as common keep appending
                                        for child_col_idx in 0..std::cmp::min(
                                            t.columns.len(),
                                            fk_table.columns.len(),
                                        ) {
                                            match (
                                                &t.columns[child_col_idx].key_type,
                                                &fk_table.columns[child_col_idx].key_type,
                                            ) {
                                                (
                                                    KeyType::ParentPrimary {
                                                        parent_table: referred_table,
                                                    },
                                                    KeyType::ParentPrimary {
                                                        parent_table: referee_table,
                                                    },
                                                ) => {
                                                    if referred_table == referee_table {
                                                        parent_table_names
                                                            .push(referred_table.clone());
                                                        parent_table_colums.push(
                                                            t.columns[child_col_idx]
                                                                .column_name
                                                                .clone(),
                                                        );
                                                        match &t.columns[child_col_idx].data {
                                                            ColumnVector::Strings(v) => {
                                                                for row in 0..t.len() {
                                                                    child_uniq_vecs[row]
                                                                        .push(v.v[row].clone());
                                                                }
                                                            }
                                                            ColumnVector::Ints(v) => {
                                                                for row in 0..t.len() {
                                                                    child_uniq_vecs[row]
                                                                        .push(v.v[row].to_string());
                                                                }
                                                            }
                                                            ColumnVector::Floats(_) => panic!(
                                                                "Floats cannot be primary keys"
                                                            ),
                                                            ColumnVector::Bools(_) => panic!(
                                                                "Bools cannot be primary keys"
                                                            ),
                                                        }
                                                    } else {
                                                        break;
                                                    }
                                                }
                                                _ => break,
                                            }
                                        }

                                        match &pk.data {
                                            ColumnVector::Strings(v) => {
                                                for row in 0..t.len() {
                                                    let e = child_vec_map
                                                        .entry(child_uniq_vecs[row].clone())
                                                        .or_default();
                                                    let res = e.insert(v.v[row].clone(), row);
                                                    assert!(res.is_none(), "We assume all child primary keys already checked for uniqueness");
                                                }
                                            }
                                            ColumnVector::Ints(v) => {
                                                for row in 0..t.len() {
                                                    let e = child_vec_map
                                                        .entry(child_uniq_vecs[row].clone())
                                                        .or_default();
                                                    let res = e.insert(v.v[row].to_string(), row);
                                                    assert!(res.is_none(), "We assume all child primary keys already checked for uniqueness");
                                                }
                                            }
                                            ColumnVector::Floats(_) => {
                                                panic!("Floats cannot be primary keys")
                                            }
                                            ColumnVector::Bools(_) => {
                                                panic!("Bools cannot be primary keys")
                                            }
                                        }

                                        let mut referee_uniq_context: Vec<Vec<String>> =
                                            Vec::with_capacity(fk_table.len());
                                        for _ in 0..fk_table.len() {
                                            referee_uniq_context.push(Vec::new());
                                        }

                                        assert!(!parent_table_names.is_empty());
                                        for ptable in &parent_table_names {
                                            let expected_key = KeyType::ParentPrimary {
                                                parent_table: ptable.clone(),
                                            };
                                            for fk_parent_col in &fk_table.columns {
                                                if fk_parent_col.key_type == expected_key {
                                                    match &fk_parent_col.data {
                                                        ColumnVector::Strings(v) => {
                                                            for row in 0..fk_table.len() {
                                                                referee_uniq_context[row]
                                                                    .push(v.v[row].clone());
                                                            }
                                                        }
                                                        ColumnVector::Ints(v) => {
                                                            for row in 0..fk_table.len() {
                                                                referee_uniq_context[row]
                                                                    .push(v.v[row].to_string());
                                                            }
                                                        }
                                                        ColumnVector::Floats(_) => {
                                                            panic!("Floats cannot be primary keys")
                                                        }
                                                        ColumnVector::Bools(_) => {
                                                            panic!("Bools cannot be primary keys")
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        let mut fkeys_vector = Vec::with_capacity(fk_table.len());
                                        let mut reverse_ref_vector: Vec<Vec<usize>> =
                                            Vec::with_capacity(t.len());
                                        for _ in 0..t.len() {
                                            reverse_ref_vector.push(Vec::new());
                                        }
                                        // we have children subsets, now we can compare
                                        match &fk_column.data {
                                            ColumnVector::Strings(v) => {
                                                for row in 0..fk_table.len() {
                                                    let res = child_vec_map
                                                        .get(&referee_uniq_context[row]);
                                                    match res {
                                                        Some(row_map) => {
                                                            match row_map.get(&v.v[row]) {
                                                                Some(idx) => {
                                                                    // update binary index
                                                                    fkeys_vector.push(*idx);
                                                                    reverse_ref_vector[*idx]
                                                                        .push(row);
                                                                }
                                                                None => {
                                                                    return Err(DatabaseValidationError::NonExistingForeignKeyToChildTable {
                                                                        table_parent_keys: referee_uniq_context[row].clone(),
                                                                        table_parent_tables: parent_table_names.iter().map(|i| i.as_str().to_string()).collect(),
                                                                        table_parent_columns: parent_table_colums.iter().map(|i| i.as_str().to_string()).collect(),
                                                                        table_with_foreign_key: fk_table.name.as_str().to_string(),
                                                                        foreign_key_column: fk_column.column_name.as_str().to_string(),
                                                                        referred_table: t.name.as_str().to_string(),
                                                                        referred_table_column: pk.column_name.as_str().to_string(),
                                                                        key_value: v.v[row].clone(),
                                                                    });
                                                                }
                                                            }
                                                        }
                                                        None => {
                                                            return Err(DatabaseValidationError::NonExistingForeignKeyToChildTable {
                                                                table_parent_keys: referee_uniq_context[row].clone(),
                                                                table_parent_tables: parent_table_names.iter().map(|i| i.as_str().to_string()).collect(),
                                                                table_parent_columns: parent_table_colums.iter().map(|i| i.as_str().to_string()).collect(),
                                                                table_with_foreign_key: fk_table.name.as_str().to_string(),
                                                                foreign_key_column: fk_column.column_name.as_str().to_string(),
                                                                referred_table: t.name.as_str().to_string(),
                                                                referred_table_column: pk.column_name.as_str().to_string(),
                                                                key_value: v.v[row].clone(),
                                                            });
                                                        }
                                                    }
                                                }
                                            }
                                            ColumnVector::Ints(v) => {
                                                for row in 0..fk_table.len() {
                                                    let res = child_vec_map
                                                        .get(&referee_uniq_context[row]);
                                                    match res {
                                                        Some(row_map) => {
                                                            match row_map.get(&v.v[row].to_string())
                                                            {
                                                                Some(idx) => {
                                                                    // update binary index
                                                                    fkeys_vector.push(*idx);
                                                                    reverse_ref_vector[*idx]
                                                                        .push(row);
                                                                }
                                                                None => {
                                                                    return Err(DatabaseValidationError::NonExistingForeignKeyToChildTable {
                                                                        table_parent_keys: referee_uniq_context[row].clone(),
                                                                        table_parent_tables: parent_table_names.iter().map(|i| i.as_str().to_string()).collect(),
                                                                        table_parent_columns: parent_table_colums.iter().map(|i| i.as_str().to_string()).collect(),
                                                                        table_with_foreign_key: fk_table.name.as_str().to_string(),
                                                                        foreign_key_column: fk_column.column_name.as_str().to_string(),
                                                                        referred_table: t.name.as_str().to_string(),
                                                                        referred_table_column: pk.column_name.as_str().to_string(),
                                                                        key_value: v.v[row].to_string(),
                                                                    });
                                                                }
                                                            }
                                                        }
                                                        None => {
                                                            return Err(DatabaseValidationError::NonExistingForeignKeyToChildTable {
                                                                table_parent_keys: referee_uniq_context[row].clone(),
                                                                table_parent_tables: parent_table_names.iter().map(|i| i.as_str().to_string()).collect(),
                                                                table_parent_columns: parent_table_colums.iter().map(|i| i.as_str().to_string()).collect(),
                                                                table_with_foreign_key: fk_table.name.as_str().to_string(),
                                                                foreign_key_column: fk_column.column_name.as_str().to_string(),
                                                                referred_table: t.name.as_str().to_string(),
                                                                referred_table_column: pk.column_name.as_str().to_string(),
                                                                key_value: v.v[row].to_string(),
                                                            });
                                                        }
                                                    }
                                                }
                                            }
                                            ColumnVector::Floats(_) => {
                                                panic!("Floats cannot be primary keys")
                                            }
                                            ColumnVector::Bools(_) => {
                                                panic!("Bools cannot be primary keys")
                                            }
                                        }

                                        // all good, update index
                                        let rel = ForeignKeyRelationship {
                                            referred_table: t.name.clone(),
                                            referee_table: fk_table.name.clone(),
                                            referee_column: fk_column.column_name.clone(),
                                        };

                                        let data = ForeignKeyRelationshipData {
                                            foreign_keys_data: fkeys_vector,
                                            reverse_referrees_data: reverse_ref_vector,
                                        };

                                        let in_res = res.foreign_keys_map.insert(rel, data);
                                        assert!(in_res.is_none());
                                    } else if *is_to_self_child_table {
                                        let rel_key = ForeignKeyToNativeChildRelationship {
                                            referred_table: t.name.clone(),
                                            referee_table: fk_table.name.clone(),
                                            referee_column: fk_column.column_name.clone(),
                                        };

                                        let data = res
                                            .foreign_to_native_child_keys_map
                                            .get(&rel_key)
                                            .unwrap();
                                        let mut buckets_referee: HashMap<
                                            Vec<String>,
                                            HashMap<String, usize>,
                                        > = HashMap::new();

                                        let mut common_keys_set = HashSet::new();
                                        for i in &data.common_keys {
                                            let _ = common_keys_set.insert(i.clone());
                                        }
                                        let mut segments_keys_set = HashSet::new();
                                        for i in &data.refereed_columns_by_key {
                                            let _ = segments_keys_set.insert(i.clone());
                                        }

                                        let mut referee_parent_keys: Vec<Vec<String>> =
                                            Vec::with_capacity(t.len());
                                        let mut referee_segment_keys: Vec<Vec<String>> =
                                            Vec::with_capacity(t.len());
                                        for _ in 0..t.len() {
                                            referee_parent_keys
                                                .push(Vec::with_capacity(data.common_keys.len()));
                                            referee_segment_keys.push(Vec::with_capacity(
                                                data.refereed_columns_by_key.len(),
                                            ));
                                        }

                                        let mut parent_table_names =
                                            Vec::with_capacity(data.common_keys.len());
                                        for column in &t.columns {
                                            if common_keys_set.contains(&column.column_name) {
                                                match &column.data {
                                                    ColumnVector::Strings(sv) => {
                                                        for i in 0..sv.v.len() {
                                                            referee_parent_keys[i]
                                                                .push(sv.v[i].clone());
                                                        }
                                                    }
                                                    ColumnVector::Ints(iv) => {
                                                        for i in 0..iv.v.len() {
                                                            referee_parent_keys[i]
                                                                .push(iv.v[i].to_string());
                                                        }
                                                    }
                                                    ColumnVector::Floats(_) => {
                                                        panic!("Floats cannot be primary keys")
                                                    }
                                                    ColumnVector::Bools(_) => {
                                                        panic!("Bools cannot be primary keys")
                                                    }
                                                }

                                                if let KeyType::ParentPrimary { parent_table } =
                                                    &column.key_type
                                                {
                                                    parent_table_names.push(parent_table.clone());
                                                }
                                            } else if segments_keys_set
                                                .contains(&column.column_name)
                                            {
                                                match &column.data {
                                                    ColumnVector::Strings(sv) => {
                                                        for i in 0..sv.v.len() {
                                                            referee_segment_keys[i]
                                                                .push(sv.v[i].clone());
                                                        }
                                                    }
                                                    ColumnVector::Ints(iv) => {
                                                        for i in 0..iv.v.len() {
                                                            referee_segment_keys[i]
                                                                .push(iv.v[i].to_string());
                                                        }
                                                    }
                                                    ColumnVector::Floats(_) => {
                                                        panic!("Floats cannot be primary keys")
                                                    }
                                                    ColumnVector::Bools(_) => {
                                                        panic!("Bools cannot be primary keys")
                                                    }
                                                }
                                            }
                                        }

                                        for (idx, k) in referee_parent_keys.into_iter().enumerate()
                                        {
                                            let e = buckets_referee
                                                .entry(k)
                                                .or_insert_with(HashMap::new);
                                            let joined_key = referee_segment_keys[idx].join("=>");
                                            let res = e.insert(joined_key, idx);
                                            // should all be unique at this point, earlier unique child checks should have triggered
                                            assert!(res.is_none());
                                        }

                                        let mut referrer_parent_keys: Vec<Vec<String>> =
                                            Vec::with_capacity(fk_table.len());
                                        let mut referrer_to_fk_keys: Vec<String> =
                                            Vec::with_capacity(fk_table.len());
                                        for _ in 0..fk_table.len() {
                                            referrer_parent_keys
                                                .push(Vec::with_capacity(data.common_keys.len()));
                                        }

                                        for column in &fk_table.columns {
                                            if common_keys_set.contains(&column.column_name) {
                                                match &column.data {
                                                    ColumnVector::Strings(sv) => {
                                                        for i in 0..sv.v.len() {
                                                            referrer_parent_keys[i]
                                                                .push(sv.v[i].clone());
                                                        }
                                                    }
                                                    ColumnVector::Ints(iv) => {
                                                        for i in 0..iv.v.len() {
                                                            referrer_parent_keys[i]
                                                                .push(iv.v[i].to_string());
                                                        }
                                                    }
                                                    ColumnVector::Floats(_) => {
                                                        panic!("Floats cannot be primary keys")
                                                    }
                                                    ColumnVector::Bools(_) => {
                                                        panic!("Bools cannot be primary keys")
                                                    }
                                                }
                                            } else if column.column_name == fk_column.column_name {
                                                match &column.data {
                                                    ColumnVector::Strings(sv) => {
                                                        for i in &sv.v {
                                                            referrer_to_fk_keys.push(i.clone());
                                                        }
                                                    }
                                                    ColumnVector::Ints(_) => {
                                                        panic!("Ints cannot be primary keys")
                                                    }
                                                    ColumnVector::Floats(_) => {
                                                        panic!("Floats cannot be primary keys")
                                                    }
                                                    ColumnVector::Bools(_) => {
                                                        panic!("Bools cannot be primary keys")
                                                    }
                                                }
                                            }
                                        }

                                        let mut fkeys_vector = Vec::with_capacity(fk_table.len());
                                        let mut reverse_ref_vector: Vec<Vec<usize>> =
                                            Vec::with_capacity(t.len());
                                        for _ in 0..t.len() {
                                            reverse_ref_vector.push(Vec::new());
                                        }
                                        for row in 0..referrer_to_fk_keys.len() {
                                            let res =
                                                buckets_referee.get(&referrer_parent_keys[row]);
                                            match res {
                                                Some(hmap) => {
                                                    match hmap.get(&referrer_to_fk_keys[row]) {
                                                        Some(idx) => {
                                                            fkeys_vector.push(*idx);
                                                            reverse_ref_vector[*idx].push(row);
                                                        }
                                                        None => {
                                                            return Err(DatabaseValidationError::NonExistingParentToChildKey {
                                                                table_parent_keys: referrer_parent_keys[row].clone(),
                                                                table_parent_tables: parent_table_names.iter().map(|i| i.as_str().to_string()).collect(),
                                                                table_parent_columns: data.common_keys.iter().map(|i| i.as_str().to_string()).collect(),
                                                                table_with_foreign_key: fk_table.name.as_str().to_string(),
                                                                foreign_key_column: fk_column.column_name.as_str().to_string(),
                                                                referred_table: t.name.as_str().to_string(),
                                                                referred_table_column: data.refereed_columns_by_key.iter().map(|i| i.as_str().to_string()).collect::<Vec<_>>().join("=>"),
                                                                key_value: referrer_to_fk_keys[row].clone(),
                                                            });
                                                        }
                                                    }
                                                }
                                                None => {
                                                    return Err(DatabaseValidationError::NonExistingParentToChildKey {
                                                        table_parent_keys: referrer_parent_keys[row].clone(),
                                                        table_parent_tables: parent_table_names.iter().map(|i| i.as_str().to_string()).collect(),
                                                        table_parent_columns: data.common_keys.iter().map(|i| i.as_str().to_string()).collect(),
                                                        table_with_foreign_key: fk_table.name.as_str().to_string(),
                                                        foreign_key_column: fk_column.column_name.as_str().to_string(),
                                                        referred_table: t.name.as_str().to_string(),
                                                        referred_table_column: data.refereed_columns_by_key.iter().map(|i| i.as_str().to_string()).collect::<Vec<_>>().join("=>"),
                                                        key_value: referrer_to_fk_keys[row].clone(),
                                                    });
                                                }
                                            }
                                        }

                                        // all good, update index
                                        let rel = ForeignKeyRelationship {
                                            referred_table: t.name.clone(),
                                            referee_table: fk_table.name.clone(),
                                            referee_column: fk_column.column_name.clone(),
                                        };

                                        let data = ForeignKeyRelationshipData {
                                            foreign_keys_data: fkeys_vector,
                                            reverse_referrees_data: reverse_ref_vector,
                                        };

                                        let in_res = res.foreign_keys_map.insert(rel, data);
                                        assert!(in_res.is_none());
                                    } else {
                                        let rel_key = ForeignKeyToForeignChildRelationship {
                                            referred_table: t.name.clone(),
                                            referee_table: fk_table.name.clone(),
                                            referee_column: fk_column.column_name.clone(),
                                        };

                                        let data = res
                                            .foreign_to_foreign_child_keys_map
                                            .get(&rel_key)
                                            .unwrap();
                                        let mut buckets_referee: HashMap<
                                            Vec<String>,
                                            HashMap<String, usize>,
                                        > = HashMap::new();

                                        let mut common_keys_set = HashSet::new();
                                        for i in &data.common_parent_keys {
                                            let _ = common_keys_set.insert(i.clone());
                                        }
                                        let mut segments_keys_set = HashSet::new();
                                        for i in &data.refereed_columns_by_key {
                                            let _ = segments_keys_set.insert(i.clone());
                                        }

                                        let mut referee_parent_keys: Vec<Vec<String>> =
                                            Vec::with_capacity(t.len());
                                        let mut referee_segment_keys: Vec<Vec<String>> =
                                            Vec::with_capacity(t.len());
                                        for _ in 0..t.len() {
                                            referee_parent_keys.push(Vec::with_capacity(
                                                data.common_parent_keys.len(),
                                            ));
                                            referee_segment_keys.push(Vec::with_capacity(
                                                data.refereed_columns_by_key.len(),
                                            ));
                                        }

                                        let mut parent_table_names =
                                            Vec::with_capacity(data.common_parent_keys.len());
                                        for column in &t.columns {
                                            if common_keys_set.contains(&column.column_name) {
                                                match &column.data {
                                                    ColumnVector::Strings(sv) => {
                                                        for i in 0..sv.v.len() {
                                                            referee_parent_keys[i]
                                                                .push(sv.v[i].clone());
                                                        }
                                                    }
                                                    ColumnVector::Ints(iv) => {
                                                        for i in 0..iv.v.len() {
                                                            referee_parent_keys[i]
                                                                .push(iv.v[i].to_string());
                                                        }
                                                    }
                                                    ColumnVector::Floats(_) => {
                                                        panic!("Floats cannot be primary keys")
                                                    }
                                                    ColumnVector::Bools(_) => {
                                                        panic!("Bools cannot be primary keys")
                                                    }
                                                }

                                                if let KeyType::ParentPrimary { parent_table } =
                                                    &column.key_type
                                                {
                                                    parent_table_names.push(parent_table.clone());
                                                }
                                            } else if segments_keys_set
                                                .contains(&column.column_name)
                                            {
                                                match &column.data {
                                                    ColumnVector::Strings(sv) => {
                                                        for i in 0..sv.v.len() {
                                                            referee_segment_keys[i]
                                                                .push(sv.v[i].clone());
                                                        }
                                                    }
                                                    ColumnVector::Ints(iv) => {
                                                        for i in 0..iv.v.len() {
                                                            referee_segment_keys[i]
                                                                .push(iv.v[i].to_string());
                                                        }
                                                    }
                                                    ColumnVector::Floats(_) => {
                                                        panic!("Floats cannot be primary keys")
                                                    }
                                                    ColumnVector::Bools(_) => {
                                                        panic!("Bools cannot be primary keys")
                                                    }
                                                }
                                            }
                                        }

                                        for (idx, k) in referee_parent_keys.into_iter().enumerate()
                                        {
                                            let e = buckets_referee
                                                .entry(k)
                                                .or_insert_with(HashMap::new);
                                            let joined_key = referee_segment_keys[idx].join("=>");
                                            let res = e.insert(joined_key, idx);
                                            // should all be unique at this point, earlier unique child checks should have triggered
                                            assert!(res.is_none());
                                        }

                                        let mut referrer_parent_keys: Vec<Vec<String>> =
                                            Vec::with_capacity(fk_table.len());
                                        let mut referrer_to_fk_keys: Vec<String> =
                                            Vec::with_capacity(fk_table.len());
                                        for _ in 0..fk_table.len() {
                                            referrer_parent_keys.push(Vec::with_capacity(
                                                data.common_parent_keys.len(),
                                            ));
                                        }

                                        for column in &fk_table.columns {
                                            if common_keys_set.contains(&column.column_name) {
                                                match &column.data {
                                                    ColumnVector::Strings(sv) => {
                                                        for i in 0..sv.v.len() {
                                                            referrer_parent_keys[i]
                                                                .push(sv.v[i].clone());
                                                        }
                                                    }
                                                    ColumnVector::Ints(iv) => {
                                                        for i in 0..iv.v.len() {
                                                            referrer_parent_keys[i]
                                                                .push(iv.v[i].to_string());
                                                        }
                                                    }
                                                    ColumnVector::Floats(_) => {
                                                        panic!("Floats cannot be primary keys")
                                                    }
                                                    ColumnVector::Bools(_) => {
                                                        panic!("Bools cannot be primary keys")
                                                    }
                                                }
                                            } else if column.column_name == fk_column.column_name {
                                                match &column.data {
                                                    ColumnVector::Strings(sv) => {
                                                        for i in &sv.v {
                                                            referrer_to_fk_keys.push(i.clone());
                                                        }
                                                    }
                                                    ColumnVector::Ints(_) => {
                                                        panic!("Ints cannot be primary keys")
                                                    }
                                                    ColumnVector::Floats(_) => {
                                                        panic!("Floats cannot be primary keys")
                                                    }
                                                    ColumnVector::Bools(_) => {
                                                        panic!("Bools cannot be primary keys")
                                                    }
                                                }
                                            }
                                        }

                                        let mut fkeys_vector = Vec::with_capacity(fk_table.len());
                                        let mut reverse_ref_vector: Vec<Vec<usize>> =
                                            Vec::with_capacity(t.len());
                                        for _ in 0..t.len() {
                                            reverse_ref_vector.push(Vec::new());
                                        }
                                        for row in 0..referrer_to_fk_keys.len() {
                                            let res =
                                                buckets_referee.get(&referrer_parent_keys[row]);
                                            match res {
                                                Some(hmap) => {
                                                    match hmap.get(&referrer_to_fk_keys[row]) {
                                                        Some(idx) => {
                                                            fkeys_vector.push(*idx);
                                                            reverse_ref_vector[*idx].push(row);
                                                        }
                                                        None => {
                                                            return Err(DatabaseValidationError::NonExistingForeignKeyToChildTable {
                                                                table_parent_keys: referrer_parent_keys[row].clone(),
                                                                table_parent_tables: parent_table_names.iter().map(|i| i.as_str().to_string()).collect(),
                                                                table_parent_columns: data.common_parent_keys.iter().map(|i| i.as_str().to_string()).collect(),
                                                                table_with_foreign_key: fk_table.name.as_str().to_string(),
                                                                foreign_key_column: fk_column.column_name.as_str().to_string(),
                                                                referred_table: t.name.as_str().to_string(),
                                                                referred_table_column: data.refereed_columns_by_key.iter().map(|i| i.as_str().to_string()).collect::<Vec<_>>().join("=>"),
                                                                key_value: referrer_to_fk_keys[row].clone(),
                                                            });
                                                        }
                                                    }
                                                }
                                                None => {
                                                    return Err(DatabaseValidationError::NonExistingForeignKeyToChildTable {
                                                        table_parent_keys: referrer_parent_keys[row].clone(),
                                                        table_parent_tables: parent_table_names.iter().map(|i| i.as_str().to_string()).collect(),
                                                        table_parent_columns: data.common_parent_keys.iter().map(|i| i.as_str().to_string()).collect(),
                                                        table_with_foreign_key: fk_table.name.as_str().to_string(),
                                                        foreign_key_column: fk_column.column_name.as_str().to_string(),
                                                        referred_table: t.name.as_str().to_string(),
                                                        referred_table_column: data.refereed_columns_by_key.iter().map(|i| i.as_str().to_string()).collect::<Vec<_>>().join("=>"),
                                                        key_value: referrer_to_fk_keys[row].clone(),
                                                    });
                                                }
                                            }
                                        }

                                        // all good, update index
                                        let rel = ForeignKeyRelationship {
                                            referred_table: t.name.clone(),
                                            referee_table: fk_table.name.clone(),
                                            referee_column: fk_column.column_name.clone(),
                                        };

                                        let data = ForeignKeyRelationshipData {
                                            foreign_keys_data: fkeys_vector,
                                            reverse_referrees_data: reverse_ref_vector,
                                        };

                                        let in_res = res.foreign_keys_map.insert(rel, data);
                                        assert!(in_res.is_none());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn ensure_primary_keys_unique_per_table_and_fkeys_exist(
    res: &mut AllData,
) -> Result<(), DatabaseValidationError> {
    for t in res.tables.iter() {
        if let Some(pk) = t.primary_key_column() {
            if let KeyType::Primary = &pk.key_type {
                match &pk.data {
                    ColumnVector::Strings(vc) => {
                        let mut pkey_map = HashMap::new();
                        for (idx, k) in vc.v.iter().enumerate() {
                            if pkey_map.insert(k.clone(), idx).is_some() {
                                return Err(DatabaseValidationError::DuplicatePrimaryKey {
                                    table_name: t.name.as_str().to_string(),
                                    value: k.clone(),
                                });
                            }
                        }

                        for fk in &res.tables {
                            if fk.name != t.name {
                                for fkc in &fk.columns {
                                    if fkc.is_fkey_to_table(&t.name) {
                                        let mut row_fk_index: Vec<usize> =
                                            Vec::with_capacity(vc.len());
                                        let mut reverse_fk_index: Vec<Vec<usize>> =
                                            Vec::with_capacity(t.len());
                                        for _ in 0..t.len() {
                                            reverse_fk_index.push(Vec::new());
                                        }
                                        match &fkc.data {
                                            ColumnVector::Strings(vc) => {
                                                for (r_idx, fval) in vc.v.iter().enumerate() {
                                                    match pkey_map.get(fval) {
                                                        Some(idx) => {
                                                            row_fk_index.push(*idx);
                                                            reverse_fk_index[*idx].push(r_idx);
                                                        }
                                                        None => {
                                                            return Err(DatabaseValidationError::NonExistingForeignKey {
                                                                table_with_foreign_key: fk.name.as_str().to_string(),
                                                                foreign_key_column: fkc.column_name.as_str().to_string(),
                                                                referred_table: t.name.as_str().to_string(),
                                                                referred_table_column: pk.column_name.as_str().to_string(),
                                                                key_value: fval.clone(),
                                                            });
                                                        }
                                                    }
                                                }
                                            }
                                            _ => {
                                                panic!("Something is very wrong, branches should match")
                                            }
                                        }
                                        let rel_key = ForeignKeyRelationship {
                                            referred_table: t.name.clone(),
                                            referee_table: fk.name.clone(),
                                            referee_column: fkc.column_name.clone(),
                                        };
                                        let data = ForeignKeyRelationshipData {
                                            foreign_keys_data: row_fk_index,
                                            reverse_referrees_data: reverse_fk_index,
                                        };
                                        let ins_res = res.foreign_keys_map.insert(rel_key, data);
                                        assert!(ins_res.is_none());
                                    }
                                }
                            }
                        }
                    }
                    ColumnVector::Ints(vc) => {
                        let mut pkey_map = HashMap::new();
                        for (idx, k) in vc.v.iter().enumerate() {
                            if pkey_map.insert(*k, idx).is_some() {
                                return Err(DatabaseValidationError::DuplicatePrimaryKey {
                                    table_name: t.name.as_str().to_string(),
                                    value: format!("{k}"),
                                });
                            }
                        }

                        for fk in &res.tables {
                            if fk.name != t.name {
                                for fkc in &fk.columns {
                                    if fkc.is_fkey_to_table(&t.name) {
                                        let mut row_fk_index = Vec::with_capacity(vc.len());
                                        let mut reverse_fk_index: Vec<Vec<usize>> =
                                            Vec::with_capacity(t.len());
                                        for _ in 0..t.len() {
                                            reverse_fk_index.push(Vec::new());
                                        }
                                        match &fkc.data {
                                            ColumnVector::Ints(vc) => {
                                                for (r_idx, fval) in vc.v.iter().enumerate() {
                                                    match pkey_map.get(fval) {
                                                        Some(idx) => {
                                                            row_fk_index.push(*idx);
                                                            reverse_fk_index[*idx].push(r_idx);
                                                        }
                                                        None => {
                                                            return Err(DatabaseValidationError::NonExistingForeignKey {
                                                                table_with_foreign_key: fk.name.as_str().to_string(),
                                                                foreign_key_column: fkc.column_name.as_str().to_string(),
                                                                referred_table: t.name.as_str().to_string(),
                                                                referred_table_column: pk.column_name.as_str().to_string(),
                                                                key_value: format!("{fval}"),
                                                            });
                                                        }
                                                    }
                                                }
                                            }
                                            _ => {
                                                panic!("Something is very wrong, branches should match")
                                            }
                                        }
                                        let rel_key = ForeignKeyRelationship {
                                            referred_table: t.name.clone(),
                                            referee_table: fk.name.clone(),
                                            referee_column: fkc.column_name.clone(),
                                        };
                                        let data = ForeignKeyRelationshipData {
                                            foreign_keys_data: row_fk_index,
                                            reverse_referrees_data: reverse_fk_index,
                                        };
                                        let ins_res = res.foreign_keys_map.insert(rel_key, data);
                                        assert!(ins_res.is_none());
                                    }
                                }
                            }
                        }
                    }
                    ColumnVector::Floats(_) => {
                        panic!("Floats are not supposed to be primary keys")
                    }
                    ColumnVector::Bools(_) => {
                        panic!("Booleans are not supposed to be primary keys")
                    }
                }
            }
        }
    }

    Ok(())
}

fn ensure_parent_primary_keys_exist_for_children(
    res: &mut AllData,
) -> Result<(), DatabaseValidationError> {
    for i in &res.tables {
        let parent_columns = i
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, i)| match i.key_type {
                KeyType::ParentPrimary { .. } => Some(idx),
                _ => None,
            })
            .collect::<Vec<_>>();

        if !parent_columns.is_empty() {
            // this table must contain parent tuple prefix
            // every table will check other table, so, no recursion needed
            let last_parent_table = *parent_columns.last().unwrap();
            // we assume order of primary keys
            let child_primary_key = last_parent_table + 1;
            if let KeyType::ParentPrimary { parent_table } = &i.columns[last_parent_table].key_type
            {
                assert_ne!(
                    *parent_table, i.name,
                    "Paranoia... We should have checked this much earlier"
                );
                // get all tuples for this table, and ensure they all exist on parent table

                let (uniq_parents_by_child, uniq_parents_by_child_vec_idx) = {
                    // check if children are unique
                    let mut columns_vec: Vec<Vec<String>> =
                        Vec::with_capacity(parent_columns.len() + 1);
                    let mut column_names_vec: Vec<String> =
                        Vec::with_capacity(parent_columns.len() + 1);

                    let mut dump_vec = |v: &ColumnVector| {
                        let mut new_vec = Vec::with_capacity(v.len());
                        match v {
                            ColumnVector::Strings(sv) => {
                                for i in &sv.v {
                                    new_vec.push(i.clone())
                                }
                            }
                            ColumnVector::Ints(iv) => {
                                for i in &iv.v {
                                    new_vec.push(format!("{i}"))
                                }
                            }
                            ColumnVector::Floats(_) => {
                                panic!("Floats have nothing to do with being primary keys, we should have checked earlier")
                            }
                            ColumnVector::Bools(_) => {
                                panic!("Booleans have nothing to do with being primary keys, we should have checked earlier")
                            }
                        }

                        columns_vec.push(new_vec);
                    };

                    for parent in parent_columns.iter() {
                        dump_vec(&i.columns[*parent].data);
                        column_names_vec.push(i.columns[*parent].column_name.as_str().to_string());
                    }
                    dump_vec(&i.columns[child_primary_key].data);
                    column_names_vec.push(
                        i.columns[child_primary_key]
                            .column_name
                            .as_str()
                            .to_string(),
                    );

                    let mut uniq_parents_by_child_vec_idx = Vec::with_capacity(i.len());
                    let mut uniq_parents_by_child = HashSet::new();
                    let mut tuple_set = HashSet::with_capacity(i.len());
                    for row in 0..i.len() {
                        let mut tuple = Vec::with_capacity(parent_columns.len() + 1);
                        for column in &columns_vec {
                            tuple.push(column[row].clone());
                        }

                        if !parent_columns.is_empty() {
                            let parent_tuple = tuple.split_last().unwrap().1.to_vec();
                            let _ = uniq_parents_by_child.insert(parent_tuple.clone());
                            uniq_parents_by_child_vec_idx.push(parent_tuple);
                        }

                        if tuple_set.contains(&tuple) {
                            return Err(
                                DatabaseValidationError::FoundDuplicateChildPrimaryKeySet {
                                    table_name: i.name.as_str().to_string(),
                                    columns: format!("({})", column_names_vec.join(", ")),
                                    duplicate_values: format!("({})", tuple.join(", ")),
                                },
                            );
                        } else {
                            let ins = tuple_set.insert(tuple);
                            assert!(ins);
                        }
                    }

                    (uniq_parents_by_child, uniq_parents_by_child_vec_idx)
                };

                {
                    // check if parents contain the children
                    let last_parent_parent_table_idx = res.find_table_named_idx(parent_table);
                    assert_eq!(last_parent_parent_table_idx.len(), 1);
                    let last_parent_table = &res.tables[last_parent_parent_table_idx[0]];
                    let mut columns_vec: Vec<Vec<String>> =
                        Vec::with_capacity(parent_columns.len());
                    let mut column_names_vec: Vec<String> =
                        Vec::with_capacity(parent_columns.len());

                    let mut dump_vec = |v: &ColumnVector| {
                        let mut new_vec = Vec::with_capacity(v.len());
                        match v {
                            ColumnVector::Strings(sv) => {
                                for i in &sv.v {
                                    new_vec.push(i.clone())
                                }
                            }
                            ColumnVector::Ints(iv) => {
                                for i in &iv.v {
                                    new_vec.push(format!("{i}"))
                                }
                            }
                            ColumnVector::Floats(_) => {
                                panic!("Floats have nothing to do with being primary keys, we should have checked earlier")
                            }
                            ColumnVector::Bools(_) => {
                                panic!("Booleans have nothing to do with being primary keys, we should have checked earlier")
                            }
                        }

                        columns_vec.push(new_vec);
                    };

                    // we assume same indexes for parent table
                    for parent in parent_columns.iter() {
                        dump_vec(&last_parent_table.columns[*parent].data);
                        column_names_vec.push(
                            last_parent_table.columns[*parent]
                                .column_name
                                .as_str()
                                .to_string(),
                        );
                    }

                    let mut parent_set = HashMap::with_capacity(i.len());
                    for row in 0..last_parent_table.len() {
                        let mut tuple = Vec::with_capacity(parent_columns.len() + 1);
                        for column in 0..parent_columns.len() {
                            tuple.push(columns_vec[column][row].clone());
                        }
                        match parent_set.get(&tuple) {
                            Some(_) => {
                                return Err(
                                    DatabaseValidationError::FoundDuplicateChildPrimaryKeySet {
                                        table_name: last_parent_table.name.as_str().to_string(),
                                        columns: format!("({})", column_names_vec.join(", ")),
                                        duplicate_values: format!("({})", tuple.join(", ")),
                                    },
                                );
                            }
                            None => {
                                let ins = parent_set.insert(tuple, row);
                                assert!(ins.is_none());
                            }
                        }
                    }

                    // we have the table, let's see if children elements have parent
                    for parent_by_child in uniq_parents_by_child.iter() {
                        if !parent_set.contains_key(parent_by_child) {
                            return Err(
                                DatabaseValidationError::ParentRecordWithSuchPrimaryKeysDoesntExist {
                                    parent_table: last_parent_table.name.as_str().to_string(),
                                    parent_columns_names_searched: format!("({})", column_names_vec.join(", ")),
                                    parent_columns_to_find: format!("({})", parent_by_child.join(", ")),
                                },
                            );
                        }
                    }

                    let parents_for_children_index = uniq_parents_by_child_vec_idx
                        .iter()
                        .map(|i| *parent_set.get(i).unwrap())
                        .collect::<Vec<_>>();

                    let mut children_for_parents_index: Vec<Vec<usize>> =
                        std::iter::repeat_with(Vec::new)
                            .take(last_parent_table.len())
                            .collect();

                    for (child_idx, parent_idx) in parents_for_children_index.iter().enumerate() {
                        children_for_parents_index[*parent_idx].push(child_idx);
                    }

                    let relationship_key = ParentKeyRelationship {
                        parent_table: last_parent_table.name.clone(),
                        child_table: i.name.clone(),
                    };
                    let data = ParentKeyRelationshipData {
                        parents_for_children_index,
                        children_for_parents_index,
                    };
                    let ins_res = res.parent_child_keys_map.insert(relationship_key, data);
                    assert!(ins_res.is_none());
                }
            } else {
                panic!("Should never happen here, we know this is parent key")
            }
        }
    }

    Ok(())
}

fn ensure_uniq_constaints_are_not_violated(res: &AllData) -> Result<(), DatabaseValidationError> {
    for t in &res.tables {
        for uc in &t.uniq_constraints {
            let table_length = t.len();
            let mut target_vectors = Vec::with_capacity(uc.fields.len());
            let fields_set = uc
                .fields
                .iter()
                .map(|i| i.as_str().to_owned())
                .collect::<HashSet<String>>();

            for f in t.columns.iter() {
                if fields_set.contains(f.column_name.as_str()) {
                    match &f.data {
                        ColumnVector::Strings(sv) => {
                            target_vectors.push(sv.v.clone());
                        }
                        ColumnVector::Ints(iv) => {
                            target_vectors.push(iv.v.iter().map(|i| i.to_string()).collect());
                        }
                        ColumnVector::Bools(bv) => {
                            target_vectors.push(bv.v.iter().map(|i| i.to_string()).collect());
                        }
                        ColumnVector::Floats(_) => {
                            panic!("Floats cannot be used in uniq constraints")
                        }
                    }
                }
            }

            let mut tuple_set: HashSet<Vec<&str>> = HashSet::with_capacity(table_length);
            for row_idx in 0..table_length {
                let mut key = Vec::with_capacity(uc.fields.len());
                for column_idx in 0..uc.fields.len() {
                    key.push(target_vectors[column_idx][row_idx].as_str());
                }

                if tuple_set.contains(&key) {
                    return Err(DatabaseValidationError::UniqConstraintViolated {
                        table_name: t.name.as_str().to_string(),
                        tuple_definition: format!(
                            "({})",
                            uc.fields
                                .iter()
                                .map(|i| i.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                        tuple_value: format!("({})", key.join(", ")),
                    });
                } else {
                    tuple_set.insert(key);
                }
            }
        }
    }

    Ok(())
}

fn init_all_declared_tables(
    res: &mut AllData,
    outputs: &SourceOutputs,
) -> Result<(), DatabaseValidationError> {
    for tbl in outputs.table_definitions() {
        // check if table is already defined
        if let Some(t) = res.tables.iter().find(|i| i.name.as_str() == tbl.name) {
            return Err(DatabaseValidationError::TableDefinedTwice {
                table_name: t.name.as_str().to_string(),
            });
        }

        if let Some(err) = validate_table_definition(tbl) {
            return Err(err);
        }

        let is_mat_view = tbl.mat_view_expression.is_some();

        let mut columns = Vec::with_capacity(tbl.columns.len());
        for i in tbl.columns.iter() {
            if is_mat_view {
                if i.has_default_value() {
                    return Err(DatabaseValidationError::MaterializedViewsCannotHaveDefaultColumnExpression {
                        table_name: tbl.name.clone(),
                        column_name: i.name.clone(),
                    });
                }

                if i.generated_expression.is_some() {
                    return Err(DatabaseValidationError::MaterializedViewsCannotHaveComputedColumnExpression {
                        table_name: tbl.name.clone(),
                        column_name: i.name.clone(),
                    });
                }
            }

            columns.push(map_parsed_column_to_data_column(i, tbl.name.as_str())?);
        }

        let mut uniq_constraints = Vec::with_capacity(tbl.uniq_constraints.len());
        for i in tbl.uniq_constraints.iter() {
            let mut hs = HashSet::new();
            for constraint in i.fields.iter() {
                if !hs.insert(constraint.clone()) {
                    return Err(DatabaseValidationError::UniqConstraintDuplicateColumn {
                        table_name: tbl.name.clone(),
                        column_name: constraint.clone(),
                    });
                }
            }

            let mut fields = Vec::with_capacity(i.fields.len());
            for uf in i.fields.iter() {
                fields.push(DBIdentifier::new(uf.as_str())?);
            }
            uniq_constraints.push(UniqConstraint { fields })
        }

        let row_checks = tbl.row_checks.clone();

        res.tables.push(DataTable {
            name: DBIdentifier::new(tbl.name.as_str())?,
            columns,
            uniq_constraints,
            row_checks,
            mat_view_expression: tbl.mat_view_expression.clone(),
            exclusive_lock: false,
        })
    }

    Ok(())
}

fn process_detached_defaults(
    res: &mut AllData,
    so: &SourceOutputs,
) -> Result<(), DatabaseValidationError> {
    let dd_iter = || so.detached_defaults().iter().flat_map(|i| i.values.iter());

    let mut processed_defaults: HashMap<String, String> = HashMap::new();
    for dd in dd_iter() {
        let table = DBIdentifier::new(&dd.table)?;
        let column = DBIdentifier::new(&dd.column)?;
        let found_tbl = res.find_table_named_idx(&table);

        if found_tbl.is_empty() {
            return Err(DatabaseValidationError::DetachedDefaultNonExistingTable {
                table: dd.table.clone(),
                column: dd.column.clone(),
                expression: dd.value.clone(),
            });
        }

        assert_eq!(found_tbl.len(), 1);
        let tbl_idx = found_tbl[0];
        match res.tables[tbl_idx]
            .columns
            .iter_mut()
            .find(|i| i.column_name == column)
        {
            Some(col) => {
                let key = format!("{}.{}", dd.table, dd.column);
                let processed = processed_defaults.insert(key, dd.value.clone());
                if let Some(expr) = processed {
                    return Err(
                        DatabaseValidationError::DetachedDefaultDefinedMultipleTimes {
                            table: dd.table.clone(),
                            column: dd.column.clone(),
                            expression_a: expr,
                            expression_b: dd.value.clone(),
                        },
                    );
                }

                if col.data.has_default_value() {
                    return Err(
                        DatabaseValidationError::DetachedDefaultDefinedForColumnAlreadyHavingDefaultValue {
                            table: dd.table.clone(),
                            column: dd.column.clone(),
                            hardcoded_default_value: col.data.default_value().unwrap(),
                            detached_default_value: dd.value.clone(),
                        },
                    );
                }
                // default value must not already be set now
                assert!(!col.data.has_default_value());

                let is_ok = col.data.try_set_default_value_from_string(&dd.value);
                if !is_ok {
                    return Err(DatabaseValidationError::DetachedDefaultBadValue {
                        table: dd.table.clone(),
                        column: dd.column.clone(),
                        value: dd.value.clone(),
                        expected_type: col.data.column_type(),
                        error: "Cannot parse value to expected type for this column".to_string(),
                    });
                }
            }
            None => {
                return Err(DatabaseValidationError::DetachedDefaultNonExistingColumn {
                    table: dd.table.clone(),
                    column: dd.column.clone(),
                    expression: dd.value.clone(),
                });
            }
        }
    }

    for td in so.table_definitions() {
        for col in &td.columns {
            let key = format!("{}.{}", td.name, col.name);
            if col.is_detached_default && !processed_defaults.contains_key(&key) {
                return Err(DatabaseValidationError::DetachedDefaultUndefined {
                    table: td.name.clone(),
                    column: col.name.clone(),
                });
            }
        }
    }

    Ok(())
}

fn assert_uniq_constraints_columns(
    res: &AllData,
    so: &SourceOutputs,
) -> Result<(), DatabaseValidationError> {
    for td in so.table_definitions() {
        let mut field_set = HashSet::new();
        for i in td.columns.iter() {
            if !field_set.insert(i.name.as_str()) {
                return Err(DatabaseValidationError::DuplicateColumnNames {
                    table_name: td.name.clone(),
                    column_name: i.name.clone(),
                });
            }
        }

        let tname = DBIdentifier::new(td.name.as_str())?;
        let found_t = res.find_table_named_idx(&tname)[0];
        let mut constraints_set = HashSet::new();
        for i in &td.uniq_constraints {
            let valid_column_set = res.tables[found_t]
                .columns
                .iter()
                .map(|i| i.column_name.as_str().to_string())
                .collect::<HashSet<_>>();
            for f in &i.fields {
                if !valid_column_set.contains(f.as_str()) {
                    return Err(DatabaseValidationError::UniqConstraintColumnDoesntExist {
                        table_name: td.name.clone(),
                        column_name: f.clone(),
                    });
                }

                for tc in td.columns.iter().filter(|c| c.name == *f) {
                    // TODO: workaroundish that we don't use enums in later stage, but oh well
                    if tc.the_type.as_str() == "FLOAT" {
                        return Err(
                            DatabaseValidationError::FloatColumnCannotBeInUniqueConstraint {
                                table_name: td.name.clone(),
                                column_name: f.clone(),
                            },
                        );
                    }
                }
            }
            let mut cloned = i.fields.clone();
            cloned.sort();
            if !constraints_set.insert(cloned.join("|")) {
                return Err(DatabaseValidationError::DuplicateUniqConstraints {
                    table_name: td.name.clone(),
                });
            }
        }
    }

    Ok(())
}

fn validate_table_metadata_interconnections(
    res: &mut AllData,
) -> Result<(), DatabaseValidationError> {
    validate_child_primary_keys(res)?;
    validate_non_child_foreign_keys(res)?;
    validate_child_foreign_keys(res)?;
    validate_child_native_keys(res)?;

    Ok(())
}

fn assert_table_column_order(res: &AllData) -> Result<(), DatabaseValidationError> {
    for t in &res.tables {
        let v1 = t
            .columns
            .iter()
            .map(|i| i.column_priority())
            .collect::<Vec<_>>();
        // we assume order, of this doesn't hold, something is very wrong and we messed up
        assert!(v1.windows(2).all(|w| w[0] <= w[1]));
    }

    Ok(())
}

fn assert_key_types_in_table(res: &AllData) -> Result<(), DatabaseValidationError> {
    for t in &res.tables {
        let mut primary_key_exists: i32 = 0;
        let mut child_primary_key_exists: i32 = 0;
        let mut parent_primary_key_exists: i32 = 0;

        for c in &t.columns {
            match &c.key_type {
                KeyType::NotAKey => {}
                KeyType::Primary => {
                    primary_key_exists += 1;
                }
                KeyType::ChildPrimary { .. } => child_primary_key_exists += 1,
                KeyType::ParentPrimary { .. } => parent_primary_key_exists += 1,
            }
        }

        if primary_key_exists > 0 {
            // only one primary key can exist on this table and nothing else
            assert_eq!(primary_key_exists, 1);
            assert_eq!(child_primary_key_exists, 0);
            assert_eq!(parent_primary_key_exists, 0);
        } else if child_primary_key_exists > 0 || parent_primary_key_exists > 0 {
            // there cannot exist ordinary primary key column
            assert_eq!(primary_key_exists, 0);
            // there must be only one child primary key
            assert_eq!(child_primary_key_exists, 1);
            // must be at least one parent if there is a child
            assert!(parent_primary_key_exists > 0);
        }
    }

    Ok(())
}

fn validate_non_child_foreign_keys(res: &mut AllData) -> Result<(), DatabaseValidationError> {
    let mut adjustments_vec = Vec::new();
    // second pass, ensure all referred tables exist
    for (table_idx, new_table) in res.tables.iter().enumerate() {
        for (column_idx, column) in new_table.columns.iter().enumerate() {
            if let Some(ForeignKey {
                foreign_table: to_table,
                is_to_foreign_child_table,
                is_to_self_child_table,
                ..
            }) = &column.maybe_foreign_key
            {
                if !*is_to_foreign_child_table && !*is_to_self_child_table {
                    let count = res.find_table_named_idx(to_table);
                    if count.is_empty() {
                        return Err(DatabaseValidationError::ForeignKeyTableDoesntExist {
                            referrer_table: new_table.name.as_str().to_string(),
                            referrer_column: column.column_name.as_str().to_string(),
                            referred_table: to_table.as_str().to_string(),
                        });
                    }
                    // if we find more than one table something is very wrong
                    assert_eq!(count.len(), 1);

                    // ensure foreign key type is same as referring primary key
                    let referred_idx = count[0];
                    // ensure referred table has primary key
                    let prim_keys_count: Vec<_> = res.tables[referred_idx]
                        .columns
                        .iter()
                        .enumerate()
                        .filter_map(|(idx, i)| {
                            if i.key_type == KeyType::Primary {
                                Some(idx)
                            } else {
                                None
                            }
                        })
                        .collect();

                    let parent_keys_count: Vec<_> = res.tables[referred_idx]
                        .columns
                        .iter()
                        .enumerate()
                        .filter_map(|(idx, i)| {
                            if let KeyType::ParentPrimary { .. } = i.key_type {
                                Some(idx)
                            } else {
                                None
                            }
                        })
                        .collect();

                    // primary and parent keys are mutually exclusive
                    assert!(!(!prim_keys_count.is_empty() && !parent_keys_count.is_empty()));

                    if prim_keys_count.is_empty() && parent_keys_count.is_empty() {
                        return Err(
                            DatabaseValidationError::ForeignKeyTableDoesntHavePrimaryKey {
                                referred_table: res.tables[referred_idx].name.as_str().to_string(),
                                referrer_table: new_table.name.as_str().to_string(),
                                referrer_column: column.column_name.as_str().to_string(),
                            },
                        );
                    }

                    let mut perform_tk_type_adjustment = || {
                        match (
                            column.data.column_type(),
                            res.tables[referred_idx]
                                .primary_key_column()
                                .unwrap()
                                .data
                                .column_type(),
                        ) {
                            // types match
                            (DBType::Text, DBType::Text) => {}
                            (DBType::Int, DBType::Int) => {}
                            (DBType::Float, DBType::Float) => {}
                            (DBType::Bool, DBType::Bool) => {}
                            // types diverge
                            (_, DBType::Text) => {
                                panic!("Cannot happen because DBText is default foreign key type")
                            }
                            (_, DBType::Int) => {
                                // child tables will always be strings connected with `->`
                                adjustments_vec.push((
                                    table_idx,
                                    column_idx,
                                    ColumnVector::Ints(ColumnVectorGeneric {
                                        v: vec![],
                                        default_value: None,
                                    }),
                                ));
                            }
                            (_, DBType::Float) => {
                                panic!("Floats cannot be primary keys, earlier validation should have returned an error")
                            }
                            (_, DBType::Bool) => {
                                panic!("Booleans cannot be primary keys, earlier validation should have returned an error")
                            }
                        }
                    };

                    if parent_keys_count.is_empty() {
                        // plain primary key mode
                        assert_eq!(prim_keys_count.len(), 1);

                        perform_tk_type_adjustment();
                    } else {
                        assert!(!parent_keys_count.is_empty());
                        if new_table.parent_table().is_none() {
                            return Err(DatabaseValidationError::ForeignKeyTableDoesNotShareCommonAncestorWithRefereeTable {
                                referred_table: res.tables[referred_idx].name.as_str().to_string(),
                                referrer_table: new_table.name.as_str().to_string(),
                                referrer_column: column.column_name.as_str().to_string(),
                            });
                        }

                        let mut common_ancestor_found = false;
                        'outer: for referred_t_col_idx in
                            (0..res.tables[referred_idx].columns.len()).rev()
                        {
                            for referrer_t_col_idx in (0..new_table.columns.len()).rev() {
                                if let (
                                    KeyType::ParentPrimary {
                                        parent_table: referred,
                                    },
                                    KeyType::ParentPrimary {
                                        parent_table: referee,
                                    },
                                ) = (
                                    &res.tables[referred_idx].columns[referred_t_col_idx].key_type,
                                    &new_table.columns[referrer_t_col_idx].key_type,
                                ) {
                                    if referred == referee {
                                        common_ancestor_found = true;
                                        break 'outer;
                                    }
                                }
                            }
                        }

                        if !common_ancestor_found {
                            return Err(DatabaseValidationError::ForeignKeyTableDoesNotShareCommonAncestorWithRefereeTable {
                                referred_table: res.tables[referred_idx].name.as_str().to_string(),
                                referrer_table: new_table.name.as_str().to_string(),
                                referrer_column: column.column_name.as_str().to_string(),
                            });
                        }

                        perform_tk_type_adjustment();
                    }
                }
            }
        }
    }

    // adjust foreign keys types and vectors based on primary key type information
    for (table_idx, column_idx, new_vector) in adjustments_vec {
        res.tables[table_idx].columns[column_idx].data = new_vector;
    }

    Ok(())
}

fn validate_child_native_keys(res: &mut AllData) -> Result<(), DatabaseValidationError> {
    let mut to_check_tables_vec: Vec<(DBIdentifier, (DBIdentifier, DBIdentifier))> = Vec::new();
    for new_table in res.tables.iter() {
        for column in new_table.columns.iter() {
            if let Some(ForeignKey {
                foreign_table: to_table,
                is_to_self_child_table,
                ..
            }) = &column.maybe_foreign_key
            {
                if *is_to_self_child_table {
                    let to_insert = (new_table.name.clone(), column.column_name.clone());
                    to_check_tables_vec.push((to_table.clone(), to_insert))
                }
            }
        }
    }

    let mut table_keys_to_restrict = HashSet::new();
    for (referred_table, (referrer_table, referrer_column)) in &to_check_tables_vec {
        let _ = table_keys_to_restrict.insert(referred_table.clone());

        let count = res.find_table_named_idx(referred_table);
        if count.is_empty() {
            return Err(DatabaseValidationError::ForeignKeyTableDoesntExist {
                referrer_table: referrer_table.as_str().to_string(),
                referrer_column: referrer_column.as_str().to_string(),
                referred_table: referred_table.as_str().to_string(),
            });
        }

        let referred_idx = count[0];
        // ensure referred table has primary key
        let prim_keys_empty = res.tables[referred_idx]
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, i)| {
                if i.key_type == KeyType::Primary {
                    Some(idx)
                } else {
                    None
                }
            })
            .next()
            .is_none();

        let parent_keys_count: Vec<_> = res.tables[referred_idx]
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, i)| {
                if let KeyType::ParentPrimary { .. } = i.key_type {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect();

        // primary and parent keys are mutually exclusive
        assert!(!(!prim_keys_empty && !parent_keys_count.is_empty()));

        // TODO: check that referred table is child to this table
        // prefix is all parent keys down from this table
        let is_this_parent = parent_keys_count.iter().find_map(|idx| {
            if let KeyType::ParentPrimary { parent_table } =
                &res.tables[referred_idx].columns[*idx].key_type
            {
                if parent_table.as_str() == referrer_table.as_str() {
                    Some(*idx)
                } else {
                    None
                }
            } else {
                panic!("Only parent primary keys should be here")
            }
        });

        if is_this_parent.is_none() {
            return Err(
                DatabaseValidationError::ReferredChildKeyTableIsNotDescendantToThisTable {
                    referrer_table: referrer_table.as_str().to_string(),
                    referrer_column: referrer_column.as_str().to_string(),
                    expected_to_be_descendant_table: referred_table.as_str().to_string(),
                },
            );
        }

        let parent_idx = is_this_parent.unwrap();

        let mut this_key_vec: Vec<DBIdentifier> = Vec::new();
        for i in parent_idx + 1..res.tables[referred_idx].columns.len() {
            match &res.tables[referred_idx].columns[i].key_type {
                KeyType::ChildPrimary { .. } | KeyType::ParentPrimary { .. } => {
                    this_key_vec.push(res.tables[referred_idx].columns[i].column_name.clone());
                }
                _ => {}
            }
        }

        let mut common_keys = Vec::new();
        for i in 0..parent_idx + 1 {
            if let KeyType::ParentPrimary { .. } = &res.tables[referred_idx].columns[i].key_type {
                common_keys.push(res.tables[referred_idx].columns[i].column_name.clone());
            }
        }

        let key = ForeignKeyToNativeChildRelationship {
            referred_table: referred_table.clone(),
            referee_table: referrer_table.clone(),
            referee_column: referrer_column.clone(),
        };

        let data = ForeignKeyToNativeChildRelationshipData {
            refereed_columns_by_key: this_key_vec,
            common_keys,
        };

        let res = res.foreign_to_native_child_keys_map.insert(key, data);
        assert!(res.is_none());
    }

    for to_restrict in &table_keys_to_restrict {
        let found = res.find_table_named_idx(to_restrict)[0];
        for column in &mut res.tables[found].columns {
            let should_restrict = match &column.key_type {
                KeyType::Primary => true,
                KeyType::ChildPrimary { .. } => true,
                KeyType::ParentPrimary { parent_table } => {
                    table_keys_to_restrict.contains(parent_table)
                }
                _ => false,
            };

            if should_restrict {
                column.is_snake_case_restricted = true;
            }
        }
    }

    Ok(())
}

fn validate_child_foreign_keys(res: &mut AllData) -> Result<(), DatabaseValidationError> {
    let mut to_check_tables_vec: Vec<(DBIdentifier, Vec<(DBIdentifier, DBIdentifier)>, bool)> =
        Vec::new();
    for new_table in res.tables.iter() {
        for column in new_table.columns.iter() {
            if let Some(ForeignKey {
                foreign_table: to_table,
                is_to_foreign_child_table,
                is_explicit_foreign_child_reference,
                ..
            }) = &column.maybe_foreign_key
            {
                if *is_to_foreign_child_table {
                    let mut found = false;
                    let to_insert = (new_table.name.clone(), column.column_name.clone());
                    for (tbl, v, _) in &mut to_check_tables_vec {
                        if !found && tbl == to_table {
                            found = true;
                            v.push(to_insert.clone());
                        }
                    }

                    if !found {
                        to_check_tables_vec.push((to_table.clone(), vec![to_insert], *is_explicit_foreign_child_reference))
                    }
                }
            }
        }
    }

    let mut table_keys_to_restrict = HashSet::new();
    for (referred_table, referees_vec, is_explicit_foreign_child_reference) in &to_check_tables_vec {
        let _ = table_keys_to_restrict.insert(referred_table.clone());

        // check table exists
        // ensure table is unrelated to this table, has no common parent.
        // but wait... it could have common parent...
        let count = res.find_table_named_idx(referred_table);
        if count.is_empty() {
            return Err(DatabaseValidationError::ForeignKeyTableDoesntExist {
                referrer_table: referees_vec[0].0.as_str().to_string(),
                referrer_column: referees_vec[0].1.as_str().to_string(),
                referred_table: referred_table.as_str().to_string(),
            });
        }

        let referred_idx = count[0];
        // ensure referred table has primary key
        let prim_keys_empty = res.tables[referred_idx]
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, i)| {
                if i.key_type == KeyType::Primary {
                    Some(idx)
                } else {
                    None
                }
            })
            .next()
            .is_none();

        let parent_keys_empty = res.tables[referred_idx]
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, i)| {
                if let KeyType::ParentPrimary { .. } = i.key_type {
                    Some(idx)
                } else {
                    None
                }
            })
            .next()
            .is_none();

        // primary and parent keys are mutually exclusive
        assert!(!(!prim_keys_empty && !parent_keys_empty));

        if parent_keys_empty {
            return Err(
                DatabaseValidationError::ForeignChildKeyTableDoesntHaveParentTable {
                    referrer_table: referees_vec[0].0.as_str().to_string(),
                    referrer_column: referees_vec[0].1.as_str().to_string(),
                    referred_table: referred_table.as_str().to_string(),
                },
            );
        }

        let parent_tables_of_referee = res.all_parent_tables(&res.tables[referred_idx]);
        for (referee_name, referee_column) in referees_vec {
            let referred_t = res.find_table_named_idx(referee_name)[0];
            let parent_tables_of_referrer_table = res.all_parent_tables(&res.tables[referred_t]);
            let mut common_parent_keys: Vec<DBIdentifier> = Vec::new();
            let mut this_explicit_key: Vec<DBIdentifier> = Vec::new();

            let mut diverged = false;
            if *is_explicit_foreign_child_reference {
                // Key was told to be explicit from the root with EXPLICIT keyword
                diverged = true;
            }
            for (idx, p_prefix) in parent_tables_of_referee.iter().enumerate() {
                match parent_tables_of_referrer_table.get(idx) {
                    Some(parent) => {
                        if parent != p_prefix {
                            diverged = true;
                        }
                    }
                    None => {
                        diverged = true;
                    }
                }

                if diverged {
                    for column in &res.tables[referred_idx].columns {
                        if let KeyType::ParentPrimary { parent_table } = &column.key_type {
                            if parent_table == p_prefix {
                                this_explicit_key.push(column.column_name.clone());
                            }
                        }
                    }
                    let _ = table_keys_to_restrict.insert(p_prefix.clone());
                } else {
                    for column in &res.tables[referred_idx].columns {
                        if let KeyType::ParentPrimary { parent_table } = &column.key_type {
                            if parent_table == p_prefix {
                                common_parent_keys.push(column.column_name.clone());
                            }
                        }
                    }
                }
            }
            this_explicit_key.push(
                res.tables[referred_idx]
                    .primary_key_column()
                    .unwrap()
                    .column_name
                    .clone(),
            );

            if !common_parent_keys.is_empty()
                && parent_tables_of_referrer_table.len() >= parent_tables_of_referee.len()
            {
                return Err(
                    DatabaseValidationError::ForeignChildKeyTableIsHigherOrEqualInAncestryThanTheReferrer {
                        referrer_table: referee_name.as_str().to_string(),
                        referrer_column: referee_column.as_str().to_string(),
                        referred_table: referred_table.as_str().to_string(),
                    },
                );
            }

            let rel = ForeignKeyToForeignChildRelationship {
                referred_table: referred_table.clone(),
                referee_table: referee_name.clone(),
                referee_column: referee_column.clone(),
            };
            let data = ForeignKeyToForeignChildRelationshipData {
                refereed_columns_by_key: this_explicit_key,
                common_parent_keys,
            };

            let res = res.foreign_to_foreign_child_keys_map.insert(rel, data);
            assert!(res.is_none());

            // now we have explicit key...
            // We must flag primary keys/child primary keys on this table as being restricted snake_case_keys

            // then in other step we check that they refer to something by this key set
            // we store this relationship in hash map

            // find a path from which uniqueness context flows
            // could be we must be very explicit
            // trace this tables parents and pick the prefix?

            // prefix count must be above
            // detect if one table is referred to another that refers back to child?
        }
    }

    for to_restrict in &table_keys_to_restrict {
        let found = res.find_table_named_idx(to_restrict)[0];
        for column in &mut res.tables[found].columns {
            let should_restrict = match &column.key_type {
                KeyType::Primary => true,
                KeyType::ChildPrimary { .. } => true,
                KeyType::ParentPrimary { parent_table } => {
                    table_keys_to_restrict.contains(parent_table)
                }
                _ => false,
            };

            if should_restrict {
                column.is_snake_case_restricted = true;
            }
        }
    }

    Ok(())
}

fn validate_child_primary_keys(res: &mut AllData) -> Result<(), DatabaseValidationError> {
    // check that parent table exists
    for cpk in &res.tables {
        for col in &cpk.columns {
            if let KeyType::ChildPrimary { parent_table } = &col.key_type {
                let parent_tables: Vec<_> = res
                    .tables
                    .iter()
                    .filter(|i| i.name == *parent_table)
                    .collect();
                if parent_tables.is_empty() {
                    return Err(DatabaseValidationError::NonExistingChildPrimaryKeyTable {
                        table_name: cpk.name.as_str().to_string(),
                        column_name: col.column_name.as_str().to_string(),
                        referred_table: parent_table.as_str().to_string(),
                    });
                }

                assert_eq!(parent_tables.len(), 1);

                // check that parent table is primary key or child primary key
                if parent_tables[0].primary_key_column().is_none() {
                    return Err(DatabaseValidationError::ParentTableHasNoPrimaryKey {
                        table_name: cpk.name.as_str().to_string(),
                        column_name: col.column_name.as_str().to_string(),
                        referred_table: parent_table.as_str().to_string(),
                    });
                }
            }
        }
    }

    // recursive algo goes after linear
    // if child primary key then follow references and try to find cycles, if cycle then error
    for cpk in &res.tables {
        for col in &cpk.columns {
            if let KeyType::ChildPrimary { parent_table } = &col.key_type {
                let mut child_stack: Vec<String> = vec![cpk.name.as_str().to_string()];
                let mut check_set = child_stack.iter().cloned().collect::<HashSet<String>>();

                find_child_primary_keys_recursive_loop(
                    res,
                    parent_table.as_str(),
                    &mut child_stack,
                    &mut check_set,
                )?;
            }
        }
    }

    let mut tables_to_extend = Vec::new();
    // find columns to extend
    for (idx, cpk) in res.tables.iter().enumerate() {
        for col in &cpk.columns {
            if let KeyType::ChildPrimary { parent_table } = &col.key_type {
                let mut parent_columns = Vec::new();
                find_parent_columns(res, &mut parent_columns, parent_table.as_str());

                tables_to_extend.push((idx, parent_columns));
            }
        }
    }

    // check if names clash before merging
    for (table_idx, extension_columns) in tables_to_extend.iter() {
        for (ext_t, ext_c) in extension_columns.iter() {
            for tc in res.tables[*table_idx].columns.iter() {
                if tc.column_name.as_str()
                    == res.tables[*ext_t].columns[*ext_c].column_name.as_str()
                {
                    return Err(DatabaseValidationError::ParentPrimaryKeyColumnNameClashesWithChildColumnName {
                        parent_table: res.tables[*ext_t].name.as_str().to_string(),
                        parent_column: res.tables[*ext_t].columns[*ext_c].column_name.as_str().to_string(),
                        child_table: res.tables[*table_idx].name.as_str().to_string(),
                        child_column: tc.column_name.as_str().to_string(),
                    });
                }
            }
        }
    }

    // first accumulate columns, then insert, as not to shift created references indexes
    let mut columns_to_insert_to_tables = Vec::with_capacity(tables_to_extend.len());
    for (table_idx, extension_columns) in tables_to_extend.iter() {
        for (ext_t, ext_c) in extension_columns.iter() {
            let parent_table = &res.tables[*ext_t];
            let parent_column = &parent_table.columns[*ext_c];
            let new_column = DataColumn {
                column_name: parent_column.column_name.clone(),
                generate_expression: parent_column.generate_expression.clone(),
                data: parent_column.data.new_like_this(),
                key_type: KeyType::ParentPrimary {
                    parent_table: parent_table.name.clone(),
                },
                maybe_foreign_key: None,
                is_snake_case_restricted: false,
            };
            columns_to_insert_to_tables.push((*table_idx, new_column));
        }
    }

    for (tidx, nc) in columns_to_insert_to_tables {
        res.tables[tidx].columns.insert(0, nc)
    }

    Ok(())
}

fn find_child_primary_keys_recursive_loop(
    res: &AllData,
    next_table: &str,
    child_stack: &mut Vec<String>,
    check_set: &mut HashSet<String>,
) -> Result<(), DatabaseValidationError> {
    child_stack.push(next_table.to_string());

    if !check_set.insert(next_table.to_string()) {
        Err(DatabaseValidationError::ChildPrimaryKeysLoopDetected {
            table_names: child_stack.clone(),
        })
    } else {
        let parent_tables: Vec<_> = res
            .tables
            .iter()
            .filter(|i| i.name.as_str() == next_table)
            .collect();
        // we should have checked this earlier with nice exception that parent tables exist everywhere
        assert_eq!(parent_tables.len(), 1);

        // we checked earlier that all good
        let parent_table = parent_tables[0].primary_key_column().unwrap();
        match &parent_table.key_type {
            KeyType::NotAKey => panic!("Should always be key here"),
            KeyType::Primary => Ok(()),
            KeyType::ChildPrimary { parent_table } => find_child_primary_keys_recursive_loop(
                res,
                parent_table.as_str(),
                child_stack,
                check_set,
            ),
            KeyType::ParentPrimary { .. } => {
                panic!("This branch should never be reached in this stage")
            }
        }
    }
}

fn find_parent_columns(res: &AllData, accum: &mut Vec<(usize, usize)>, table_name: &str) {
    for (idx, i) in res.tables.iter().enumerate() {
        if i.name.as_str() == table_name {
            for (idx2, c) in i.columns.iter().enumerate() {
                match &c.key_type {
                    KeyType::Primary => {
                        accum.push((idx, idx2));
                        break;
                    }
                    KeyType::ChildPrimary { parent_table } => {
                        accum.push((idx, idx2));
                        find_parent_columns(res, accum, parent_table.as_str());
                        break;
                    }
                    _ => {}
                }
            }
            break;
        }
    }
}

fn insert_main_data(
    res: &mut AllData,
    outputs: &SourceOutputs,
) -> Result<(), DatabaseValidationError> {
    for ds in outputs.table_data_segments() {
        match ds {
            TableDataSegment::DataFrame(df) => {
                let mut data_slices = Vec::with_capacity(df.data.len());
                let mut replacement_maps: Vec<Vec<(i32, usize, usize)>> = Vec::with_capacity(df.data.len());
                for i in df.data.iter() {
                    let mut row = Vec::with_capacity(i.value_fields.len());
                    let mut replacement_map: Vec<(i32, usize, usize)> = Vec::with_capacity(i.value_fields.len());
                    for f in i.value_fields.iter() {
                        row.push(f.value.as_str());
                        replacement_map.push((df.source_file_id, f.offset_start, f.offset_end));
                    }
                    data_slices.push(row);
                    replacement_maps.push(replacement_map);
                }
                let target_fields = df
                    .target_fields
                    .iter()
                    .map(|i| i.as_str())
                    .collect::<Vec<_>>();
                insert_table_data(
                    res,
                    df.target_table_name.as_str(),
                    &target_fields,
                    &data_slices,
                    &replacement_maps,
                    df.is_exclusive,
                )?;
            }
            TableDataSegment::StructuredData(sd) => insert_structured_data(res, sd)?,
        }
    }

    Ok(())
}

fn insert_structured_data(
    res: &mut AllData,
    sd: &TableDataStruct,
) -> Result<(), DatabaseValidationError> {
    let tname_id = DBIdentifier::new(sd.target_table_name.as_str())?;
    let tbl_idx = res.find_table_named_idx(&tname_id);
    if tbl_idx.is_empty() {
        return Err(DatabaseValidationError::TargetTableForDataNotFound {
            table_name: sd.target_table_name.to_string(),
        });
    }
    assert_eq!(tbl_idx.len(), 1);
    let tbl_idx = tbl_idx[0];

    if res.tables[tbl_idx].exclusive_lock {
        return Err(DatabaseValidationError::ExclusiveDataDefinedMultipleTimes {
            table_name: res.tables[tbl_idx].name.as_str().to_string(),
        });
    }

    let replacements = res.table_replacements.get(&tbl_idx);
    let prim_key_idxs = res.tables[tbl_idx].primary_keys_with_parents();

    for row in &sd.map {
        let mut uniq_fields = HashMap::with_capacity(row.value_fields.len());
        for (row_kv_idx, pair) in row.value_fields.iter().enumerate() {
            // check for duplicate fields
            if uniq_fields
                .insert(DBIdentifier::new(pair.key.as_str())?, row_kv_idx)
                .is_some()
            {
                return Err(DatabaseValidationError::DuplicateStructuredDataFields {
                    table_name: sd.target_table_name.clone(),
                    duplicated_column: pair.key.clone(),
                });
            }
        }

        for pair in row.value_fields.iter() {
            let column_name_dbi = DBIdentifier::new(pair.key.as_str())?;
            // check if column exists
            let column = res.tables[tbl_idx].find_column_named_idx(&column_name_dbi);
            if column.is_empty() {
                return Err(DatabaseValidationError::DataTargetColumnNotFound {
                    table_name: sd.target_table_name.clone(),
                    target_column_name: pair.key.clone(),
                });
            }

            assert_eq!(column.len(), 1);

            // check if all required columns are present
            let required_columns = res.tables[tbl_idx].required_table_columns();
            for rq in required_columns {
                if !uniq_fields.contains_key(&rq) {
                    return Err(
                        DatabaseValidationError::DataRequiredNonDefaultColumnValueNotProvided {
                            table_name: sd.target_table_name.clone(),
                            column_name: rq.as_str().to_string(),
                        },
                    );
                }
            }
        }

        let mut row_replacement = None;
        if let Some(replacements) = replacements {
            let mut composite_key: Vec<&str> = Vec::new();
            for prim_idx in &prim_key_idxs {
                if let Some(sc) = uniq_fields.get(&res.tables[tbl_idx].columns[*prim_idx].column_name) {
                    composite_key.push(&row.value_fields[*sc].value.value);
                }
            }
            let composite_key = composite_key.join("=>");
            if let Some(replacement) = replacements.get(&composite_key) {
                // can only come from lua
                if sd.source_file_id < 0 {
                    return Err(DatabaseValidationError::ReplacementOverLuaGeneratedValuesIsNotSupported {
                        table: res.tables[tbl_idx].name.as_str().to_string(),
                        replacement_primary_key: composite_key,
                    });
                }

                *replacement.use_count.borrow_mut() += 1;
                row_replacement = Some(replacement);
            }
        }

        for col in &mut res.tables[tbl_idx].columns {
            let is_required = col.is_required();
            let kv_idx = uniq_fields.get(&col.column_name);

            // primary key is always first column, we can rely on that
            if col.generate_expression.is_some() {
                if kv_idx.is_some() {
                    return Err(
                        DatabaseValidationError::ComputerColumnCannotBeExplicitlySpecified {
                            table_name: tname_id.as_str().to_string(),
                            column_name: col.column_name.as_str().to_string(),
                            compute_expression: col.generate_expression.as_ref().unwrap().clone(),
                        },
                    );
                }

                col.data.push_dummy_values(1);
            } else {
                match &mut col.data {
                    ColumnVector::Strings(v) => {
                        if !is_required && kv_idx.is_none() {
                            let res = v.push_default_value();
                            assert!(res, "Default value is assumed to exist here");
                        } else {
                            let kv_idx = *kv_idx.unwrap();
                            let to_push =
                                if let Some(row_replacement) = &row_replacement {
                                    if let Some(col_replacement) = row_replacement.values.get(col.column_name.as_str()) {
                                        res.source_replacements.push(
                                            ScheduledValueReplacementInSource {
                                                source_file_idx: sd.source_file_id,
                                                offset_start: row.value_fields[kv_idx].value.offset_start,
                                                offset_end: row.value_fields[kv_idx].value.offset_end,
                                                value_to_replace_with: col_replacement.clone(),
                                            }
                                        );
                                        col_replacement
                                    } else {
                                        &row.value_fields[kv_idx].value.value
                                    }
                                } else {
                                    &row.value_fields[kv_idx].value.value
                                };
                            v.v.push(to_push.clone());
                        }
                    }
                    ColumnVector::Ints(v) => {
                        if !is_required && kv_idx.is_none() {
                            // insert default value
                            let res = v.push_default_value();
                            assert!(res, "Default value is assumed to exist here");
                        } else {
                            let kv_idx = *kv_idx.unwrap();
                            let to_push =
                                if let Some(row_replacement) = &row_replacement {
                                    let to_use = row_replacement.values.get(col.column_name.as_str()).unwrap();
                                    res.source_replacements.push(
                                        ScheduledValueReplacementInSource {
                                            source_file_idx: sd.source_file_id,
                                            offset_start: row.value_fields[kv_idx].value.offset_start,
                                            offset_end: row.value_fields[kv_idx].value.offset_end,
                                            value_to_replace_with: to_use.clone(),
                                        }
                                    );
                                to_use
                            } else {
                                &row.value_fields[kv_idx].value.value
                            };
                            match to_push.parse::<i64>() {
                                Ok(i) => {
                                    v.v.push(i);
                                }
                                Err(_) => {
                                    return Err(
                                        DatabaseValidationError::DataCannotParseDataStructColumnValue {
                                            table_name: sd.target_table_name.clone(),
                                            column_name: col.column_name.as_str().to_string(),
                                            expected_type: col.data.column_type(),
                                            column_value: row.value_fields[kv_idx].value.value.clone(),
                                        },
                                    );
                                }
                            }
                        }
                    }
                    ColumnVector::Floats(v) => {
                        if !is_required && kv_idx.is_none() {
                            // insert default value
                            let res = v.push_default_value();
                            assert!(res, "Default value is assumed to exist here");
                        } else {
                            let kv_idx = *kv_idx.unwrap();
                            let to_push =
                                if let Some(row_replacement) = &row_replacement {
                                    let to_use = row_replacement.values.get(col.column_name.as_str()).unwrap();
                                    res.source_replacements.push(
                                        ScheduledValueReplacementInSource {
                                            source_file_idx: sd.source_file_id,
                                            offset_start: row.value_fields[kv_idx].value.offset_start,
                                            offset_end: row.value_fields[kv_idx].value.offset_end,
                                            value_to_replace_with: to_use.clone(),
                                        }
                                    );
                                to_use
                            } else {
                                &row.value_fields[kv_idx].value.value
                            };
                            match to_push.parse::<f64>() {
                                Ok(i) => {
                                    v.v.push(i);
                                }
                                Err(_) => {
                                    return Err(
                                        DatabaseValidationError::DataCannotParseDataStructColumnValue {
                                            table_name: sd.target_table_name.clone(),
                                            column_name: col.column_name.as_str().to_string(),
                                            expected_type: col.data.column_type(),
                                            column_value: row.value_fields[kv_idx].value.value.clone(),
                                        },
                                    );
                                }
                            }
                        }
                    }
                    ColumnVector::Bools(v) => {
                        if !is_required && kv_idx.is_none() {
                            // insert default value
                            let res = v.push_default_value();
                            assert!(res, "Default value is assumed to exist here");
                        } else {
                            let kv_idx = *kv_idx.unwrap();
                            let to_push =
                                if let Some(row_replacement) = &row_replacement {
                                    let to_use = row_replacement.values.get(col.column_name.as_str()).unwrap();
                                    res.source_replacements.push(
                                        ScheduledValueReplacementInSource {
                                            source_file_idx: sd.source_file_id,
                                            offset_start: row.value_fields[kv_idx].value.offset_start,
                                            offset_end: row.value_fields[kv_idx].value.offset_end,
                                            value_to_replace_with: to_use.clone(),
                                        }
                                    );
                                to_use
                            } else {
                                &row.value_fields[kv_idx].value.value
                            };
                            match to_push.parse::<bool>() {
                                Ok(i) => {
                                    v.v.push(i);
                                }
                                Err(_) => {
                                    return Err(
                                        DatabaseValidationError::DataCannotParseDataStructColumnValue {
                                            table_name: sd.target_table_name.clone(),
                                            column_name: col.column_name.as_str().to_string(),
                                            expected_type: col.data.column_type(),
                                            column_value: row.value_fields[kv_idx].value.value.clone(),
                                        },
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        if sd.is_exclusive {
            res.tables[tbl_idx].exclusive_lock = true;
        }
    }

    Ok(())
}

fn recur_check_exclusive_data_violations_structured(
    counts: &mut HashMap<String, Vec<bool>>,
    table_data: &TableDataStruct,
) {
    {
        let entry = counts
            .entry(table_data.target_table_name.clone())
            .or_default();
        entry.push(table_data.is_exclusive);
    }

    for i in &table_data.map {
        for e in &i.extra_data {
            recur_check_exclusive_data_violations_structured(counts, e);
        }
    }
}

fn recur_check_exclusive_data_violations(
    counts: &mut HashMap<String, Vec<bool>>,
    table_data: &TableData,
) {
    {
        let entry = counts
            .entry(table_data.target_table_name.clone())
            .or_default();
        entry.push(table_data.is_exclusive);
    }

    for i in &table_data.data {
        for e in &i.extra_data {
            recur_check_exclusive_data_violations(counts, e);
        }
    }
}

fn check_exclusive_data_violations(
    table_data_slice: &[TableDataSegment],
) -> Result<(), DatabaseValidationError> {
    let mut counts: HashMap<String, Vec<bool>> = HashMap::new();

    for i in table_data_slice {
        match i {
            TableDataSegment::DataFrame(df) => {
                recur_check_exclusive_data_violations(&mut counts, df);
            }
            TableDataSegment::StructuredData(sd) => {
                recur_check_exclusive_data_violations_structured(&mut counts, sd);
            }
        }
    }

    for (k, v) in counts {
        let count = v.iter().filter(|i| **i).count();

        if count > 0 && v.len() > 1 {
            return Err(DatabaseValidationError::ExclusiveDataDefinedMultipleTimes {
                table_name: k,
            });
        }
    }

    Ok(())
}

fn maybe_insert_lua_data(
    res: &mut AllData,
    outputs: &SourceOutputs,
) -> Result<(), DatabaseValidationError> {
    let segments = outputs.lua_segments();
    let mut maps_to_push = Vec::new();
    if !segments.is_empty() {
        let lua = res.lua_runtime.lock().unwrap();

        // if someone modified this variable in lua and it panics its their fault
        let root_table: mlua::Table = lua
            .globals()
            .get("__do_not_refer_to_this_internal_value_in_your_code_dumbo__")
            .map_err(|e| DatabaseValidationError::LuaDataTableError {
                error: e.to_string(),
            })?;

        for pair in root_table.pairs::<mlua::Value, mlua::Value>() {
            let (k, v) = pair.map_err(|e| DatabaseValidationError::LuaDataTableError {
                error: e.to_string(),
            })?;

            let expected_table = if let mlua::Value::String(k) = &k {
                String::from_utf8(k.as_bytes().to_vec()).map_err(|_| {
                    DatabaseValidationError::LuaDataTableInvalidKeyTypeIsNotValidUtf8String {
                        lossy_value: String::from_utf8_lossy(k.as_bytes()).to_string(),
                        bytes: k.as_bytes().to_vec(),
                    }
                })?
            } else {
                return Err(
                    DatabaseValidationError::LuaDataTableInvalidKeyTypeIsNotString {
                        found_value: lua_value_to_string_descriptive(&k),
                    },
                );
            };

            let values_array = if let mlua::Value::Table(t) = v {
                t
            } else {
                return Err(DatabaseValidationError::LuaDataTableInvalidTableValue {
                    found_value: lua_value_to_string_descriptive(&v),
                });
            };

            match res
                .tables
                .iter_mut()
                .find(|i| i.name.as_str() == expected_table)
            {
                Some(t) => {
                    let mut td_struct = TableDataStruct {
                        target_table_name: t.name.as_str().to_string(),
                        is_exclusive: false,
                        map: vec![],
                        source_file_id: -1,
                    };
                    for v in values_array.sequence_values::<mlua::Value>() {
                        let v = v.map_err(|e| DatabaseValidationError::LuaDataTableError {
                            error: e.to_string(),
                        })?;

                        if let mlua::Value::Table(record) = v {
                            let mut fields = TableDataStructFields {
                                value_fields: vec![],
                                extra_data: vec![],
                            };
                            for rec_field in record.pairs::<mlua::Value, mlua::Value>() {
                                let (column, the_val) = rec_field.map_err(|e| {
                                    DatabaseValidationError::LuaDataTableError {
                                        error: e.to_string(),
                                    }
                                })?;

                                let row_key = if let mlua::Value::String(column) = &column {
                                    String::from_utf8(column.as_bytes().to_vec()).map_err(|_| {
                                        DatabaseValidationError::LuaDataTableRecordInvalidColumnNameUtf8String {
                                            lossy_value: String::from_utf8_lossy(column.as_bytes()).to_string(),
                                            bytes: column.as_bytes().to_vec(),
                                        }
                                    })?
                                } else {
                                    return Err(DatabaseValidationError::LuaDataTableInvalidRecordColumnNameValue {
                                        found_value: lua_value_to_string_descriptive(&column),
                                    });
                                };

                                match &the_val {
                                    mlua::Value::Boolean(_)
                                    | mlua::Value::Integer(_)
                                    | mlua::Value::Number(_)
                                    | mlua::Value::String(_) => {}
                                    _ => {
                                        return Err(DatabaseValidationError::LuaDataTableRecordInvalidColumnValue {
                                            column_name: row_key,
                                            column_value: lua_value_to_string_descriptive(&the_val),
                                        });
                                    }
                                }

                                let final_v = lua_value_to_string(&the_val);
                                fields.value_fields.push(TableDataStructField {
                                    key: row_key,
                                    value: ValueWithPos {
                                        value: final_v,
                                        offset_start: 0,
                                        offset_end: 0,
                                    },
                                });
                            }

                            td_struct.map.push(fields);
                        } else {
                            return Err(DatabaseValidationError::LuaDataTableInvalidRecordValue {
                                found_value: lua_value_to_string_descriptive(&v),
                            });
                        }
                    }

                    maps_to_push.push(td_struct);
                }
                None => {
                    return Err(DatabaseValidationError::LuaDataTableNoSuchTable {
                        expected_insertion_table: expected_table.to_string(),
                    });
                }
            }
        }
    }

    // insert the data
    for seg in &maps_to_push {
        insert_structured_data(res, seg)?;
    }

    Ok(())
}

fn insert_extra_data(
    res: &mut AllData,
    table_data_slice: &[TableDataSegment],
) -> Result<(), DatabaseValidationError> {
    for ds in table_data_slice {
        match ds {
            TableDataSegment::DataFrame(df) => {
                insert_extra_data_dataframes(res, std::slice::from_ref(df), &[])?
            }
            TableDataSegment::StructuredData(sd) => {
                insert_extra_data_structured(res, std::slice::from_ref(sd), &[])?
            }
        }
    }

    Ok(())
}

fn insert_extra_data_structured(
    res: &mut AllData,
    table_data_slice: &[TableDataStruct],
    parent_primary_key_stack: &[ContextualInsertStackItem],
) -> Result<(), DatabaseValidationError> {
    for ds in table_data_slice {
        let main_table_name = DBIdentifier::new(ds.target_table_name.as_str())?;

        // there's only one foreign key reference, we're good,
        // insert data
        let parent_table_idx = res.find_table_named_idx(&main_table_name);

        assert_eq!(parent_table_idx.len(), 1);
        let parent_table_idx = parent_table_idx[0];

        let parent_implicit_primary_column_idx =
            res.tables[parent_table_idx].implicit_parent_primary_keys();

        // what to push to stack? either this row's child primary key or primary key
        for row in ds.map.iter().filter(|i| !i.extra_data.is_empty()) {
            if parent_implicit_primary_column_idx.is_empty() {
                return Err(DatabaseValidationError::ExtraDataParentMustHavePrimaryKey {
                    parent_table: ds.target_table_name.clone(),
                });
            }

            assert!(!parent_implicit_primary_column_idx.is_empty());

            let primary_parent_column_names = parent_implicit_primary_column_idx
                .iter()
                .map(|i| &res.tables[parent_table_idx].columns[*i].column_name)
                .collect::<Vec<_>>();

            let this_row_primary_key_values: Vec<ContextualInsertStackItem> =
                parent_primary_key_stack.into();

            let this_main_df_primary_key_columns: Vec<usize> = row
                .value_fields
                .iter()
                .enumerate()
                .filter_map(|(idx, kv_pair)| {
                    for ppcn in &primary_parent_column_names {
                        if kv_pair.key == ppcn.as_str() {
                            return Some(idx);
                        }
                    }

                    None
                })
                .collect();

            // if this got inserted and recursed here, and didn't fail earlier
            // then we must have valid primary key column
            assert_eq!(this_main_df_primary_key_columns.len(), 1);

            let mut new_key_stack_item;

            // eprintln!("tfields {:?}", target_fields);
            // eprintln!("slices  {:?}", data_slices);
            for extra in row.extra_data.iter() {
                // extra data cannot be the same column name
                let extra_id = DBIdentifier::new(extra.target_table_name.as_str())?;
                if extra_id.as_str() == ds.target_table_name.as_str() {
                    return Err(DatabaseValidationError::ExtraDataRecursiveInsert {
                        parent_table: ds.target_table_name.clone(),
                        extra_table: extra_id.as_str().to_string(),
                    });
                }

                let extra_table: Vec<_> = res.find_table_named_idx(&extra_id);

                if extra_table.is_empty() {
                    return Err(DatabaseValidationError::ExtraDataTableNotFound {
                        parent_table: ds.target_table_name.to_string(),
                        extra_table: extra_id.as_str().to_string(),
                    });
                }

                assert_eq!(extra_table.len(), 1);

                let extra_table_idx = extra_table[0];
                // extra table MUST have a single foreign key as primary key of this table

                let insertion_mode = res.tables[parent_table_idx]
                    .determine_nested_insertion_mode(&res.tables[extra_table_idx]);

                match insertion_mode {
                    NestedInsertionMode::TablesUnrelated => {
                        return Err(
                            DatabaseValidationError::ExtraTableHasNoForeignKeysToThisTable {
                                parent_table: ds.target_table_name.to_string(),
                                extra_table: extra_id.as_str().to_string(),
                            },
                        );
                    }
                    NestedInsertionMode::AmbigousForeignKeys { column_list } => {
                        return Err(DatabaseValidationError::ExtraTableMultipleAmbigousForeignKeysToThisTable {
                                    parent_table: ds.target_table_name.to_string(),
                                    extra_table: extra_id.as_str().to_string(),
                                    column_list: column_list.into_iter().map(|i| {
                                        res.tables[extra_table_idx].columns[i].column_name.as_str().to_string()
                                    }).collect(),
                                });
                    }
                    NestedInsertionMode::ForeignKeyMode { foreign_key_column } => {
                        let fkey_col = &res.tables[extra_table_idx].columns[foreign_key_column];

                        for i in extra.map.iter() {
                            for j in &i.value_fields {
                                if j.key == fkey_col.column_name.as_str() {
                                    return Err(
                                            DatabaseValidationError::ExtraTableCannotRedefineReferenceKey {
                                                parent_table: ds.target_table_name.to_string(),
                                                extra_table: extra_id.as_str().to_string(),
                                                column_name: j.key.clone(),
                                            },
                                        );
                                }
                            }
                        }

                        new_key_stack_item = this_main_df_primary_key_columns
                            .iter()
                            .map(|i| ContextualInsertStackItem {
                                table: res.tables[parent_table_idx].name.as_str().to_string(),
                                key: fkey_col.column_name.as_str().to_string(),
                                value: row.value_fields[*i].value.value.clone(),
                            })
                            .collect::<Vec<_>>();
                    }
                    NestedInsertionMode::ChildPrimaryKeyMode { parent_key_columns } => {
                        for i in extra.map.iter() {
                            for j in &i.value_fields {
                                for pkey in &parent_key_columns {
                                    if j.key
                                        == res.tables[extra_table_idx].columns[*pkey]
                                            .column_name
                                            .as_str()
                                    {
                                        return Err(
                                                DatabaseValidationError::ExtraTableCannotRedefineReferenceKey {
                                                    parent_table: ds.target_table_name.to_string(),
                                                    extra_table: extra_id.as_str().to_string(),
                                                    column_name: j.key.clone(),
                                                },
                                            );
                                    }
                                }
                            }
                        }

                        let last_col_idx = *parent_key_columns.last().unwrap();
                        new_key_stack_item = this_main_df_primary_key_columns
                            .iter()
                            .map(|i| ContextualInsertStackItem {
                                table: res.tables[parent_table_idx].name.as_str().to_string(),
                                key: res.tables[parent_table_idx].columns[last_col_idx]
                                    .column_name
                                    .as_str()
                                    .to_string(),
                                value: row.value_fields[*i].value.value.clone(),
                            })
                            .collect::<Vec<_>>();
                    }
                }

                assert_eq!(new_key_stack_item.len(), 1);
                let mut this_row_primary_key_values = this_row_primary_key_values.clone();
                this_row_primary_key_values.extend(new_key_stack_item);

                for col in &res.tables[parent_table_idx].columns {
                    if let KeyType::ParentPrimary { parent_table } = &col.key_type {
                        let found = this_row_primary_key_values
                            .iter()
                            .any(|i| i.key == col.column_name.as_str());
                        if !found {
                            if let Some(v) = row
                                .value_fields
                                .iter()
                                .find(|i| i.key == col.column_name.as_str())
                            {
                                let new_item = ContextualInsertStackItem {
                                    table: parent_table.as_str().to_string(),
                                    key: col.column_name.as_str().to_string(),
                                    value: v.value.value.clone(),
                                };
                                this_row_primary_key_values.insert(0, new_item);
                            }
                        }
                    }
                }

                // eprintln!("staxa   {:?}", this_row_primary_key_values);

                for old_i in &this_row_primary_key_values {
                    if extra.target_table_name == old_i.table {
                        let mut table_loop = this_row_primary_key_values
                            .iter()
                            .map(|i| i.table.clone())
                            .collect::<Vec<_>>();
                        table_loop.push(extra.target_table_name.clone());
                        return Err(
                            DatabaseValidationError::CyclingTablesInContextualInsertsNotAllowed {
                                table_loop,
                            },
                        );
                    }
                }

                for i in extra.map.iter() {
                    let mut data_slices = Vec::with_capacity(extra.map.len());
                    let mut replacement_maps: Vec<Vec<(i32, usize, usize)>> = Vec::with_capacity(extra.map.len());
                    let row_target_size = i.value_fields.len() + this_row_primary_key_values.len();
                    let mut replacement_map: Vec<(i32, usize, usize)> = Vec::with_capacity(row_target_size);
                    let mut row_values = Vec::with_capacity(row_target_size);
                    let mut target_fields = Vec::new();
                    for pkv in &this_row_primary_key_values {
                        target_fields.push(pkv.key.as_str());
                        row_values.push(pkv.value.as_str());
                        replacement_map.push((-1, 0, 0));
                    }
                    for f in i.value_fields.iter() {
                        target_fields.push(f.key.as_str());
                        row_values.push(f.value.value.as_str());
                        replacement_map.push((ds.source_file_id, f.value.offset_start, f.value.offset_end));
                    }
                    data_slices.push(row_values);
                    replacement_maps.push(replacement_map);

                    // eprintln!("stack   {:?}", this_row_primary_key_values);
                    // eprintln!("tfields {:?}", target_fields);
                    // eprintln!("slices  {:?}", data_slices);

                    insert_table_data(
                        res,
                        extra.target_table_name.as_str(),
                        target_fields.as_slice(),
                        &data_slices,
                        &replacement_maps,
                        false,
                    )?;
                }

                insert_extra_data_structured(
                    res,
                    std::slice::from_ref(extra),
                    &this_row_primary_key_values,
                )?
            }
        }
    }

    Ok(())
}

// Insert data regarding WITH statements
fn insert_extra_data_dataframes(
    res: &mut AllData,
    table_data_slice: &[TableData],
    parent_primary_key_stack: &[ContextualInsertStackItem],
) -> Result<(), DatabaseValidationError> {
    for ds in table_data_slice {
        let main_table_name = DBIdentifier::new(ds.target_table_name.as_str())?;

        // there's only one foreign key reference, we're good,
        // insert data
        let parent_table_idx = res.find_table_named_idx(&main_table_name);

        assert_eq!(parent_table_idx.len(), 1);
        let parent_table_idx = parent_table_idx[0];

        let parent_implicit_primary_column_idx =
            res.tables[parent_table_idx].implicit_parent_primary_keys();

        for (parent_row_idx, row) in ds
            .data
            .iter()
            .filter(|i| !i.extra_data.is_empty())
            .enumerate()
        {
            if parent_implicit_primary_column_idx.is_empty() {
                return Err(DatabaseValidationError::ExtraDataParentMustHavePrimaryKey {
                    parent_table: ds.target_table_name.clone(),
                });
            }

            assert!(!parent_implicit_primary_column_idx.is_empty());

            let this_main_df_primary_key_columns: Vec<usize>;
            let primary_parent_column_names = parent_implicit_primary_column_idx
                .iter()
                .map(|i| &res.tables[parent_table_idx].columns[*i].column_name)
                .collect::<Vec<_>>();

            let mut this_row_primary_key_values: Vec<ContextualInsertStackItem> =
                parent_primary_key_stack.into();

            let new_key_stack_item;

            if !ds.target_fields.is_empty() {
                this_main_df_primary_key_columns = ds
                    .target_fields
                    .iter()
                    .enumerate()
                    .filter_map(|tf| {
                        for ppcn in &primary_parent_column_names {
                            if tf.1 == ppcn.as_str() {
                                return Some(tf.0);
                            }
                        }

                        None
                    })
                    .collect();

                new_key_stack_item = this_main_df_primary_key_columns
                    .iter()
                    .map(|i| ContextualInsertStackItem {
                        table: res.tables[parent_table_idx].name.as_str().to_string(),
                        key: res.tables[parent_table_idx].columns[*i]
                            .column_name
                            .as_str()
                            .to_string(),
                        value: row.value_fields[*i].value.clone(),
                    })
                    .collect::<Vec<_>>();
            } else {
                this_main_df_primary_key_columns = res.tables[parent_table_idx]
                    .default_tuple_order()
                    .iter()
                    .filter_map(|tf| {
                        for ppcn in &primary_parent_column_names {
                            if tf.1.column_name.as_str() == ppcn.as_str() {
                                return Some(tf.0);
                            }
                        }
                        None
                    })
                    .collect();

                new_key_stack_item = this_main_df_primary_key_columns
                    .iter()
                    .map(|i| ContextualInsertStackItem {
                        table: res.tables[parent_table_idx].name.as_str().to_string(),
                        key: res.tables[parent_table_idx].columns[*i]
                            .column_name
                            .as_str()
                            .to_string(),
                        value: row.value_fields[*i - this_row_primary_key_values.len()].value.clone(),
                    })
                    .collect::<Vec<_>>();
            }

            // if this got inserted and recursed here, and didn't fail earlier
            // then we must have valid primary key column
            assert_eq!(this_main_df_primary_key_columns.len(), 1);

            assert_eq!(new_key_stack_item.len(), 1);
            this_row_primary_key_values.extend(new_key_stack_item);

            // if we're missing some parent keys try to find them in parent dataframe
            let mut tfields = ds.target_fields.clone();
            if tfields.is_empty() {
                tfields.extend(
                    res.tables[parent_table_idx]
                        .default_tuple_order()
                        .into_iter()
                        .map(|i| i.1.column_name.as_str().to_string()),
                );
            }

            for col in &res.tables[parent_table_idx].columns {
                if let KeyType::ParentPrimary { parent_table } = &col.key_type {
                    let found = this_row_primary_key_values
                        .iter()
                        .any(|i| i.key == col.column_name.as_str());
                    if !found {
                        if let Some((cidx, _)) = tfields
                            .iter()
                            .enumerate()
                            .find(|i| i.1 == col.column_name.as_str())
                        {
                            let new_item = ContextualInsertStackItem {
                                table: parent_table.as_str().to_string(),
                                key: col.column_name.as_str().to_string(),
                                value: ds.data[parent_row_idx].value_fields[cidx].value.clone(),
                            };
                            this_row_primary_key_values.insert(0, new_item);
                        }
                    }
                }
            }

            // TODO: insert the parent primary key with context here
            // eprintln!("_____________________________________________");
            // eprintln!("{}", ds.target_table_name);
            // eprintln!("s {:?}", parent_primary_key_stack);
            // eprintln!("c {:?}", this_main_df_primary_key_columns);
            // eprintln!("v {:?}", this_row_primary_key_values);
            // eprintln!("f {:?}", row.value_fields);
            // eprintln!("p {:?}", primary_parent_column_names);

            for extra in row.extra_data.iter() {
                // extra data cannot be the same column name
                let extra_id = DBIdentifier::new(extra.target_table_name.as_str())?;
                if extra_id.as_str() == ds.target_table_name.as_str() {
                    return Err(DatabaseValidationError::ExtraDataRecursiveInsert {
                        parent_table: ds.target_table_name.clone(),
                        extra_table: extra_id.as_str().to_string(),
                    });
                }

                for old_i in &this_row_primary_key_values {
                    if extra.target_table_name == old_i.table {
                        let mut table_loop = this_row_primary_key_values
                            .iter()
                            .map(|i| i.table.clone())
                            .collect::<Vec<_>>();
                        table_loop.push(extra.target_table_name.clone());
                        return Err(
                            DatabaseValidationError::CyclingTablesInContextualInsertsNotAllowed {
                                table_loop,
                            },
                        );
                    }
                }

                let extra_table: Vec<_> = res.find_table_named_idx(&extra_id);

                if extra_table.is_empty() {
                    return Err(DatabaseValidationError::ExtraDataTableNotFound {
                        parent_table: ds.target_table_name.to_string(),
                        extra_table: extra_id.as_str().to_string(),
                    });
                }

                assert_eq!(extra_table.len(), 1);

                let extra_table_idx = extra_table[0];
                // extra table MUST have a single foreign key as primary key of this table
                {
                    let mut target_fields = extra.target_fields.clone();
                    let has_fields_set = !extra.target_fields.is_empty();

                    let insertion_mode = res.tables[parent_table_idx]
                        .determine_nested_insertion_mode(&res.tables[extra_table_idx]);

                    match insertion_mode {
                        NestedInsertionMode::TablesUnrelated => {
                            return Err(
                                DatabaseValidationError::ExtraTableHasNoForeignKeysToThisTable {
                                    parent_table: ds.target_table_name.to_string(),
                                    extra_table: extra_id.as_str().to_string(),
                                },
                            );
                        }
                        NestedInsertionMode::AmbigousForeignKeys { column_list } => {
                            return Err(DatabaseValidationError::ExtraTableMultipleAmbigousForeignKeysToThisTable {
                                    parent_table: ds.target_table_name.to_string(),
                                    extra_table: extra_id.as_str().to_string(),
                                    column_list: column_list.into_iter().map(|i| {
                                        res.tables[extra_table_idx].columns[i].column_name.as_str().to_string()
                                    }).collect(),
                                });
                        }
                        NestedInsertionMode::ForeignKeyMode { foreign_key_column } => {
                            let fkey_col = &res.tables[extra_table_idx].columns[foreign_key_column];

                            if has_fields_set {
                                for i in extra.target_fields.iter() {
                                    if i == fkey_col.column_name.as_str() {
                                        return Err(
                                            DatabaseValidationError::ExtraTableCannotRedefineReferenceKey {
                                                parent_table: ds.target_table_name.to_string(),
                                                extra_table: extra_id.as_str().to_string(),
                                                column_name: i.clone(),
                                            },
                                        );
                                    }
                                }

                                target_fields.insert(0, fkey_col.column_name.as_str().to_string());
                            } else {
                                // insert relevant foregin keys first
                                if let Some(ForeignKey { foreign_table, .. }) =
                                    &fkey_col.maybe_foreign_key
                                {
                                    for pkey in &this_row_primary_key_values {
                                        if pkey.table == foreign_table.as_str() {
                                            target_fields
                                                .push(fkey_col.column_name.as_str().to_string());
                                        }
                                    }
                                } else {
                                    panic!("Dereference of foreign key type must work here, or insertion mode function is wrong")
                                }

                                // insert all default values except those that were defined
                                for (_, col) in
                                    res.tables[extra_table_idx].default_tuple_order().iter()
                                {
                                    let undefined_in_stack = !this_row_primary_key_values
                                        .iter()
                                        .any(|i| i.key == col.column_name.as_str());
                                    let undefined_in_fk = !target_fields
                                        .iter()
                                        .any(|i| i.as_str() == col.column_name.as_str());
                                    if undefined_in_fk && undefined_in_stack {
                                        target_fields.push(col.column_name.as_str().to_string());
                                    }
                                }
                            }
                        }
                        NestedInsertionMode::ChildPrimaryKeyMode { parent_key_columns } => {
                            if has_fields_set {
                                for i in extra.target_fields.iter() {
                                    for pkey in &parent_key_columns {
                                        if i == res.tables[extra_table_idx].columns[*pkey]
                                            .column_name
                                            .as_str()
                                        {
                                            return Err(
                                                DatabaseValidationError::ExtraTableCannotRedefineReferenceKey {
                                                    parent_table: ds.target_table_name.to_string(),
                                                    extra_table: extra_id.as_str().to_string(),
                                                    column_name: i.clone(),
                                                },
                                            );
                                        }
                                    }
                                }

                                // reverse iterator to first insert younger parent columns
                                for pkey in this_row_primary_key_values.iter().rev() {
                                    target_fields.insert(0, pkey.key.clone());
                                }
                            } else {
                                // find the first column name that we don't already have on stack
                                for pkey in this_row_primary_key_values.iter().rev() {
                                    target_fields.insert(0, pkey.key.clone());
                                }

                                for (_, col) in
                                    res.tables[extra_table_idx].default_tuple_order().iter()
                                {
                                    let not_yet_defined = !this_row_primary_key_values
                                        .iter()
                                        .any(|i| i.key == col.column_name.as_str());
                                    if not_yet_defined {
                                        target_fields.push(col.column_name.as_str().to_string());
                                    }
                                }
                            }
                        }
                    }

                    // how to recognize the key if it is not specified?
                    // by a certain order?

                    let mut data_slices = Vec::with_capacity(extra.data.len());
                    let mut replacement_maps: Vec<Vec<(i32, usize, usize)>> = Vec::with_capacity(extra.data.len());
                    for i in extra.data.iter() {
                        let target_row_size = i.value_fields.len() + this_row_primary_key_values.len();
                        let mut row_values: Vec<&str> = Vec::with_capacity(target_row_size);
                        let mut replacement_map: Vec<(i32, usize, usize)> = Vec::with_capacity(target_row_size);
                        for pkv in &this_row_primary_key_values {
                            row_values.push(pkv.value.as_str());
                            replacement_map.push((-1, 0, 0));
                        }
                        for f in i.value_fields.iter() {
                            row_values.push(f.value.as_str());
                            replacement_map.push((ds.source_file_id, f.offset_start, f.offset_end));
                        }
                        data_slices.push(row_values);
                        replacement_maps.push(replacement_map);
                    }
                    // eprintln!("tfields {:?}", target_fields);
                    // eprintln!("slices  {:?}", data_slices);
                    let target_fields =
                        target_fields.iter().map(|i| i.as_str()).collect::<Vec<_>>();
                    insert_table_data(
                        res,
                        extra.target_table_name.as_str(),
                        target_fields.as_slice(),
                        &data_slices,
                        &replacement_maps,
                        false,
                    )?;
                }
            }

            insert_extra_data_dataframes(res, &row.extra_data, &this_row_primary_key_values)?
        }
    }

    Ok(())
}

fn insert_table_data(
    res: &mut AllData,
    target_table_name: &str,
    target_table_fields: &[&str],
    input_data: &[Vec<&str>],
    source_replacement_map: &[Vec<(i32, usize, usize)>],
    is_exclusive: bool,
) -> Result<(), DatabaseValidationError> {
    let target_tbl_dbi = DBIdentifier::new(target_table_name)?;
    let target_table_idx = res.find_table_named_idx(&target_tbl_dbi);

    if target_table_idx.is_empty() {
        return Err(DatabaseValidationError::TargetTableForDataNotFound {
            table_name: target_table_name.to_string(),
        });
    }

    assert_eq!(target_table_idx.len(), 1);
    let target_table_idx = target_table_idx[0];

    if res.tables[target_table_idx].exclusive_lock {
        return Err(DatabaseValidationError::ExclusiveDataDefinedMultipleTimes {
            table_name: target_table_name.to_string(),
        });
    }

    if res.tables[target_table_idx].mat_view_expression.is_some() {
        return Err(
            DatabaseValidationError::DataInsertionsToMaterializedViewsNotAllowed {
                table_name: target_table_name.to_string(),
            },
        );
    }

    let mut source_replacements: Vec<ScheduledValueReplacementInSource> = Vec::new();
    let mut replacement_data: Vec<Vec<&str>> = Vec::new();
    let mut input_data_replaced: &[Vec<&str>] = input_data;
    if let Some(replacements) = res.table_replacements.get(&target_table_idx) {
        replacement_data.reserve_exact(input_data.len());
        let pkey_column = res.tables[target_table_idx].primary_key_column().unwrap();
        assert!(matches!(pkey_column.key_type, KeyType::Primary | KeyType::ChildPrimary { .. }));

        let key_idxs = res.tables[target_table_idx].primary_keys_with_parents();
        let mut dataframe_key_order: Vec<usize> = Vec::with_capacity(key_idxs.len());
        for key_idx in key_idxs {
            for (t_idx, tval) in target_table_fields.iter().enumerate() {
                if *tval == res.tables[target_table_idx].columns[key_idx].column_name.as_str() {
                    dataframe_key_order.push(t_idx);
                    break;
                }
            }
        }

        if !dataframe_key_order.is_empty() {
            for row_idx in 0..input_data.len() {
                let original_row = &input_data[row_idx];
                let mut new_row: Vec<&str> = Vec::with_capacity(original_row.len());
                let mut composite_key: Vec<&str> = Vec::new();
                for ord_idx in &dataframe_key_order {
                    composite_key.push(original_row[*ord_idx]);
                }
                let composite_key = composite_key.join("=>");
                if let Some(replacement) = replacements.get(&composite_key) {
                    let mut use_count = replacement.use_count.borrow_mut();
                    *use_count += 1;
                    for (f_idx, field) in target_table_fields.iter().enumerate() {
                        if let Some(f_repl) = replacement.values.get(*field) {
                            let (source_file_idx, offset_start, offset_end) = source_replacement_map[row_idx][f_idx];
                            assert!(source_file_idx >= 0, "Unknown source id, should never be reached");
                            source_replacements.push(ScheduledValueReplacementInSource {
                                source_file_idx, offset_start, offset_end, value_to_replace_with: f_repl.clone(),
                            });
                            new_row.push(&f_repl);
                        } else {
                            new_row.push(original_row[f_idx])
                        }
                    }
                } else {
                    for (f_idx, _) in target_table_fields.iter().enumerate() {
                        new_row.push(original_row[f_idx])
                    }
                }

                replacement_data.push(new_row);
            }
        }

        input_data_replaced = &replacement_data;
    }

    res.tables[target_table_idx].try_insert_dataframe(target_table_fields, input_data_replaced)?;
    res.source_replacements.extend(source_replacements);

    if is_exclusive {
        res.tables[target_table_idx].exclusive_lock = true;
    }

    Ok(())
}

fn map_parsed_column_to_data_column(
    input: &TableColumn,
    table_name: &str,
) -> Result<DataColumn, DatabaseValidationError> {
    let column_name = DBIdentifier::new(input.name.as_str())?;
    let forbid_default_value = || {
        if input.has_default_value() {
            return Err(DatabaseValidationError::PrimaryKeysCannotHaveDefaultValue {
                table_name: table_name.to_string(),
                column_name: column_name.as_str().to_string(),
            });
        }

        Ok(())
    };
    let forbid_computed_value = || {
        if input.generated_expression.is_some() {
            return Err(
                DatabaseValidationError::PrimaryOrForeignKeysCannotHaveComputedValue {
                    table_name: table_name.to_string(),
                    column_name: column_name.as_str().to_string(),
                },
            );
        }

        Ok(())
    };

    let key_type = if let Some(cpkey) = &input.child_primary_key {
        forbid_default_value()?;
        forbid_computed_value()?;

        KeyType::ChildPrimary {
            parent_table: DBIdentifier::new(cpkey.as_str())?,
        }
    } else if input.is_primary_key {
        forbid_default_value()?;
        forbid_computed_value()?;

        KeyType::Primary
    } else {
        KeyType::NotAKey
    };

    let maybe_foreign_key = if input.is_reference_to_other_table {
        forbid_computed_value()?;

        Some(ForeignKey {
            foreign_table: DBIdentifier::new(&input.the_type)?,
            is_to_foreign_child_table: input.is_reference_to_foreign_child_table,
            is_explicit_foreign_child_reference: input.is_explicit_foreign_child_reference,
            is_to_self_child_table: input.is_reference_to_self_child_table,
        })
    } else {
        None
    };

    let mut data = match &key_type {
        KeyType::NotAKey
        | KeyType::Primary
        | KeyType::ChildPrimary { parent_table: _ }
        | KeyType::ParentPrimary { parent_table: _ } => match input.the_type.as_str() {
            "TEXT" => ColumnVector::Strings(ColumnVectorGeneric {
                v: vec![],
                default_value: None,
            }),
            "INT" => ColumnVector::Ints(ColumnVectorGeneric {
                v: vec![],
                default_value: None,
            }),
            "FLOAT" => match &key_type {
                KeyType::NotAKey => ColumnVector::Floats(ColumnVectorGeneric {
                    v: vec![],
                    default_value: None,
                }),
                KeyType::Primary
                | KeyType::ChildPrimary { parent_table: _ }
                | KeyType::ParentPrimary { parent_table: _ } => {
                    return Err(DatabaseValidationError::FloatColumnCannotBePrimaryKey {
                        table_name: table_name.to_string(),
                        column_name: input.name.to_string(),
                    });
                }
            },
            "BOOL" => match &key_type {
                KeyType::NotAKey => ColumnVector::Bools(ColumnVectorGeneric {
                    v: vec![],
                    default_value: None,
                }),
                KeyType::Primary
                | KeyType::ChildPrimary { parent_table: _ }
                | KeyType::ParentPrimary { parent_table: _ } => {
                    return Err(DatabaseValidationError::BooleanColumnCannotBePrimaryKey {
                        table_name: table_name.to_string(),
                        column_name: input.name.to_string(),
                    });
                }
            },
            other => {
                if input.is_reference_to_other_table {
                    // defaults to string, in later pass is overriden to the correct type
                    ColumnVector::Strings(ColumnVectorGeneric {
                        v: vec![],
                        default_value: None,
                    })
                } else {
                    panic!("Unexpected type, should have been caught in validation stage: {other}")
                }
            }
        },
    };

    if input.has_default_value() && input.generated_expression.is_some() {
        return Err(
            DatabaseValidationError::DefaultValueAndComputedValueAreMutuallyExclusive {
                table_name: table_name.to_string(),
                column_name: column_name.as_str().to_string(),
            },
        );
    }

    match &input.default_expression {
        Some(input_value) => {
            let is_ok = data.try_set_default_value_from_string(input_value.as_str());
            if !is_ok {
                return Err(DatabaseValidationError::CannotParseDefaultColumnValue {
                    table_name: table_name.to_string(),
                    column_name: column_name.as_str().to_string(),
                    column_type: data.column_type(),
                    the_value: input_value.clone(),
                });
            }
        }
        None => {}
    };

    Ok(DataColumn {
        column_name,
        data,
        key_type,
        generate_expression: input.generated_expression.clone(),
        is_snake_case_restricted: false,
        maybe_foreign_key,
    })
}

fn check_is(input: &str, constant: &str) -> bool {
    input == constant
}

fn starts_with(input: &str, constant: &str) -> bool {
    input.starts_with(constant)
}

type ResColumnNames = &'static [(&'static str, &'static dyn Fn(&str, &str) -> bool)];

fn reserved_table_column_names() -> ResColumnNames {
    &[
        ("rowid", &check_is),         // sqlite special column
        ("parent", &check_is),        // generated column for storing parent row id
        ("children_", &starts_with),  // generated column for children of parents to table
        ("referrers_", &starts_with), // generated column for all values referred to by certain table
    ]
}

fn validate_table_definition(td: &TableDefinition) -> Option<DatabaseValidationError> {
    if td.name.to_lowercase() != td.name {
        return Some(DatabaseValidationError::TableNameIsNotLowercase {
            table_name: td.name.clone(),
        });
    }

    for i in td.columns.iter() {
        if i.name.to_lowercase() != i.name {
            return Some(DatabaseValidationError::ColumnNameIsNotLowercase {
                table_name: td.name.clone(),
                column_name: i.name.clone(),
            });
        }

        for (reserved, check) in reserved_table_column_names() {
            if check(i.name.as_str(), reserved) {
                return Some(DatabaseValidationError::ColumnNameIsReserved {
                    table_name: td.name.clone(),
                    column_name: i.name.clone(),
                    reserved_names: reserved_table_column_names()
                        .iter()
                        .map(|(i, _)| i.to_string())
                        .collect(),
                });
            }
        }
    }

    let pkeys_idx: Vec<_> = td
        .columns
        .iter()
        .enumerate()
        .filter_map(|(idx, i)| if i.is_primary_key { Some(idx) } else { None })
        .collect();
    if pkeys_idx.len() > 1 {
        return Some(DatabaseValidationError::MoreThanOnePrimaryKey {
            table_name: td.name.clone(),
        });
    }

    if !pkeys_idx.is_empty() && pkeys_idx[0] != 0 {
        return Some(DatabaseValidationError::PrimaryKeyColumnMustBeFirst {
            table_name: td.name.clone(),
            column_name: td.columns[pkeys_idx[0]].name.clone(),
        });
    }

    for i in &td.columns {
        match i.the_type.as_str() {
            "TEXT" | "INT" | "FLOAT" | "BOOL" => {}
            other => {
                // enforce references to other tables once all are processed
                if !i.is_reference_to_other_table {
                    return Some(DatabaseValidationError::UnknownColumnType {
                        table_name: td.name.clone(),
                        column_name: i.name.clone(),
                        column_type: other.to_string(),
                    });
                }
            }
        }
    }

    None
}

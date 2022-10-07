use std::{str::FromStr, collections::HashMap};

use regex::Regex;

use crate::db_parser::TableRowCheck;

use super::errors::DatabaseValidationError;

pub struct ColumnVectorGeneric<T: Clone + FromStr> {
    pub v: Vec<T>,
    pub default_value: Option<T>,
}

pub enum ColumnVector {
    Strings(ColumnVectorGeneric<String>),
    Ints(ColumnVectorGeneric<i64>),
    Floats(ColumnVectorGeneric<f64>),
    Bools(ColumnVectorGeneric<bool>),
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum KeyType {
    NotAKey,
    PrimaryKey,
    ChildPrimaryKey { parent_table: DBIdentifier },
    ParentPrimaryKey { parent_table: DBIdentifier },
    ForeignKey { foreign_table: DBIdentifier, is_to_child_table: bool },
}

impl KeyType {
    pub fn is_fkey_to_table(&self, table: &DBIdentifier) -> bool {
        match self {
            KeyType::ForeignKey { foreign_table, .. } => foreign_table == table,
            _ => false
        }
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum DBType {
    DBText,
    DBInt,
    DBFloat,
    DBBool,
}

pub struct DataColumn {
    pub column_name: DBIdentifier,
    pub data: ColumnVector,
    pub key_type: KeyType,
    pub generate_expression: Option<String>,
    pub is_snake_case_restricted: bool,
}

#[derive(Debug, Clone)]
pub struct UniqConstraint {
    pub fields: Vec<DBIdentifier>,
}

pub struct DataTable {
    pub name: DBIdentifier,
    pub columns: Vec<DataColumn>,
    pub uniq_constraints: Vec<UniqConstraint>,
    pub row_checks: Vec<TableRowCheck>,
    pub mat_view_expression: Option<String>,
    pub exclusive_lock: bool,
}

pub enum ConsistentStringDataframeValidationError {
    TooManyColumns {
        row_index: usize,
        row_size: usize,
        expected_size: usize,
    },
    TooFewColumns {
        row_index: usize,
        row_size: usize,
        expected_size: usize,
    },
    DuplicateFields {
        field_name: String,
    }
}

// validate column/row count
pub struct ConsistentDataFrameColumn<'a> {
    pub column_name: &'a str,
    pub column_data: Vec<&'a str>,
}

pub struct ConsistentStringDataframe<'a> {
    column_data: Vec<ConsistentDataFrameColumn<'a>>,
    column_index: HashMap<String, usize>,
}

#[derive(PartialEq, Eq, Debug, Clone, Hash)]
pub struct DBIdentifier(String);

impl DBIdentifier {
    pub fn new(input: &str) -> Result<DBIdentifier, DatabaseValidationError> {
        lazy_static! {
            static ref VALID_DB_ID: Regex = Regex::new("^[a-z0-9_]+$").unwrap();
        }

        if !VALID_DB_ID.is_match(input) {
            return Err(DatabaseValidationError::InvalidDBIdentifier(
                input.to_string(),
            ));
        }

        Ok(DBIdentifier(input.to_string()))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

pub enum NestedInsertionMode {
    ForeignKeyMode {
        foreign_key_column: usize,
    },
    ChildPrimaryKeyMode {
        parent_key_columns: Vec<usize>,
    },
    AmbigousForeignKeys {
        column_list: Vec<usize>,
    },
    TablesUnrelated,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct ContextualInsertStackItem {
    pub table: String,
    pub key: String,
    pub value: String,
}

pub struct SerializedVector<'a, T> {
    pub table_name: &'a str,
    pub column_name: String,
    pub v: &'a Vec<T>,
    pub last_for_table: bool,
}

pub enum SerializationVector<'a> {
    StringsColumn(SerializedVector<'a, String>),
    IntsColumn(SerializedVector<'a, i64>),
    FloatsColumn(SerializedVector<'a, f64>),
    BoolsColumn(SerializedVector<'a, bool>),
    FkeysColumn {
        sv: SerializedVector<'a, usize>,
        foreign_table: String,
    },
    FkeysOneToManyColumn {
        sv: SerializedVector<'a, Vec<usize>>,
        foreign_table: String,
    },
}

impl DataTable {
    pub fn default_tuple_order(&self) -> Vec<(usize, &DataColumn)> {
        self.columns
            .iter()
            .enumerate()
            .filter(|i| {
                i.1.is_required()
            })
            .collect()
    }

    pub fn primary_key_column(&self) -> Option<&DataColumn> {
        for i in &self.columns {
            match i.key_type {
                KeyType::PrimaryKey | KeyType::ChildPrimaryKey { .. } => return Some(i),
                _ => {}
            }
        }

        return None;
    }

    pub fn len(&self) -> usize {
        self.columns[0].data.len()
    }

    pub fn find_column_named_idx(&self, dbi: &DBIdentifier) -> Vec<usize> {
        let mut res = Vec::with_capacity(1);
        for (idx, i) in self.columns.iter().enumerate() {
            if i.column_name.as_str() == dbi.as_str() {
                res.push(idx);
            }
        }
        res
    }

    pub fn required_table_columns(&self) -> Vec<DBIdentifier> {
        let mut res = Vec::new();
        for i in &self.columns {
            if i.is_required() {
                res.push(i.column_name.clone())
            }
        }
        res.shrink_to_fit();
        res
    }

    fn prepare_columns_to_insert(&self, target_table_fields: &[&str]) -> Result<Vec<String>, DatabaseValidationError> {
        let mut columns_to_insert = Vec::with_capacity(target_table_fields.len());
        if target_table_fields.len() > 0 {
            for tf in target_table_fields.iter() {
                let tf_dbi = DBIdentifier::new(tf)?;
                let found: Vec<_> = self.find_column_named_idx(&tf_dbi);

                if found.is_empty() {
                    return Err(DatabaseValidationError::DataTargetColumnNotFound {
                        table_name: self.name.as_str().to_string(),
                        target_column_name: tf.to_string(),
                    });
                }
                assert_eq!(found.len(), 1);
                columns_to_insert.push(self.columns[found[0]].column_name.as_str().to_string());
            }
        } else {
            columns_to_insert = self
                .default_tuple_order()
                .iter()
                .map(|(idx, _)| self.columns[*idx].column_name.as_str().to_string())
                .collect();
        }

        Ok(columns_to_insert)
    }

    fn create_consistent_df_or_error<'a>(&self, columns_to_insert: &[&'a str], input_data: &Vec<Vec<&'a str>>)
        -> Result<ConsistentStringDataframe<'a>, DatabaseValidationError>
    {
        match ConsistentStringDataframe::new(columns_to_insert, input_data) {
            Ok(ok) => Ok(ok),
            Err(ConsistentStringDataframeValidationError::TooManyColumns {
                row_index,
                row_size,
                expected_size,
            }) => {
                return Err(DatabaseValidationError::DataTooManyColumns {
                    table_name: self.name.as_str().to_string(),
                    row_index,
                    row_size,
                    expected_size,
                });
            }
            Err(ConsistentStringDataframeValidationError::TooFewColumns {
                row_index,
                row_size,
                expected_size,
            }) => {
                return Err(DatabaseValidationError::DataTooFewColumns {
                    table_name: self.name.as_str().to_string(),
                    row_index,
                    row_size,
                    expected_size,
                });
            }
            Err(ConsistentStringDataframeValidationError::DuplicateFields { field_name }) => {
                return Err(DatabaseValidationError::DuplicateDataColumnNames {
                    table_name: self.name.as_str().to_string(),
                    column_name: field_name,
                });
            }
        }
    }

    pub fn try_insert_dataframe(
        &mut self,
        target_table_fields: &[&str],
        input_data: &Vec<Vec<&str>>,
    ) -> Result<(), DatabaseValidationError> {
        let columns_to_insert = self.prepare_columns_to_insert(target_table_fields)?;
        let columns_to_insert = columns_to_insert
            .iter()
            .map(|i| i.as_str())
            .collect::<Vec<_>>();

        let consistent_df = self.create_consistent_df_or_error(
            columns_to_insert.as_slice(), input_data
        )?;

        // check if we insert data where we don't provide columns
        // and there is no default value for the column
        for table_column in self.columns.iter_mut() {
            match consistent_df.column_by_name(table_column.column_name.as_str()) {
                Some((col_idx, df_column)) => {
                    match table_column
                        .data
                        .try_parse_and_append_vector(df_column.column_data.as_slice())
                    {
                        Err((idx, the_value)) => {
                            return Err(DatabaseValidationError::DataCannotParseDataColumnValue {
                                table_name: self.name.as_str().to_string(),
                                row_index: idx + 1,
                                column_index: col_idx + 1,
                                column_name: df_column.column_name.to_string(),
                                column_value: the_value,
                                expected_type: table_column.data.column_type(),
                            });
                        }
                        Ok(_) => {}
                    }
                },
                None => {
                    if !table_column.data.has_default_value() {
                        if table_column.generate_expression.is_some() {
                            // push dummy empty data to be computed later
                            table_column.data.push_dummy_values(consistent_df.len());
                        } else {
                            return Err(
                                DatabaseValidationError::DataRequiredNonDefaultColumnValueNotProvided {
                                    table_name: self.name.as_str().to_string(),
                                    column_name: table_column.column_name.as_str().to_string(),
                                },
                            );
                        }
                    } else {
                        table_column.data.push_default_values(consistent_df.len());
                    }
                },
            }
        }

        Ok(())
    }

    pub fn implicit_parent_primary_keys(&self) -> Vec<usize> {
        self
            .columns
            .iter()
            .enumerate()
            .filter_map(|i| {
                match i.1.key_type {
                    KeyType::PrimaryKey | KeyType::ChildPrimaryKey { .. } => Some(i.0),
                    _ => None,
                }
            })
            .collect()
    }

    pub fn determine_nested_insertion_mode(&self, child_table: &DataTable) -> NestedInsertionMode {
        let main_table_parent_key = KeyType::ParentPrimaryKey {
            parent_table: self.name.clone(),
        };

        let references: Vec<_> =
            child_table
                .columns
                .iter()
                .enumerate()
                .filter_map(|(idx, i)| {
                    if i.key_type.is_fkey_to_table(&self.name)
                        || i.key_type == main_table_parent_key
                    {
                        Some(idx)
                    } else {
                        None
                    }
                })
                .collect();

        let mut foreign_keys = Vec::new();
        let mut parent_pkey = Vec::new();
        for i in references.iter() {
            match &child_table.columns[*i].key_type {
                KeyType::ForeignKey { .. } => foreign_keys.push(*i),
                KeyType::ParentPrimaryKey { .. } => parent_pkey.push(*i),
                _ => {}
            }
        }

        let is_child_primary_key_mode = parent_pkey.len() > 0;
        let is_foreign_key_mode = !is_child_primary_key_mode;
        // parent primary key is always preferred instead of foreign key
        if is_foreign_key_mode {
            if foreign_keys.len() > 1 {
                return NestedInsertionMode::AmbigousForeignKeys { column_list: foreign_keys };
            } else if foreign_keys.len() == 1 {
                return NestedInsertionMode::ForeignKeyMode { foreign_key_column: foreign_keys[0] };
            } else {
                return NestedInsertionMode::TablesUnrelated;
            }
        } else {
            // we check this earlier with nice user error
            assert!(parent_pkey.len() > 0);
            return NestedInsertionMode::ChildPrimaryKeyMode {
                parent_key_columns: parent_pkey,
            };
        }
    }

    pub fn row_as_pretty_json(&self, row_idx: usize) -> Option<String> {
        use serde_json::Number;
        use serde_json::Value;
        if row_idx >= self.len() { return None }

        let mut row_value = serde_json::Map::default();

        for col in &self.columns {
            let cn = col.column_name.as_str().to_string();
            match &col.data {
                ColumnVector::Strings(v) => {
                    let i = row_value.insert(cn, Value::String(v.v[row_idx].clone()));
                    assert!(i.is_none());
                }
                ColumnVector::Ints(v) => {
                    let i = row_value.insert(
                        cn,
                        Value::Number(Number::from_f64(v.v[row_idx] as f64).unwrap()),
                    );
                    assert!(i.is_none());
                }
                ColumnVector::Floats(v) => {
                    let i = row_value
                        .insert(cn, Value::Number(Number::from_f64(v.v[row_idx]).unwrap()));
                    assert!(i.is_none());
                }
                ColumnVector::Bools(v) => {
                    let i = row_value.insert(cn, Value::Bool(v.v[row_idx]));
                    assert!(i.is_none());
                }
            }
        }

        Some(serde_json::to_string_pretty(&row_value).unwrap())
    }

    pub fn parent_table(&self) -> Option<DBIdentifier> {
        self.columns.iter().filter_map(|i| {
            match &i.key_type {
                KeyType::ParentPrimaryKey { parent_table } => {
                    Some(parent_table.clone())
                },
                _ => { None },
            }
        }).last()
    }
}

impl DataColumn {
    pub fn column_priority(&self) -> i32 {
        match &self.key_type {
            KeyType::PrimaryKey => 1,
            KeyType::ParentPrimaryKey { .. } => 2,
            KeyType::ChildPrimaryKey { .. } => 3,
            KeyType::ForeignKey { .. } => 10,
            KeyType::NotAKey => 10,
        }
    }

    pub fn is_required(&self) -> bool {
        match &self.key_type {
            KeyType::PrimaryKey => true,
            KeyType::ParentPrimaryKey { .. } => true,
            KeyType::ChildPrimaryKey { .. } => true,
            KeyType::ForeignKey { .. } => true,
            KeyType::NotAKey => {
                !self.data.has_default_value() && self.generate_expression.is_none()
            },
        }
    }

    pub fn sqlite_type_name(&self) -> &'static str {
        match self.data {
            ColumnVector::Strings(_) => "TEXT",
            ColumnVector::Ints(_) => "INTEGER",
            ColumnVector::Floats(_) => "REAL",
            ColumnVector::Bools(_) => "INTEGER",
        }
    }
}

impl<T: Clone + FromStr> ColumnVectorGeneric<T> {
    pub fn len(&self) -> usize {
        self.v.len()
    }

    pub fn new_like_this(&self) -> ColumnVectorGeneric<T> {
        ColumnVectorGeneric {
            v: vec![],
            default_value: None,
        }
    }

    pub fn has_default_value(&self) -> bool {
        self.default_value.is_some()
    }

    pub fn push_default_value(&mut self) -> bool {
        match &self.default_value {
            Some(df) => {
                self.v.push(df.clone());
                true
            }
            None => false,
        }
    }

    pub fn push_default_values(&mut self, count: usize) -> bool {
        match &self.default_value {
            Some(df) => {
                for _ in 0..count {
                    self.v.push(df.clone());
                }
                true
            }
            None => false,
        }
    }

    pub fn push_dummy_values(&mut self, dummy: &T, count: usize) {
        for _ in 0..count {
            self.v.push(dummy.clone());
        }
    }

    pub fn try_set_default_value_from_string(&mut self, input: &str) -> bool {
        match input.parse::<T>() {
            Ok(ok) => {
                self.default_value = Some(ok);
                true
            }
            Err(_) => false,
        }
    }

    /// Try to parse vector of values and insert them
    /// If error, failed to parse string is returned with its number
    pub fn try_parse_and_append_vector(&mut self, input: &[&str]) -> Result<(), (usize, String)> {
        let mut res = Vec::with_capacity(input.len());
        for (idx, i) in input.iter().enumerate() {
            match i.parse::<T>() {
                Ok(ok) => {
                    res.push(ok);
                }
                Err(_) => {
                    return Err((idx, i.to_string()));
                }
            }
        }

        self.v.extend(res);

        Ok(())
    }
}

// TODO: repetition, not great...
impl ColumnVector {
    pub fn len(&self) -> usize {
        match self {
            ColumnVector::Strings(vc) => vc.len(),
            ColumnVector::Ints(vc) => vc.len(),
            ColumnVector::Floats(vc) => vc.len(),
            ColumnVector::Bools(vc) => vc.len(),
        }
    }

    pub fn new_like_this(&self) -> ColumnVector {
        match self {
            ColumnVector::Strings(v) => ColumnVector::Strings(v.new_like_this()),
            ColumnVector::Ints(v) => ColumnVector::Ints(v.new_like_this()),
            ColumnVector::Floats(v) => ColumnVector::Floats(v.new_like_this()),
            ColumnVector::Bools(v) => ColumnVector::Bools(v.new_like_this()),
        }
    }

    pub fn has_default_value(&self) -> bool {
        match self {
            ColumnVector::Strings(v) => v.has_default_value(),
            ColumnVector::Ints(v) => v.has_default_value(),
            ColumnVector::Floats(v) => v.has_default_value(),
            ColumnVector::Bools(v) => v.has_default_value(),
        }
    }

    pub fn push_default_values(&mut self, count: usize) -> bool {
        match self {
            ColumnVector::Strings(v) => v.push_default_values(count),
            ColumnVector::Ints(v) => v.push_default_values(count),
            ColumnVector::Floats(v) => v.push_default_values(count),
            ColumnVector::Bools(v) => v.push_default_values(count),
        }
    }

    pub fn push_dummy_values(&mut self, count: usize) {
        match self {
            ColumnVector::Strings(v) => v.push_dummy_values(&"".to_string(), count),
            ColumnVector::Ints(v) => v.push_dummy_values(&0, count),
            ColumnVector::Floats(v) => v.push_dummy_values(&0.0, count),
            ColumnVector::Bools(v) => v.push_dummy_values(&false, count),
        }
    }

    pub fn try_set_default_value_from_string(&mut self, input: &str) -> bool {
        match self {
            ColumnVector::Strings(v) => v.try_set_default_value_from_string(input),
            ColumnVector::Ints(v) => v.try_set_default_value_from_string(input),
            ColumnVector::Floats(v) => v.try_set_default_value_from_string(input),
            ColumnVector::Bools(v) => v.try_set_default_value_from_string(input),
        }
    }

    pub fn try_parse_and_append_vector(&mut self, input: &[&str]) -> Result<(), (usize, String)> {
        match self {
            ColumnVector::Strings(v) => v.try_parse_and_append_vector(input),
            ColumnVector::Ints(v) => v.try_parse_and_append_vector(input),
            ColumnVector::Floats(v) => v.try_parse_and_append_vector(input),
            ColumnVector::Bools(v) => v.try_parse_and_append_vector(input),
        }
    }

    pub fn column_type(&self) -> DBType {
        match self {
            ColumnVector::Strings(_) => DBType::DBText,
            ColumnVector::Ints(_) => DBType::DBInt,
            ColumnVector::Floats(_) => DBType::DBFloat,
            ColumnVector::Bools(_) => DBType::DBBool,
        }
    }
}

impl<'a> ConsistentStringDataframe<'a> {
    pub fn new(
        target_fields: &[&'a str],
        row_based_data: &Vec<Vec<&'a str>>,
    ) -> Result<ConsistentStringDataframe<'a>, ConsistentStringDataframeValidationError> {
        if row_based_data.is_empty() {
            panic!("Should be caught by the parser");
            // return Err(ConsistentStringDataframeValidationError::DataFrameHasNoRows);
        }

        if target_fields.is_empty() {
            panic!("Should be caught by the parser");
            // return Err(ConsistentStringDataframeValidationError::DataFrameHasNoTargetFields);
        }

        for (idx, row) in row_based_data.iter().enumerate() {
            if row.len() > target_fields.len() {
                return Err(ConsistentStringDataframeValidationError::TooManyColumns {
                    row_index: idx + 1,
                    row_size: row.len(),
                    expected_size: target_fields.len(),
                });
            } else if row.len() < target_fields.len() {
                return Err(ConsistentStringDataframeValidationError::TooFewColumns {
                    row_index: idx + 1,
                    row_size: row.len(),
                    expected_size: target_fields.len(),
                });
            }
        }

        let row_count = row_based_data.len();
        let column_count = target_fields.len();

        let mut index_map = HashMap::with_capacity(column_count);
        let mut res_vec = Vec::with_capacity(column_count);
        for col in 0..column_count {
            let mut col_data = Vec::with_capacity(row_count);
            for row in 0..row_count {
                col_data.push(row_based_data[row][col]);
            }
            if index_map.insert(target_fields[col].to_string(), res_vec.len()).is_some() {
                return Err(ConsistentStringDataframeValidationError::DuplicateFields {
                    field_name: target_fields[col].to_string(),
                });
            }
            res_vec.push(ConsistentDataFrameColumn {
                column_name: target_fields[col],
                column_data: col_data,
            });

        }

        Ok(ConsistentStringDataframe {
            column_data: res_vec,
            column_index: index_map,
        })
    }

    pub fn len(&self) -> usize {
        self.column_data[0].column_data.len()
    }

    pub fn column_by_name(&self, column_name: &str) -> Option<(usize, &ConsistentDataFrameColumn)> {
        match self.column_index.get(column_name) {
            Some(idx) => Some((*idx, &self.column_data[*idx])),
            None => None,
        }
    }
}

impl<'a> SerializationVector<'a> {
    pub fn table_name(&self) -> &str {
        match self {
            SerializationVector::StringsColumn(v) => v.table_name,
            SerializationVector::IntsColumn(v) => v.table_name,
            SerializationVector::FloatsColumn(v) => v.table_name,
            SerializationVector::BoolsColumn(v) => v.table_name,
            SerializationVector::FkeysColumn { sv, .. } => sv.table_name,
            SerializationVector::FkeysOneToManyColumn { sv, .. } => sv.table_name,
        }
    }
}
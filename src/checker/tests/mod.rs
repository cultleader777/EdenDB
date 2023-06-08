mod child_foreign_keys;
mod child_keys_and_foreign_keys;
pub mod common;
mod common_parent_fkeys;
#[cfg(feature = "datalog")]
mod datalog_proofs;
mod detached_defaults;
mod integration;
mod lua_column_checks;
mod lua_data_insertion;
mod lua_generated_columns;
mod lua_multifile;
mod main;
mod regression;
mod sql_materialized_views;
mod sql_proofs;
mod struct_statement;
mod with_statement;

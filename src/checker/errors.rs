use super::types::DBType;

#[derive(PartialEq, Eq, Debug)]
pub enum DatabaseValidationError {
    TableDefinedTwice {
        table_name: String,
    },
    TableNameIsNotLowercase {
        table_name: String,
    },
    DataInsertionsToMaterializedViewsNotAllowed {
        table_name: String,
    },
    ColumnNameIsNotLowercase {
        table_name: String,
        column_name: String,
    },
    ColumnNameIsReserved {
        table_name: String,
        column_name: String,
        reserved_names: Vec<String>,
    },
    DuplicateColumnNames {
        table_name: String,
        column_name: String,
    },
    DuplicateDataColumnNames {
        table_name: String,
        column_name: String,
    },
    MoreThanOnePrimaryKey {
        table_name: String,
    },
    PrimaryKeyColumnMustBeFirst {
        table_name: String,
        column_name: String,
    },
    FloatColumnCannotBePrimaryKey {
        table_name: String,
        column_name: String,
    },
    BooleanColumnCannotBePrimaryKey {
        table_name: String,
        column_name: String,
    },
    FloatColumnCannotBeInUniqueConstraint {
        table_name: String,
        column_name: String,
    },
    UniqConstraintColumnDoesntExist {
        table_name: String,
        column_name: String,
    },
    DuplicateUniqConstraints {
        table_name: String,
    },
    UnknownColumnType {
        table_name: String,
        column_name: String,
        column_type: String,
    },
    ForeignKeyTableDoesntExist {
        referrer_table: String,
        referrer_column: String,
        referred_table: String,
    },
    ForeignKeyTableDoesntHavePrimaryKey {
        referrer_table: String,
        referrer_column: String,
        referred_table: String,
    },
    ForeignChildKeyTableDoesntHaveParentTable {
        referrer_table: String,
        referrer_column: String,
        referred_table: String,
    },
    ForeignChildKeyTableIsHigherOrEqualInAncestryThanTheReferrer {
        referrer_table: String,
        referrer_column: String,
        referred_table: String,
    },
    ForeignChildKeyTableIntegerKeyMustBeNonNegative {
        referred_table: String,
        offending_column: String,
        offending_value: i64,
    },
    ForeignChildKeyTableStringMustBeSnakeCase {
        referred_table: String,
        offending_column: String,
        offending_value: String,
    },
    ForeignChildKeyReferrerHasIncorrectSegmentsInCompositeKey {
        referrer_table: String,
        referrer_column: String,
        referee_table: String,
        expected_segments: usize,
        actual_segments: usize,
        offending_value: String,
    },
    ForeignChildKeyReferrerCannotHaveWhitespaceInSegments {
        referrer_table: String,
        referrer_column: String,
        referee_table: String,
        offending_value: String,
    },
    ForeignKeyTableDoesNotShareCommonAncestorWithRefereeTable {
        referrer_table: String,
        referrer_column: String,
        referred_table: String,
    },
    InvalidDBIdentifier(String),
    CannotParseDefaultColumnValue {
        table_name: String,
        column_type: DBType,
        column_name: String,
        the_value: String,
    },
    TargetTableForDataNotFound {
        table_name: String,
    },
    UniqConstraintDuplicateColumn {
        table_name: String,
        column_name: String,
    },
    DataTargetColumnNotFound {
        table_name: String,
        target_column_name: String,
    },
    DataTooManyColumns {
        table_name: String,
        row_index: usize,
        row_size: usize,
        expected_size: usize,
    },
    DataTooFewColumns {
        table_name: String,
        row_index: usize,
        row_size: usize,
        expected_size: usize,
    },
    DataCannotParseDataColumnValue {
        table_name: String,
        row_index: usize,
        column_index: usize,
        column_name: String,
        column_value: String,
        expected_type: DBType,
    },
    DataCannotParseDataStructColumnValue {
        table_name: String,
        column_name: String,
        column_value: String,
        expected_type: DBType,
    },
    DataRequiredNonDefaultColumnValueNotProvided {
        table_name: String,
        column_name: String,
    },
    PrimaryKeysCannotHaveDefaultValue {
        table_name: String,
        column_name: String,
    },
    PrimaryOrForeignKeysCannotHaveComputedValue {
        table_name: String,
        column_name: String,
    },
    DefaultValueAndComputedValueAreMutuallyExclusive {
        table_name: String,
        column_name: String,
    },
    MaterializedViewsCannotHaveDefaultColumnExpression {
        table_name: String,
        column_name: String,
    },
    MaterializedViewsCannotHaveComputedColumnExpression {
        table_name: String,
        column_name: String,
    },
    ComputerColumnCannotBeExplicitlySpecified {
        table_name: String,
        column_name: String,
        compute_expression: String,
    },
    ExtraDataParentMustHavePrimaryKey {
        parent_table: String,
    },
    ExtraDataRecursiveInsert {
        parent_table: String,
        extra_table: String,
    },
    ExtraDataTableNotFound {
        parent_table: String,
        extra_table: String,
    },
    ExtraTableHasNoForeignKeysToThisTable {
        parent_table: String,
        extra_table: String,
    },
    ExtraTableMultipleAmbigousForeignKeysToThisTable {
        parent_table: String,
        extra_table: String,
        column_list: Vec<String>,
    },
    ExtraTableCannotRedefineReferenceKey {
        parent_table: String,
        extra_table: String,
        column_name: String,
    },
    DuplicatePrimaryKey {
        table_name: String,
        value: String,
    },
    NonExistingForeignKey {
        table_with_foreign_key: String,
        foreign_key_column: String,
        referred_table: String,
        referred_table_column: String,
        key_value: String,
    },
    NonExistingForeignKeyToChildTable {
        table_parent_keys: Vec<String>,
        table_parent_tables: Vec<String>,
        table_parent_columns: Vec<String>,
        table_with_foreign_key: String,
        foreign_key_column: String,
        referred_table: String,
        referred_table_column: String,
        key_value: String,
    },
    NonExistingParentToChildKey {
        table_parent_keys: Vec<String>,
        table_parent_tables: Vec<String>,
        table_parent_columns: Vec<String>,
        table_with_foreign_key: String,
        foreign_key_column: String,
        referred_table: String,
        referred_table_column: String,
        key_value: String,
    },
    ReferredChildKeyTableIsNotDescendantToThisTable {
        referrer_table: String,
        referrer_column: String,
        expected_to_be_descendant_table: String,
    },
    UniqConstraintViolated {
        table_name: String,
        tuple_definition: String,
        tuple_value: String,
    },
    NonExistingChildPrimaryKeyTable {
        table_name: String,
        column_name: String,
        referred_table: String,
    },
    ParentTableHasNoPrimaryKey {
        table_name: String,
        column_name: String,
        referred_table: String,
    },
    ChildPrimaryKeysLoopDetected {
        table_names: Vec<String>,
    },
    ParentPrimaryKeyColumnNameClashesWithChildColumnName {
        parent_table: String,
        parent_column: String,
        child_table: String,
        child_column: String,
    },
    FoundDuplicateChildPrimaryKeySet {
        table_name: String,
        columns: String,
        duplicate_values: String,
    },
    ParentRecordWithSuchPrimaryKeysDoesntExist {
        parent_table: String,
        parent_columns_names_searched: String,
        parent_columns_to_find: String,
    },
    ExclusiveDataDefinedMultipleTimes {
        table_name: String,
    },
    DuplicateStructuredDataFields {
        table_name: String,
        duplicated_column: String,
    },
    CyclingTablesInContextualInsertsNotAllowed {
        table_loop: Vec<String>,
    },
    NanOrInfiniteFloatNumbersAreNotAllowed {
        table_name: String,
        column_name: String,
        column_value: String,
        row_index: usize,
    },
    LuaCheckExpressionLoadError {
        table_name: String,
        expression: String,
        error: String,
    },
    LuaColumnGenerationError {
        table_name: String,
        expression: String,
        error: String,
    },
    LuaColumnGenerationExpressionLoadError {
        table_name: String,
        column_name: String,
        expression: String,
        error: String,
    },
    LuaColumnGenerationExpressionComputeError {
        table_name: String,
        column_name: String,
        input_row_fields: Vec<String>,
        input_row_values: Vec<String>,
        expression: String,
        error: String,
    },
    LuaColumnGenerationExpressionComputeTypeMismatch {
        table_name: String,
        column_name: String,
        input_row_fields: Vec<String>,
        input_row_values: Vec<String>,
        computed_value: String,
        expression: String,
        error: String,
    },
    LuaCheckEvaluationFailed {
        table_name: String,
        expression: String,
        column_names: Vec<String>,
        row_values: Vec<String>,
        error: String,
    },
    LuaCheckEvaluationErrorUnexpectedReturnType {
        table_name: String,
        expression: String,
        column_names: Vec<String>,
        row_values: Vec<String>,
        error: String,
    },
    LuaCheckEvaluationError {
        table_name: String,
        expression: String,
        column_names: Vec<String>,
        row_values: Vec<String>,
        error: String,
    },
    LuaSourcesLoadError {
        error: String,
        source_file: String,
    },
    SqlProofTableNotFound {
        table_name: String,
        comment: String,
        proof_expression: String,
    },
    SqlProofQueryError {
        error: String,
        table_name: String,
        comment: String,
        proof_expression: String,
    },
    SqlProofQueryPlanningError {
        error: String,
        table_name: String,
        comment: String,
        proof_expression: String,
    },
    SqlProofQueryErrorSingleRowIdColumnExpected {
        error: String,
        table_name: String,
        comment: String,
        proof_expression: String,
    },
    SqlProofQueryColumnOriginMismatchesExpected {
        error: String,
        expected_column_origin_table: String,
        expected_column_origin_name: String,
        actual_column_origin_table: String,
        actual_column_origin_name: String,
        comment: String,
        proof_expression: String,
    },
    SqlProofOffendersFound {
        table_name: String,
        comment: String,
        proof_expression: String,
        // pretty printed json
        offending_columns: Vec<String>,
    },
    DatalogProofTableNotFound {
        table_name: String,
        comment: String,
        proof_expression: String,
    },
    DatalogProofOutputRuleNotFound {
        error: String,
        table_name: String,
        comment: String,
        proof_expression: String,
    },
    DatalogProofTooManyOutputRules {
        error: String,
        table_name: String,
        comment: String,
        proof_expression: String,
    },
    DatalogProofQueryParseError {
        error: String,
        table_name: String,
        comment: String,
        proof_expression: String,
    },
    DatalogProofNoRulesFound {
        error: String,
        table_name: String,
        comment: String,
        proof_expression: String,
    },
    DatalogProofBadOutputRuleFormat {
        error: String,
        table_name: String,
        comment: String,
        proof_expression: String,
    },
    DatalogProofTableExpectedNotFoundInTheOutputQuery {
        error: String,
        table_name: String,
        comment: String,
        proof_expression: String,
    },
    DatalogProofQueryingFailure {
        error: String,
        table_name: String,
        comment: String,
        proof_expression: String,
    },
    DatalogProofOffendersFound {
        table_name: String,
        comment: String,
        proof_expression: String,
        // pretty printed json
        offending_columns: Vec<String>,
    },
    SqlMatViewStatementPrepareException {
        table_name: String,
        sql_expression: String,
        error: String,
    },
    SqlMatViewStatementInitException {
        table_name: String,
        sql_expression: String,
        error: String,
    },
    SqlMatViewWrongColumnCount {
        table_name: String,
        sql_expression: String,
        expected_columns: usize,
        actual_columns: usize,
    },
    SqlMatViewStatementQueryException {
        table_name: String,
        sql_expression: String,
        error: String,
    },
    SqlMatViewNullReturnsUnsupported {
        table_name: String,
        sql_expression: String,
        column_name: String,
        return_row_index: usize,
    },
    SqlMatViewWrongColumnTypeReturned {
        table_name: String,
        sql_expression: String,
        column_name: String,
        return_row_index: usize,
        actual_column_type: String,
        expected_column_type: DBType,
    },
    FailureReadingExternalFile {
        target_file_path: String,
        error: String,
    },
    LuaDataTableError {
        error: String,
    },
    LuaDataTableInvalidKeyTypeIsNotString {
        found_value: String,
    },
    LuaDataTableInvalidKeyTypeIsNotValidUtf8String {
        lossy_value: String,
        bytes: Vec<u8>,
    },
    LuaDataTableNoSuchTable {
        expected_insertion_table: String,
    },
    LuaDataTableInvalidTableValue {
        found_value: String,
    },
    LuaDataTableInvalidRecordValue {
        found_value: String,
    },
    LuaDataTableInvalidRecordColumnNameValue {
        found_value: String,
    },
    LuaDataTableRecordInvalidColumnNameUtf8String {
        lossy_value: String,
        bytes: Vec<u8>,
    },
    LuaDataTableRecordInvalidColumnValue {
        column_name: String,
        column_value: String,
    },
    DetachedDefaultUndefined {
        table: String,
        column: String,
    },
    DetachedDefaultDefinedMultipleTimes {
        table: String,
        column: String,
        expression_a: String,
        expression_b: String,
    },
    DetachedDefaultNonExistingTable {
        table: String,
        column: String,
        expression: String,
    },
    DetachedDefaultNonExistingColumn {
        table: String,
        column: String,
        expression: String,
    },
    DetachedDefaultBadValue {
        table: String,
        column: String,
        value: String,
        expected_type: DBType,
        error: String,
    },
}

impl std::fmt::Display for DatabaseValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "table definition validation error: {:?}", self)
    }
}

impl std::error::Error for DatabaseValidationError {}
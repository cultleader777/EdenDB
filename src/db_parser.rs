extern crate nom;

use std::{
    collections::{HashSet, VecDeque},
    error::Error,
    path::Path,
};

use nom::{
    branch::alt,
    bytes::complete::{escaped, tag, take_while1},
    character::complete::{char, multispace0, multispace1, none_of, one_of, space1},
    combinator::{cut, fail, map, opt, recognize},
    error::{ErrorKind, ParseError, VerboseError, VerboseErrorKind},
    multi::{many0, separated_list0, separated_list1},
    sequence::{delimited, preceded, tuple},
    Parser,
};

use crate::checker::errors::DatabaseValidationError;

#[derive(Debug)]
pub struct TableColumn {
    pub name: String,
    pub the_type: String,
    pub is_reference_to_other_table: bool,
    pub is_reference_to_foreign_child_table: bool,
    pub is_explicit_foreign_child_reference: bool,
    pub is_reference_to_self_child_table: bool,
    pub is_primary_key: bool,
    pub child_primary_key: Option<String>,
    pub default_expression: Option<String>,
    pub is_detached_default: bool,
    pub generated_expression: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UniqConstraint {
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRowCheck {
    pub expression: String,
}

#[derive(Debug)]
pub struct TableDefinition {
    pub name: String,
    pub columns: Vec<TableColumn>,
    pub uniq_constraints: Vec<UniqConstraint>,
    pub row_checks: Vec<TableRowCheck>,
    pub mat_view_expression: Option<String>,
}

#[derive(PartialEq, Eq, Debug)]
pub struct TableDataRow {
    pub value_fields: Vec<String>,
    pub extra_data: Vec<TableData>,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct TableDataStructField {
    pub key: String,
    pub value: String,
}

#[derive(PartialEq, Eq, Debug)]
pub struct TableDataStructFields {
    pub value_fields: Vec<TableDataStructField>,
    pub extra_data: Vec<TableDataStruct>,
}

#[derive(PartialEq, Eq, Debug)]
pub struct TableDataStruct {
    pub target_table_name: String,
    pub is_exclusive: bool,
    pub map: Vec<TableDataStructFields>,
}

#[derive(PartialEq, Eq, Debug)]
pub struct TableData {
    pub target_table_name: String,
    pub target_fields: Vec<String>,
    pub data: Vec<TableDataRow>,
    pub is_exclusive: bool,
}

pub struct InputSource {
    pub path: String,
    pub contents: Option<String>,
    pub source_dir: Option<String>,
}

impl TableColumn {
    pub fn has_default_value(&self) -> bool {
        // detached defaults with default expressions are mutually exclusive
        assert!(!(self.default_expression.is_some() && self.is_detached_default));
        self.default_expression.is_some() || self.is_detached_default
    }
}

/// Reading external files is disabled, mainly for testing
#[cfg(test)]
pub fn parse_sources(input: &mut [InputSource]) -> Result<SourceOutputs, Box<dyn Error + '_>> {
    parse_sources_inner(input, false)
}

pub fn parse_sources_with_external(
    input: &mut [InputSource],
) -> Result<SourceOutputs, Box<dyn Error + '_>> {
    parse_sources_inner(input, true)
}

pub fn strip_source_comments(input: &str) -> String {
    let mut res = String::with_capacity(input.len());

    for line in input.lines() {
        match line.split_once("//") {
            Some((prefix, _)) => res += prefix,
            None => res += line,
        }

        res += "\n";
    }

    res
}

pub fn parse_sources_inner(
    input: &mut [InputSource],
    read_external_files: bool,
) -> Result<SourceOutputs, Box<dyn Error + '_>> {
    let mut result = SourceOutputs {
        table_definitions: vec![],
        table_data_segments: vec![],
        lua_segments: vec![],
        data_segments: vec![],
        sql_proofs: vec![],
        datalog_proofs: vec![],
        detached_defaults: vec![],
    };

    let mut queue: VecDeque<SourceOutputs> = VecDeque::new();
    let mut read_sources: HashSet<String> = HashSet::new();
    let mut finalized: Vec<SourceOutputs> = Vec::new();

    for i in input {
        maybe_read_input_source(i, read_external_files, &mut read_sources).unwrap();

        let src = i.contents.as_ref().unwrap();
        let (_, res) = parse_source_with_path(src.as_str(), &i.source_dir)
            .map_err(|e| to_parsing_error(&i.path, src.as_str(), e))?;
        queue.push_back(res);
    }

    while !queue.is_empty() {
        let mut current = queue.pop_front().unwrap();

        for lua_seg in &mut current.lua_segments {
            maybe_read_input_source(lua_seg, read_external_files, &mut read_sources).unwrap();
        }

        for d_seg in &mut current.data_segments {
            maybe_read_input_source(d_seg, read_external_files, &mut read_sources).unwrap();

            let src = d_seg.contents.as_ref().unwrap();
            let (_, res) = parse_source_with_path(src, &d_seg.source_dir)
                .map_err(|e| to_parsing_error(&d_seg.path, src.as_str(), e))?;

            queue.push_back(res);
        }

        finalized.push(current);
    }

    for res in finalized {
        result.merge(res);
    }

    Ok(result)
}

#[derive(Debug)]
struct ParsingError {
    source_file: String,
    output_message: String,
}

impl std::error::Error for ParsingError {}

impl std::fmt::Display for ParsingError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}: {}", self.source_file, self.output_message)
    }
}

fn to_parsing_error(
    filename: &str,
    src: &str,
    e: nom::Err<nom::error::VerboseError<&str>>,
) -> ParsingError {
    let e = match e {
        nom::Err::Incomplete(_) => {
            panic!("This branch should never be reached")
        }
        nom::Err::Error(e) => e,
        nom::Err::Failure(e) => e,
    };
    let mut msg = nom::error::convert_error(src, e);
    assert_eq!(&msg[0..3], "0: ");
    let good_msg = msg.split_off(3);

    ParsingError {
        source_file: filename.to_string(),
        output_message: good_msg,
    }
}

fn parse_source_with_path<'a>(
    src: &'a str,
    source_path: &'a Option<String>,
) -> IResult<&'a str, SourceOutputs> {
    let (txt, mut res) = parse_source(src)?;

    for lua_seg in &mut res.lua_segments {
        lua_seg.source_dir = source_path.clone();
    }

    for lua_seg in &mut res.data_segments {
        lua_seg.source_dir = source_path.clone();
    }

    Ok((txt, res))
}

fn maybe_read_input_source<'a>(
    seg: &'a mut InputSource,
    reading_ext_enabled: bool,
    already_read_register: &mut HashSet<String>,
) -> Result<(), Box<dyn Error + 'a>> {
    if seg.contents.is_none() {
        if reading_ext_enabled {
            let path = seg.path.clone();
            let p = match &seg.source_dir {
                Some(s) => std::fs::canonicalize(Path::new(s).to_path_buf().join(path.as_str())),
                None => std::fs::canonicalize(Path::new(path.as_str())),
            };
            let mut p = p.map_err(|e| DatabaseValidationError::FailureReadingExternalFile {
                target_file_path: seg.path.clone(),
                error: e.to_string(),
            })?;
            let absolute = p.to_str().unwrap().to_string();
            let already_exists = !already_read_register.insert(absolute);
            if already_exists {
                // TODO: think of occasions when can be a valid case
                panic!(
                    "Source file {} is being read twice, circular dependency?",
                    seg.path
                );
            }

            let res = std::fs::read_to_string(&p).map_err(|e| {
                DatabaseValidationError::FailureReadingExternalFile {
                    target_file_path: seg.path.clone(),
                    error: e.to_string(),
                }
            })?;
            let pop_res = p.pop();
            assert!(pop_res);
            seg.contents = Some(res);
            seg.source_dir = Some(p.to_str().unwrap().to_owned());
        } else {
            panic!("Reading of external files is disabled")
        }
    }

    if let Some(r) = seg.contents.as_mut() {
        let mut stripped = strip_source_comments(r.as_str());
        std::mem::swap(r, &mut stripped);
    }

    Ok(())
}

pub enum TableDataSegment {
    DataFrame(TableData),
    StructuredData(TableDataStruct),
}

pub struct SourceOutputs {
    table_definitions: Vec<TableDefinition>,
    table_data_segments: Vec<TableDataSegment>,
    lua_segments: Vec<InputSource>,
    data_segments: Vec<InputSource>,
    sql_proofs: Vec<ExpressionProof>,
    datalog_proofs: Vec<ExpressionProof>,
    detached_defaults: Vec<DetachedDefaults>,
}

impl SourceOutputs {
    fn merge(&mut self, to_merge: SourceOutputs) {
        self.table_definitions.extend(to_merge.table_definitions);
        self.table_data_segments
            .extend(to_merge.table_data_segments);
        self.lua_segments.extend(to_merge.lua_segments);
        self.data_segments.extend(to_merge.data_segments);
        self.sql_proofs.extend(to_merge.sql_proofs);
        self.datalog_proofs.extend(to_merge.datalog_proofs);
        self.detached_defaults.extend(to_merge.detached_defaults);
    }

    pub fn table_definitions(&self) -> &[TableDefinition] {
        &self.table_definitions
    }

    pub fn table_data_segments(&self) -> &[TableDataSegment] {
        &self.table_data_segments
    }

    pub fn lua_segments(&self) -> &[InputSource] {
        &self.lua_segments
    }

    pub fn sql_proofs(&self) -> &[ExpressionProof] {
        &self.sql_proofs
    }

    pub fn datalog_proofs(&self) -> &[ExpressionProof] {
        &self.datalog_proofs
    }

    pub fn detached_defaults(&self) -> &[DetachedDefaults] {
        &self.detached_defaults
    }
}

pub enum ValidExpressions {
    Sql,
    Datalog,
}

pub struct ExpressionProof {
    pub comment: String,
    pub output_table_name: String,
    pub expression: String,
    pub expression_type: ValidExpressions,
}

pub struct DetachedDefaultDefinition {
    pub table: String,
    pub column: String,
    pub value: String,
}

pub struct DetachedDefaults {
    pub values: Vec<DetachedDefaultDefinition>,
}

enum ValidSourceSegments {
    TDef(TableDefinition),
    TData(TableData),
    TDataStruct(TableDataStruct),
    LuaSegment(InputSource),
    DataSegment(InputSource),
    ExpressionProof(ExpressionProof),
    DetachedDefaults(DetachedDefaults),
}

pub type IResult<I, O, E = nom::error::VerboseError<I>> = Result<(I, O), nom::Err<E>>;

enum TableRowReturn {
    Col(TableColumn),
    Constraint(UniqConstraint),
    Check(TableRowCheck),
}

fn parse_source(input: &str) -> IResult<&str, SourceOutputs> {
    let mut res = SourceOutputs {
        table_definitions: vec![],
        table_data_segments: vec![],
        lua_segments: vec![],
        data_segments: vec![],
        sql_proofs: vec![],
        datalog_proofs: vec![],
        detached_defaults: vec![],
    };

    let (tail, output) = many0(preceded(
        multispace0,
        alt((
            parse_include_segment,
            map(parse_table, ValidSourceSegments::TDef),
            map(parse_materialized_view, ValidSourceSegments::TDef),
            map(parse_table_data, ValidSourceSegments::TData),
            map(parse_table_data_structs, ValidSourceSegments::TDataStruct),
            map(parse_sql_proof, ValidSourceSegments::ExpressionProof),
            map(
                parse_detached_defaults,
                ValidSourceSegments::DetachedDefaults,
            ),
        )),
    ))
    .parse(input)?;

    for i in output {
        match i {
            ValidSourceSegments::TDef(td) => {
                res.table_definitions.push(td);
            }
            ValidSourceSegments::TData(td) => {
                res.table_data_segments
                    .push(TableDataSegment::DataFrame(td));
            }
            ValidSourceSegments::TDataStruct(ts) => {
                res.table_data_segments
                    .push(TableDataSegment::StructuredData(ts));
            }
            ValidSourceSegments::LuaSegment(s) => {
                res.lua_segments.push(s);
            }
            ValidSourceSegments::DataSegment(s) => {
                res.data_segments.push(s);
            }
            ValidSourceSegments::ExpressionProof(sp) => match &sp.expression_type {
                ValidExpressions::Sql => {
                    res.sql_proofs.push(sp);
                }
                ValidExpressions::Datalog => {
                    res.datalog_proofs.push(sp);
                }
            },
            ValidSourceSegments::DetachedDefaults(dd) => {
                res.detached_defaults.push(dd);
            }
        }
    }

    let (tail, _) = multispace0.parse(tail)?;

    if !tail.is_empty() {
        return Err(nom::Err::Error(VerboseError {
            errors: vec![(tail, VerboseErrorKind::Context("parsing failure"))],
        }));
    }

    Ok((tail, res))
}

fn curly_braces_expression(input: &str) -> IResult<&str, &str> {
    let (tail, (_, content, _)) =
        tuple((char('{'), take_until_unbalanced('{', '}'), char('}'))).parse(input)?;

    Ok((tail, content))
}

fn parse_materialized_view(input: &str) -> IResult<&str, TableDefinition> {
    let (tail, (_, _, _, _, table_name, _, rows, _, _, _, sql_expression)) = tuple((
        tag("MATERIALIZED"),
        multispace1,
        tag("VIEW"),
        multispace1,
        valid_table_or_column_name,
        multispace1,
        parse_table_definition,
        multispace1,
        tag("AS"),
        multispace1,
        curly_braces_expression,
    ))
    .parse(input)?;

    let mut columns = vec![];
    let mut uniq_constraints = vec![];
    let mut row_checks = vec![];

    for i in rows {
        match i {
            TableRowReturn::Col(c) => columns.push(c),
            TableRowReturn::Constraint(c) => uniq_constraints.push(c),
            TableRowReturn::Check(c) => row_checks.push(c),
        }
    }

    Ok((
        tail,
        TableDefinition {
            name: table_name.to_owned(),
            columns,
            uniq_constraints,
            row_checks,
            mat_view_expression: Some(sql_expression.to_string()),
        },
    ))
}

fn parse_table(input: &str) -> IResult<&str, TableDefinition> {
    let (tail, (_, _, table_name, _, rows)) = tuple((
        tag("TABLE"),
        multispace1,
        valid_table_or_column_name,
        multispace1,
        parse_table_definition,
    ))
    .parse(input)?;

    let mut columns = vec![];
    let mut uniq_constraints = vec![];
    let mut row_checks = vec![];

    for i in rows {
        match i {
            TableRowReturn::Col(c) => columns.push(c),
            TableRowReturn::Constraint(c) => uniq_constraints.push(c),
            TableRowReturn::Check(c) => row_checks.push(c),
        }
    }

    Ok((
        tail,
        TableDefinition {
            name: table_name.to_owned(),
            columns,
            uniq_constraints,
            row_checks,
            mat_view_expression: None,
        },
    ))
}

fn parse_table_data(input: &str) -> IResult<&str, TableData> {
    let (tail, (_, _, is_exclusive, table_name, maybe_fields, _, _, data, ..)) = tuple((
        tag("DATA"),
        multispace1,
        opt(tuple((tag("EXCLUSIVE"), multispace1))),
        valid_table_or_column_name,
        opt(tuple((multispace0, parse_bracket_field_list))),
        multispace1,
        char('{'),
        parse_table_data_rows_with_inner,
        char('}'),
    ))
    .parse(input)?;

    let res = TableData {
        target_table_name: table_name.to_string(),
        target_fields: maybe_fields.map(|i| i.1).unwrap_or_default(),
        data,
        is_exclusive: is_exclusive.is_some(),
    };

    Ok((tail, res))
}

fn parse_table_data_struct_literals(input: &str) -> IResult<&str, Vec<TableDataStructFields>> {
    let (tail, res) = alt((
        map(
            tuple((char('{'), parse_table_data_structs_with_inner, char('}'))),
            |(_, sf, _)| vec![sf],
        ),
        map(
            tuple((
                char('['),
                multispace0,
                separated_list1(
                    tuple((multispace0, char(','), multispace0)),
                    tuple((char('{'), parse_table_data_structs_with_inner, char('}'))),
                ),
                opt(tuple((multispace0, char(',')))),
                multispace0,
                char(']'),
            )),
            |(_, _, sf, ..)| sf.into_iter().map(|(_, sf, _)| sf).collect::<Vec<_>>(),
        ),
    ))
    .parse(input)?;

    Ok((tail, res))
}

fn parse_table_data_structs(input: &str) -> IResult<&str, TableDataStruct> {
    let (tail, (_, _, _, _, is_exclusive, table_name, _, data)) = tuple((
        tag("DATA"),
        multispace1,
        tag("STRUCT"),
        multispace1,
        opt(tuple((tag("EXCLUSIVE"), multispace1))),
        valid_table_or_column_name,
        multispace1,
        parse_table_data_struct_literals,
    ))
    .parse(input)?;

    let res = TableDataStruct {
        target_table_name: table_name.to_string(),
        is_exclusive: is_exclusive.is_some(),
        map: data,
    };

    Ok((tail, res))
}

fn parse_sql_proof(input: &str) -> IResult<&str, ExpressionProof> {
    let (tail, (_, _, comment, _, _, _, _, _, _, _, tname, maybe_lang, _, sql_expression)) =
        tuple((
            tag("PROOF"),
            multispace1,
            parse_quoted_text,
            multispace1,
            tag("NONE"),
            multispace1,
            tag("EXIST"),
            multispace1,
            tag("OF"),
            multispace1,
            valid_table_or_column_name,
            opt(tuple((multispace1, alt((tag("SQL"), tag("DATALOG")))))),
            multispace1,
            curly_braces_expression,
        ))
        .parse(input)?;

    Ok((
        tail,
        ExpressionProof {
            comment: comment.to_string(),
            output_table_name: tname.to_string(),
            expression: sql_expression.to_string(),
            expression_type: maybe_lang
                .map(|(_, t)| match t {
                    "SQL" => ValidExpressions::Sql,
                    "DATALOG" => ValidExpressions::Datalog,
                    _ => panic!("Must have matched some, bug in code."),
                })
                .unwrap_or(ValidExpressions::Sql),
        },
    ))
}

fn parse_detached_defaults(input: &str) -> IResult<&str, DetachedDefaults> {
    let (full_tail, (_, _, cb)) =
        tuple((tag("DEFAULTS"), multispace1, curly_braces_expression)).parse(input)?;

    let (_, (_, elems, ..)) = tuple((
        multispace0,
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            tuple((
                valid_table_or_column_name,
                char('.'),
                valid_table_or_column_name,
                multispace1,
                parse_table_data_point,
            )),
        ),
        opt(tuple((multispace0, char(',')))),
        multispace0,
    ))
    .parse(cb)?;

    let elems: Vec<(&str, char, &str, &str, &str)> = elems;

    let mut res = DetachedDefaults { values: Vec::new() };

    for (table_name, _, column_name, _, value) in elems {
        res.values.push(DetachedDefaultDefinition {
            table: table_name.to_string(),
            column: column_name.to_string(),
            value: value.to_string(),
        });
    }

    Ok((full_tail, res))
}

fn parse_include_segment(input: &str) -> IResult<&str, ValidSourceSegments> {
    let (tail, (_, maybe_lang, _, src)) = tuple((
        tag("INCLUDE"),
        opt(tuple((multispace1, alt((tag("LUA"), tag("DATA")))))),
        multispace1,
        alt((
            map(curly_braces_expression, |src| InputSource {
                contents: Some(src.to_string()),
                path: "inline".to_string(),
                source_dir: None,
            }),
            map(parse_quoted_text, |path| InputSource {
                path: path.to_string(),
                contents: None,
                source_dir: None,
            }),
        )),
    ))
    .parse(input)?;

    let seg = match maybe_lang {
        Some((_, lang)) => match lang {
            "LUA" => ValidSourceSegments::LuaSegment(src),
            "DATA" => ValidSourceSegments::DataSegment(src),
            _ => {
                panic!("Should never be reached")
            }
        },
        None => ValidSourceSegments::DataSegment(src),
    };

    Ok((tail, seg))
}

fn parse_table_row(input: &str) -> IResult<&str, TableRowReturn> {
    alt((
        parse_table_column,
        parse_table_uniq_constraint,
        parse_row_check,
    ))
    .parse(input)
}

fn parse_table_column(input: &str) -> IResult<&str, TableRowReturn> {
    enum DefaultVariant {
        Hardcoded(String),
        Detached,
    }

    let (tail, (column_name, _, is_ref, column_type, maybe_default, is_generated, is_primary_key)) =
        tuple((
            valid_table_or_column_name,
            multispace1,
            opt(tuple((
                tag("REF"),
                multispace1,
                opt(tuple((
                    opt(tuple((
                        opt(tuple((tag("EXPLICIT"), multispace1))),
                        tag("FOREIGN"),
                        multispace1,
                    ))),
                    tag("CHILD"),
                    multispace1,
                ))),
            ))),
            valid_table_or_column_name,
            opt(alt((
                tuple((multispace1, tag("DETACHED"), multispace1, tag("DEFAULT")))
                    .map(|_| DefaultVariant::Detached),
                tuple((
                    multispace1,
                    tag("DEFAULT"),
                    multispace1,
                    parse_table_data_point,
                ))
                .map(|(_, _, _, v)| DefaultVariant::Hardcoded(v.to_string())),
            ))),
            opt(tuple((
                multispace1,
                tag("GENERATED"),
                multispace1,
                tag("AS"),
                multispace1,
                curly_braces_expression,
            ))),
            opt(tuple((
                multispace1,
                tag("PRIMARY"),
                multispace1,
                tag("KEY"),
                opt(tuple((
                    multispace1,
                    tag("CHILD"),
                    multispace1,
                    tag("OF"),
                    multispace1,
                    valid_table_or_column_name,
                ))),
            ))),
        ))
        .parse(input)?;

    let is_pkey = is_primary_key.is_some();
    let maybe_child_prim_key = is_primary_key.and_then(|(_, _, _, _, chld)| {
        chld.map(|(_, _, _, _, _, parent_name)| parent_name.to_string())
    });
    let maybe_generated = is_generated.map(|(_, _, _, _, _, gen)| gen.to_string());

    let (is_reference_to_foreign_child_table, is_explicit_foreign_child_reference) = is_ref
        .map(|(_, _, child)| match child {
            Some((is_foreign, _, _)) => {
                match is_foreign {
                    Some((is_explicit, _, _)) => {
                        (true, is_explicit.is_some())
                    }
                    None => (false, false)
                }
            },
            None => (false, false),
        })
        .unwrap_or((false, false));

    let is_reference_to_self_child_table = is_ref
        .map(|(_, _, child)| match child {
            Some((is_foreign, _, _)) => is_foreign.is_none(),
            None => false,
        })
        .unwrap_or(false);

    let default_expression = if let Some(DefaultVariant::Hardcoded(expr)) = &maybe_default {
        Some(expr.to_string())
    } else {
        None
    };

    let is_detached_default = matches!(maybe_default, Some(DefaultVariant::Detached));

    Ok((
        tail,
        TableRowReturn::Col(TableColumn {
            name: column_name.to_owned(),
            the_type: column_type.to_owned(),
            is_reference_to_other_table: is_ref.is_some(),
            is_reference_to_foreign_child_table,
            is_explicit_foreign_child_reference,
            is_reference_to_self_child_table,
            is_primary_key: is_pkey,
            child_primary_key: maybe_child_prim_key,
            generated_expression: maybe_generated,
            default_expression,
            is_detached_default,
        }),
    ))
}

fn parse_bracket_field_list(input: &str) -> IResult<&str, Vec<String>> {
    let (tail, (_, _, out, ..)) = tuple((
        char('('),
        multispace0,
        separated_list0(
            tuple((multispace0, char(','), multispace0)),
            valid_table_or_column_name,
        ),
        multispace0,
        char(')'),
    ))
    .parse(input)?;

    let res = out.iter().map(|i| i.to_string()).collect::<Vec<_>>();

    Ok((tail, res))
}

fn parse_table_uniq_constraint(input: &str) -> IResult<&str, TableRowReturn> {
    let (tail, (_, _, lst)) =
        tuple((tag("UNIQUE"), multispace0, parse_bracket_field_list)).parse(input)?;

    let fields = lst.into_iter().collect::<Vec<_>>();

    Ok((tail, TableRowReturn::Constraint(UniqConstraint { fields })))
}

fn parse_table_definition(input: &str) -> IResult<&str, Vec<TableRowReturn>> {
    let (tail, tdef) = curly_braces_expression(input)?;

    let (_, tpl) = tuple((
        multispace1,
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_table_row,
        ),
        opt(tuple((multispace0, char(',')))),
        multispace0,
    ))
    .parse(tdef)?;

    let (_, columns, ..) = tpl;

    Ok((tail, columns))
}

fn parse_row_check(input: &str) -> IResult<&str, TableRowReturn> {
    let res = tuple((tag("CHECK"), multispace1, curly_braces_expression)).parse(input)?;

    let (tail, (_, _, check_expr)) = res;

    Ok((
        tail,
        TableRowReturn::Check(TableRowCheck {
            expression: check_expr.to_string(),
        }),
    ))
}

pub fn valid_table_or_column_name(input: &str) -> IResult<&str, &str> {
    let (tail, tname) = take_while1(|c: char| c.is_alphanumeric() || c == '_').parse(input)?;

    Ok((tail, tname))
}

fn valid_unquoted_data_char(c: char) -> bool {
    let valid_chars = "_-.!@#$%^&*=>";
    if c.is_alphanumeric() {
        return true;
    }

    for vc in valid_chars.chars() {
        if c == vc {
            return true;
        }
    }

    false
}

fn valid_unquoted_data_word(input: &str) -> IResult<&str, &str> {
    let invalid_reserved_next_words = &["WITH", "GENERATED", "PRIMARY"];

    for inv in invalid_reserved_next_words {
        if input.len() > inv.len()
            && input.starts_with(*inv)
            && input.as_bytes()[inv.len()].is_ascii_whitespace()
        {
            return fail(input);
        }
    }

    take_while1(valid_unquoted_data_char).parse(input)
}

fn valid_unquoted_data_segment(input: &str) -> IResult<&str, &str> {
    recognize(tuple((
        valid_unquoted_data_word,
        many0(tuple((space1, valid_unquoted_data_word))),
    )))
    .parse(input)
}

fn parse_quoted_text(input: &str) -> IResult<&str, &str> {
    let (tail, res) = alt((
        delimited(
            char('\"'),
            cut(escaped(many0(none_of("\"")), '\\', one_of("\"\\"))),
            char('\"'),
        ),
        delimited(
            char('\''),
            cut(escaped(many0(none_of("\'")), '\\', one_of("\'\\"))),
            char('\''),
        ),
    ))
    .parse(input)?;

    Ok((tail, res))
}

pub fn parse_table_data_point(input: &str) -> IResult<&str, &str> {
    let (tail, res) = alt((parse_quoted_text, valid_unquoted_data_segment)).parse(input)?;

    Ok((tail, res))
}

fn parse_table_data_row_without_inner(input: &str) -> IResult<&str, Vec<String>> {
    let (tail, res) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        alt((parse_table_data_point, multispace0)),
    )
    .parse(input)?;

    if res != [""] {
        let res = res.iter().map(|i| i.to_string()).collect();
        Ok((tail, res))
    } else {
        Err(nom::Err::Error(VerboseError {
            errors: vec![(input, VerboseErrorKind::Context("empty vec"))],
        }))
    }
}

fn parse_table_data_rows_with_inner(input: &str) -> IResult<&str, Vec<TableDataRow>> {
    let (tail, (_, elems, ..)) = tuple((
        multispace0,
        separated_list1(
            tuple((multispace0, char(';'), multispace0)),
            tuple((
                parse_table_data_row_without_inner,
                many0(tuple((
                    multispace1,
                    tag("WITH"),
                    multispace1,
                    valid_table_or_column_name,
                    opt(tuple((multispace0, parse_bracket_field_list))),
                    multispace1,
                    char('{'),
                    parse_table_data_rows_with_inner,
                    char('}'),
                ))),
            )),
        ),
        opt(tuple((multispace0, char(';')))),
        multispace0,
    ))
    .parse(input)?;

    let res = elems
        .into_iter()
        .map(|i| TableDataRow {
            value_fields: i.0,
            extra_data: i
                .1
                .into_iter()
                .map(|j| {
                    let extra_table = j.3;
                    let maybe_column_labels = j.4;
                    let the_matrix = j.7;
                    let extra_target_fields =
                        maybe_column_labels.map(|cl| cl.1).unwrap_or_default();
                    TableData {
                        target_table_name: extra_table.to_string(),
                        target_fields: extra_target_fields,
                        data: the_matrix,
                        is_exclusive: false,
                    }
                })
                .collect(),
        })
        .collect();

    Ok((tail, res))
}

fn parse_struct_kv_pair(input: &str) -> IResult<&str, TableDataStructField> {
    let (tail, (k, _, _, _, v)) = tuple((
        valid_table_or_column_name,
        multispace0,
        char(':'),
        multispace0,
        parse_table_data_point,
    ))
    .parse(input)?;

    Ok((
        tail,
        TableDataStructField {
            key: k.to_string(),
            value: v.to_string(),
        },
    ))
}

fn parse_table_data_structs_with_inner(input: &str) -> IResult<&str, TableDataStructFields> {
    let (tail, (_, elems, _, extra_data, ..)) = tuple((
        multispace0,
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_struct_kv_pair,
        ),
        opt(tuple((multispace0, char(',')))),
        many0(tuple((
            multispace1,
            tag("WITH"),
            multispace1,
            valid_table_or_column_name,
            multispace1,
            parse_table_data_struct_literals,
        ))),
        opt(tuple((multispace0, char(',')))),
        multispace0,
    ))
    .parse(input)?;

    let res = TableDataStructFields {
        value_fields: elems,
        extra_data: extra_data
            .into_iter()
            .map(|(_, _, _, target_table, _, fields)| {
                TableDataStruct {
                    target_table_name: target_table.to_string(),
                    map: fields,
                    // inner fields can never be exclusive
                    is_exclusive: false,
                }
            })
            .collect::<Vec<_>>(),
    };

    Ok((tail, res))
}

// thanx copy paste bois
// https://docs.rs/parse-hyperlinks/0.23.3/src/parse_hyperlinks/lib.rs.html#41
pub fn take_until_unbalanced(
    opening_bracket: char,
    closing_bracket: char,
) -> impl Fn(&str) -> IResult<&str, &str> {
    move |i: &str| {
        let mut index = 0;
        let mut bracket_counter = 0;
        while let Some(n) = &i[index..].find(&[opening_bracket, closing_bracket, '\\'][..]) {
            index += n;
            let mut it = i[index..].chars();
            match it.next().unwrap_or_default() {
                c if c == '\\' => {
                    // Skip the escape char `\`.
                    index += '\\'.len_utf8();
                    // Skip also the following char.
                    let c = it.next().unwrap_or_default();
                    index += c.len_utf8();
                }
                c if c == opening_bracket => {
                    bracket_counter += 1;
                    index += opening_bracket.len_utf8();
                }
                c if c == closing_bracket => {
                    // Closing bracket.
                    bracket_counter -= 1;
                    index += closing_bracket.len_utf8();
                }
                // Can not happen.
                _ => unreachable!(),
            };
            // We found the unmatched closing bracket.
            if bracket_counter == -1 {
                // We do not consume it.
                index -= closing_bracket.len_utf8();
                return Ok((&i[index..], &i[0..index]));
            };
        }

        if bracket_counter == 0 {
            Ok(("", i))
        } else {
            Err(nom::Err::Error(VerboseError::from_error_kind(
                i,
                ErrorKind::TakeUntil,
            )))
        }
    }
}

#[test]
fn test_parse_bracket_field_list() {
    assert_eq!(
        parse_bracket_field_list(r#"(peace,bois)"#).unwrap().1,
        vec!["peace", "bois"],
    );
    let res = parse_bracket_field_list(r#"(peace, bois)"#).unwrap();
    assert_eq!(res.1, vec!["peace", "bois"],);
    assert_eq!(res.0, "",);
    let res = parse_bracket_field_list(r#"( peace , bois ) "#).unwrap();
    assert_eq!(res.1, vec!["peace", "bois"],);
    assert_eq!(res.0, " ",);
    assert!(parse_bracket_field_list(r#" ( peace , bois )"#).is_err(),);
    assert!(parse_bracket_field_list(r#" ( peace , bois ) "#).is_err(),);
}

#[test]
fn test_parsing_datapoint() {
    assert_eq!(
        parse_table_data_point("\" bozo lozo rozo \"").unwrap().1,
        " bozo lozo rozo "
    );
    assert_eq!(
        parse_table_data_point("\" bozo lozo\\ rozo \"").unwrap().1,
        " bozo lozo\\ rozo "
    );
    assert_eq!(parse_table_data_point("sup boi").unwrap().1, "sup boi");
    assert_eq!(parse_table_data_point(r#""3,14""#).unwrap().1, "3,14");
    assert_eq!(parse_table_data_point(r#"3.14-_@"#).unwrap().1, "3.14-_@");
    let res = parse_table_data_point(r#"le WITH tag "#).unwrap();
    assert_eq!(res.0, " WITH tag ");
    assert_eq!(res.1, "le");
    let res = parse_table_data_point(r#"le WITHboi "#).unwrap();
    assert_eq!(res.0, " ");
    assert_eq!(res.1, "le WITHboi");
    let res = parse_table_data_point(r#"le WITH"#).unwrap();
    assert_eq!(res.0, "");
    assert_eq!(res.1, "le WITH");
    let res = parse_table_data_point(r#"no cut pls "#).unwrap();
    assert_eq!(res.0, " ");
    assert_eq!(res.1, "no cut pls");
}

#[test]
fn test_parse_single_table_data_row() {
    assert_eq!(
        parse_table_data_row_without_inner(r#"moo, boo ,hoo, 123 , "  456  ""#)
            .unwrap()
            .1,
        ["moo", "boo", "hoo", "123", "  456  "]
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<String>>(),
    );
    assert_eq!(
        parse_table_data_row_without_inner(r#",,,"#).unwrap().1,
        ["", "", "", ""]
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<String>>(),
    );
    assert_eq!(
        parse_table_data_row_without_inner(r#","#).unwrap().1,
        ["", ""]
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<String>>(),
    );
    assert!(parse_table_data_row_without_inner(r#""#).is_err());
}

#[test]
fn test_parse_multiple_table_data_rows_single() {
    assert_eq!(
        parse_table_data_rows_with_inner(
            r#"
            moo, boo ,hoo, 123 , "  456  "
        "#
        )
        .unwrap()
        .1,
        vec![TableDataRow {
            value_fields: vec![
                "moo".to_string(),
                "boo".to_string(),
                "hoo".to_string(),
                "123".to_string(),
                "  456  ".to_string()
            ],
            extra_data: vec![]
        },],
    );
}

#[test]
fn test_parse_multiple_table_data_rows_single_multi() {
    let test_str = r#"
        moo, boo ,hoo, 123 , "  456  "; peace, bois;
        what up , wit it ,vanilla face;
        "  hey ho; here" , she goes
        ;sizzle; mcpizzle ; dizzle;,,,;
    "#;
    let res = parse_table_data_rows_with_inner(test_str).unwrap();
    assert_eq!(
        res.1,
        vec![
            TableDataRow {
                value_fields: vec![
                    "moo".to_string(),
                    "boo".to_string(),
                    "hoo".to_string(),
                    "123".to_string(),
                    "  456  ".to_string()
                ],
                extra_data: vec![]
            },
            TableDataRow {
                value_fields: vec!["peace".to_string(), "bois".to_string()],
                extra_data: vec![]
            },
            TableDataRow {
                value_fields: vec![
                    "what up".to_string(),
                    "wit it".to_string(),
                    "vanilla face".to_string()
                ],
                extra_data: vec![]
            },
            TableDataRow {
                value_fields: vec!["  hey ho; here".to_string(), "she goes".to_string()],
                extra_data: vec![]
            },
            TableDataRow {
                value_fields: vec!["sizzle".to_string()],
                extra_data: vec![]
            },
            TableDataRow {
                value_fields: vec!["mcpizzle".to_string()],
                extra_data: vec![]
            },
            TableDataRow {
                value_fields: vec!["dizzle".to_string()],
                extra_data: vec![]
            },
            TableDataRow {
                value_fields: vec![
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                    "".to_string()
                ],
                extra_data: vec![]
            },
        ],
    );
    assert_eq!(res.0, "");
}

#[test]
fn test_simple_parse_table_data_regression() {
    assert_eq!(
        parse_table_data(r#"DATA servers { server, region WITH network { a } }"#)
            .unwrap()
            .1,
        TableData {
            target_table_name: "servers".to_string(),
            target_fields: vec![],
            is_exclusive: false,
            data: vec![TableDataRow {
                value_fields: vec!["server".to_string(), "region".to_string()],
                extra_data: vec![TableData {
                    target_table_name: "network".to_string(),
                    target_fields: vec![],
                    is_exclusive: false,
                    data: vec![TableDataRow {
                        value_fields: vec!["a".to_string()],
                        extra_data: vec![],
                    }],
                },],
            }],
        },
    );
}

#[test]
fn test_simple_parse_table_data() {
    assert_eq!(
        parse_table_data(
            r#"DATA mcshizzle ( my , bois ) {
            hello, bois;
        }"#
        )
        .unwrap()
        .1,
        TableData {
            target_table_name: "mcshizzle".to_string(),
            target_fields: vec!["my".to_string(), "bois".to_string()],
            is_exclusive: false,
            data: vec![TableDataRow {
                value_fields: vec!["hello".to_string(), "bois".to_string()],
                extra_data: vec![],
            }],
        },
    );
}

#[test]
fn test_bigger_parse_table_data() {
    assert_eq!(
        parse_table_data(
            r#"DATA mcshizzle ( my , bois ) {
            hello, bois;
            "hey ",  " ho "
        }"#
        )
        .unwrap()
        .1,
        TableData {
            target_table_name: "mcshizzle".to_string(),
            target_fields: vec!["my".to_string(), "bois".to_string()],
            is_exclusive: false,
            data: vec![
                TableDataRow {
                    value_fields: vec!["hello".to_string(), "bois".to_string()],
                    extra_data: vec![],
                },
                TableDataRow {
                    value_fields: vec!["hey ".to_string(), " ho ".to_string()],
                    extra_data: vec![],
                },
            ],
        },
    );
}

#[test]
fn test_optional_columns_table_data() {
    assert_eq!(
        parse_table_data(
            r#"DATA mcshizzle {
            hello, bois;
            "hey ",  " ho "
              WITH bananas ( som , cols ) {
                a, b;
                c, d;
              } WITH stinker {
                moo, hoo
              };
            slo, down, boi
        }"#
        )
        .unwrap()
        .1,
        TableData {
            target_table_name: "mcshizzle".to_string(),
            target_fields: vec![],
            is_exclusive: false,
            data: vec![
                TableDataRow {
                    value_fields: vec!["hello".to_string(), "bois".to_string()],
                    extra_data: vec![],
                },
                TableDataRow {
                    value_fields: vec!["hey ".to_string(), " ho ".to_string()],
                    extra_data: vec![
                        TableData {
                            target_table_name: "bananas".to_string(),
                            target_fields: vec!["som".to_string(), "cols".to_string()],
                            is_exclusive: false,
                            data: vec![
                                TableDataRow {
                                    value_fields: vec!["a".to_string(), "b".to_string()],
                                    extra_data: vec![],
                                },
                                TableDataRow {
                                    value_fields: vec!["c".to_string(), "d".to_string()],
                                    extra_data: vec![]
                                }
                            ]
                        },
                        TableData {
                            target_table_name: "stinker".to_string(),
                            target_fields: vec![],
                            is_exclusive: false,
                            data: vec![TableDataRow {
                                value_fields: vec!["moo".to_string(), "hoo".to_string()],
                                extra_data: vec![],
                            }]
                        }
                    ],
                },
                TableDataRow {
                    value_fields: vec!["slo".to_string(), "down".to_string(), "boi".to_string()],
                    extra_data: vec![],
                },
            ],
        },
    );
}

#[test]
fn test_parse_table_name() {
    assert!(valid_table_or_column_name("Meow_pLs").is_ok());
}

#[test]
fn test_parse_example_table() {
    let test_table = r#"TABLE regions {
        mnemonic String PRIMARY KEY,
        full_name String,
    }"#;
    let res = parse_table(test_table);

    assert!(res.is_ok());

    let (input, td) = res.unwrap();
    assert_eq!(input, "");
    assert_eq!(td.name, "regions");

    assert_eq!(td.columns.len(), 2);

    assert_eq!(td.columns[0].name, "mnemonic");
    assert_eq!(td.columns[0].the_type, "String");
    assert!(td.columns[0].is_primary_key);

    assert_eq!(td.columns[1].name, "full_name");
    assert_eq!(td.columns[1].the_type, "String");
    assert!(!td.columns[1].is_primary_key);
}

#[test]
fn test_parse_uniq_constraint_table() {
    let test_table = r#"TABLE network_interfaces_ipv4 {
        name TEXT,
        ipv4 TEXT,
        server REF servers,
        some_def INT DEFAULT 3.14,
        UNIQUE (name, server),
        UNIQUE (server, name),
        UNIQUE (lol),
    }"#;
    let res = parse_table(test_table);

    assert!(res.is_ok());

    let (input, td) = res.unwrap();
    assert_eq!(input, "");
    assert_eq!(td.name, "network_interfaces_ipv4");

    assert_eq!(td.columns.len(), 4);

    assert_eq!(td.columns[0].name, "name");
    assert_eq!(td.columns[0].the_type, "TEXT");
    assert!(!td.columns[0].is_primary_key);
    assert!(!td.columns[0].is_reference_to_other_table);
    assert!(!td.columns[0].is_reference_to_foreign_child_table);
    assert_eq!(td.columns[0].default_expression, None);

    assert_eq!(td.columns[1].name, "ipv4");
    assert_eq!(td.columns[1].the_type, "TEXT");
    assert!(!td.columns[1].is_primary_key);
    assert!(!td.columns[1].is_reference_to_other_table);
    assert_eq!(td.columns[1].default_expression, None);

    assert_eq!(td.columns[2].name, "server");
    assert_eq!(td.columns[2].the_type, "servers");
    assert!(!td.columns[2].is_primary_key);
    assert!(td.columns[2].is_reference_to_other_table);
    assert_eq!(td.columns[2].default_expression, None);

    assert_eq!(td.columns[3].name, "some_def");
    assert_eq!(td.columns[3].the_type, "INT");
    assert!(!td.columns[3].is_primary_key);
    assert!(!td.columns[3].is_reference_to_other_table);
    assert_eq!(td.columns[3].default_expression, Some("3.14".to_string()));
}

#[test]
fn test_smoke() {
    let res = parse_source(test_data());
    assert!(res.is_ok());
    let (leftover, res) = res.unwrap();
    assert_eq!(res.table_definitions.len(), 4);
    assert_eq!(res.table_data_segments.len(), 2);
    assert_eq!(leftover, "");
}

#[cfg(test)]
fn test_data() -> &'static str {
    r#"
TABLE regions {
    mnemonic String PRIMARY KEY,
    full_name TEXT,
}

TABLE servers {
    hostname TEXT PRIMARY KEY,
    region REF regions,
}

TABLE networks {
    mnemonic TEXT PRIMARY KEY,
}

TABLE network_interfaces_ipv4 {
    name TEXT,
    ipv4 TEXT,
    server REF servers,
    UNIQUE (name, server)
}


DATA regions(mnemonic, full_name) {
    RegionA, "Some region boi"
}

DATA servers {
    "server1", RegionA
      WITH NetworkInterfaces {
        "eth0", "192.168.77.1";
      }
    ;
}
    "#
}

#[test]
fn test_syntax_error() {
    let source = r#"
    TABLE this is weird {
        syntax;
        what is this?
    }
"#;
    let mut inp = [InputSource {
        contents: Some(source.to_string()),
        path: "test".to_string(),
        source_dir: None,
    }];
    let parsed = crate::db_parser::parse_sources(&mut inp);
    assert!(parsed.is_err());
}

#[test]
fn test_nested_structs_parse() {
    let source = r#"DATA STRUCT server {
    hostname: mclassen,
    dizzle:shizzle ,
    WITH disk {
        dev_slot: "/dev/sda" ,
        bozo: 7,
        lozo:7.77,
    } ,
}"#;

    let (tail, parsed) = parse_table_data_structs(source).unwrap();

    assert_eq!(tail, "");

    assert_eq!(parsed.target_table_name, "server");

    assert_eq!(parsed.map[0].value_fields.len(), 2);
    assert_eq!(parsed.map[0].value_fields[0].key, "hostname");
    assert_eq!(parsed.map[0].value_fields[0].value, "mclassen");
    assert_eq!(parsed.map[0].value_fields[1].key, "dizzle");
    assert_eq!(parsed.map[0].value_fields[1].value, "shizzle");

    assert_eq!(parsed.map[0].extra_data.len(), 1);
    assert_eq!(parsed.map[0].extra_data[0].target_table_name, "disk");
    assert_eq!(parsed.map[0].extra_data[0].map[0].value_fields.len(), 3);
    assert_eq!(
        parsed.map[0].extra_data[0].map[0].value_fields[0].key,
        "dev_slot"
    );
    assert_eq!(
        parsed.map[0].extra_data[0].map[0].value_fields[0].value,
        "/dev/sda"
    );
    assert_eq!(
        parsed.map[0].extra_data[0].map[0].value_fields[1].key,
        "bozo"
    );
    assert_eq!(
        parsed.map[0].extra_data[0].map[0].value_fields[1].value,
        "7"
    );
    assert_eq!(
        parsed.map[0].extra_data[0].map[0].value_fields[2].key,
        "lozo"
    );
    assert_eq!(
        parsed.map[0].extra_data[0].map[0].value_fields[2].value,
        "7.77"
    );
}

#[test]
fn test_nested_structs_parse_arrays() {
    let source = r#"DATA STRUCT server [
    {
        hostname: mclassen,
        dizzle:shizzle ,
        WITH disk [
            {
                dev_slot: "/dev/sda" ,
                bozo: 7,
                lozo:7.77,
            },
            {
                dev_slot: "/dev/sdb" ,
                bozo: 8,
                lozo:8.88,
            },
        ] ,
    },
    {
        hostname: thicc,
        dizzle: boi,
    }
]"#;

    let (tail, parsed) = parse_table_data_structs(source).unwrap();

    assert_eq!(tail, "");

    assert_eq!(parsed.target_table_name, "server");

    assert_eq!(parsed.map.len(), 2);
    assert_eq!(parsed.map[0].value_fields.len(), 2);
    assert_eq!(parsed.map[0].value_fields[0].key, "hostname");
    assert_eq!(parsed.map[0].value_fields[0].value, "mclassen");
    assert_eq!(parsed.map[0].value_fields[1].key, "dizzle");
    assert_eq!(parsed.map[0].value_fields[1].value, "shizzle");

    assert_eq!(parsed.map[0].extra_data.len(), 1);
    assert_eq!(parsed.map[0].extra_data[0].target_table_name, "disk");
    assert_eq!(parsed.map[0].extra_data[0].map.len(), 2);

    assert_eq!(parsed.map[0].extra_data[0].map[0].value_fields.len(), 3);
    assert_eq!(
        parsed.map[0].extra_data[0].map[0].value_fields[0].key,
        "dev_slot"
    );
    assert_eq!(
        parsed.map[0].extra_data[0].map[0].value_fields[0].value,
        "/dev/sda"
    );
    assert_eq!(
        parsed.map[0].extra_data[0].map[0].value_fields[1].key,
        "bozo"
    );
    assert_eq!(
        parsed.map[0].extra_data[0].map[0].value_fields[1].value,
        "7"
    );
    assert_eq!(
        parsed.map[0].extra_data[0].map[0].value_fields[2].key,
        "lozo"
    );
    assert_eq!(
        parsed.map[0].extra_data[0].map[0].value_fields[2].value,
        "7.77"
    );

    assert_eq!(parsed.map[0].extra_data[0].map[1].value_fields.len(), 3);
    assert_eq!(
        parsed.map[0].extra_data[0].map[1].value_fields[0].key,
        "dev_slot"
    );
    assert_eq!(
        parsed.map[0].extra_data[0].map[1].value_fields[0].value,
        "/dev/sdb"
    );
    assert_eq!(
        parsed.map[0].extra_data[0].map[1].value_fields[1].key,
        "bozo"
    );
    assert_eq!(
        parsed.map[0].extra_data[0].map[1].value_fields[1].value,
        "8"
    );
    assert_eq!(
        parsed.map[0].extra_data[0].map[1].value_fields[2].key,
        "lozo"
    );
    assert_eq!(
        parsed.map[0].extra_data[0].map[1].value_fields[2].value,
        "8.88"
    );

    assert_eq!(parsed.map[1].value_fields.len(), 2);
    assert_eq!(parsed.map[1].value_fields[0].key, "hostname");
    assert_eq!(parsed.map[1].value_fields[0].value, "thicc");
    assert_eq!(parsed.map[1].value_fields[1].key, "dizzle");
    assert_eq!(parsed.map[1].value_fields[1].value, "boi");
}

#[test]
fn test_parse_row_check() {
    let res = parse_row_check("CHECK { hey { boi } stop {doin} dat }");
    let (tail, res) = res.unwrap();
    assert_eq!(tail, "");
    if let TableRowReturn::Check(c) = res {
        assert_eq!(
            c,
            TableRowCheck {
                expression: " hey { boi } stop {doin} dat ".to_string()
            }
        )
    } else {
        panic!()
    }
}

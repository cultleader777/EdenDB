#[cfg(test)]
use super::common::assert_compiles_data;
#[cfg(test)]
use super::common::assert_test_validaton_exception;
#[cfg(test)]
use super::common::assert_test_validaton_exception_return_error;
#[cfg(test)]
use crate::checker::errors::DatabaseValidationError;
#[cfg(test)]
use serde_json::json;

#[test]
fn test_datalog_proof_table_not_found() {
    assert_test_validaton_exception(
        DatabaseValidationError::DatalogProofTableNotFound {
            table_name: "non_existing_table".to_string(),
            proof_expression: "\n  some_output(Offender) :- t_cholo__id(Val, Offender), Val > 1.\n"
                .to_string(),
            comment: "no id is more than 1".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
}

DATA cholo {
    1;
    2;
    3;
}

PROOF "no id is more than 1" NONE EXIST OF non_existing_table DATALOG {
  some_output(Offender) :- t_cholo__id(Val, Offender), Val > 1.
}
        "#,
    );
}

#[test]
fn test_datalog_proof_bad_format() {
    assert_test_validaton_exception(
        DatabaseValidationError::DatalogProofOutputRuleNotFound {
            table_name: "cholo".to_string(),
            proof_expression: "\n  some_output(Offender) :- t_cholo__id(Val, Offender), Val > 1.\n".to_string(),
            comment: "no id is more than 1".to_string(),
            error: "Datalog proof must contain output rule for offenders in the format like 'OUTPUT(Offender) :- t_some_table__some_column(ColVal, Offender), ColVal = 7.'".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
}

DATA cholo {
    1;
    2;
    3;
}

PROOF "no id is more than 1" NONE EXIST OF cholo DATALOG {
  some_output(Offender) :- t_cholo__id(Val, Offender), Val > 1.
}
        "#,
    );
}

#[test]
fn test_datalog_proof_too_many_rules() {
    assert_test_validaton_exception(
        DatabaseValidationError::DatalogProofTooManyOutputRules {
            table_name: "cholo".to_string(),
            proof_expression: "\n  OUTPUT(Offender) :- t_cholo__id(Val, Offender), Val > 1.\n  OUTPUT(Offender) :- t_cholo__id(Val, Offender), Val < 1.\n".to_string(),
            comment: "no id is more than 1".to_string(),
            error: "Only one 'OUTPUT(Offender)' rule may exist in a datalog proof.".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
}

DATA cholo {
    1;
    2;
    3;
}

PROOF "no id is more than 1" NONE EXIST OF cholo DATALOG {
  OUTPUT(Offender) :- t_cholo__id(Val, Offender), Val > 1.
  OUTPUT(Offender) :- t_cholo__id(Val, Offender), Val < 1.
}
        "#,
    );
}

#[test]
fn test_datalog_proof_syntax_error() {
    let err = assert_test_validaton_exception_return_error(
        r#"
TABLE cholo {
    id INT,
}

DATA cholo {
    1;
    2;
    3;
}

PROOF "no id is more than 1" NONE EXIST OF cholo DATALOG {
  OUTPUT(Offender) :- this aint a thing boi.
}
        "#,
    );

    if let DatabaseValidationError::DatalogProofQueryParseError {
        error,
        table_name,
        comment,
        proof_expression,
    } = err
    {
        assert_eq!(table_name, "cholo");
        assert_eq!(comment, "no id is more than 1");
        assert_eq!(
            proof_expression,
            "\n  OUTPUT(Offender) :- this aint a thing boi.\n"
        );
        assert!(error.contains("expected comparison_operator"));
    } else {
        panic!()
    }
}

#[test]
fn test_datalog_proof_try_tricking_output_with_comments() {
    let err = assert_test_validaton_exception_return_error(
        r#"
TABLE cholo {
    id INT,
}

DATA cholo {
    1;
    2;
    3;
}

PROOF "no id is more than 1" NONE EXIST OF cholo DATALOG {
  % OUTPUT(Offender)
}
        "#,
    );

    if let DatabaseValidationError::DatalogProofNoRulesFound {
        error,
        table_name,
        comment,
        proof_expression,
    } = err
    {
        assert_eq!(table_name, "cholo");
        assert_eq!(comment, "no id is more than 1");
        assert_eq!(proof_expression, "\n  % OUTPUT(Offender)\n");
        assert_eq!(error, "No datalog queries found in proof. There must exist one OUTPUT(Offender) rule in a proof.");
    } else {
        panic!()
    }
}

#[test]
fn test_datalog_proof_incorrect_variable_order() {
    assert_test_validaton_exception(
        DatabaseValidationError::DatalogProofTableExpectedNotFoundInTheOutputQuery {
            table_name: "cholo".to_string(),
            proof_expression: "\n  OUTPUT(Offender) :- t_cholo__id(Offender, Val), Val > 1.\n".to_string(),
            comment: "no id is more than 1".to_string(),
            error: "Expected term like 't_cholo__<column name>(_, Offender)' not found in the output query".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
}

DATA cholo {
    1;
    2;
    3;
}

PROOF "no id is more than 1" NONE EXIST OF cholo DATALOG {
  OUTPUT(Offender) :- t_cholo__id(Offender, Val), Val > 1.
}
        "#,
    );
}

#[test]
fn test_datalog_proof_offenders_found() {
    assert_test_validaton_exception(
        DatabaseValidationError::DatalogProofOffendersFound {
            table_name: "cholo".to_string(),
            proof_expression: "\n    OUTPUT(Offender) :- t_cholo__id(Val, Offender).\n".to_string(),
            comment: "fail all".to_string(),
            offending_columns: vec![
                "{
  \"id\": 1.0
}"
                .to_string(),
                "{
  \"id\": 2.0
}"
                .to_string(),
                "{
  \"id\": 3.0
}"
                .to_string(),
            ],
        },
        r#"
TABLE cholo {
    id INT,
}

DATA cholo {
    1;
    2;
    3;
}

PROOF "fail all" NONE EXIST OF cholo DATALOG {
    OUTPUT(Offender) :- t_cholo__id(Val, Offender).
}
        "#,
    );
}

#[test]
fn test_datalog_proof_offenders_found_exact() {
    assert_test_validaton_exception(
        DatabaseValidationError::DatalogProofOffendersFound {
            table_name: "cholo".to_string(),
            proof_expression: "\n    OUTPUT(Offender) :- t_cholo__id(1, Offender).\n".to_string(),
            comment: "fail exact".to_string(),
            offending_columns: vec!["{
  \"id\": 1.0
}"
            .to_string()],
        },
        r#"
TABLE cholo {
    id INT,
}

DATA cholo {
    1;
    2;
    3;
}

PROOF "fail exact" NONE EXIST OF cholo DATALOG {
    OUTPUT(Offender) :- t_cholo__id(1, Offender).
}
        "#,
    );
}

#[test]
fn test_datalog_proof_incorrect_types() {
    assert_test_validaton_exception(
        DatabaseValidationError::DatalogProofQueryingFailure {
            table_name: "cholo".to_string(),
            proof_expression: "\n    OUTPUT(Offender) :- t_cholo__id(\"1\", Offender).\n".to_string(),
            comment: "fail 1".to_string(),
            error: "A requested operation cannot be performed as the values have incompatible types `integer`, `string`.".to_string(),
        },
        r#"
TABLE cholo {
    id INT,
}

DATA cholo {
    1;
    2;
    3;
}

PROOF "fail 1" NONE EXIST OF cholo DATALOG {
    OUTPUT(Offender) :- t_cholo__id("1", Offender).
}
        "#,
    );
}

#[test]
#[should_panic] // comparisons not implemented yet by datalog engine
fn test_datalog_proof_offenders_found_comparison() {
    assert_test_validaton_exception(
        DatabaseValidationError::DatalogProofOffendersFound {
            table_name: "cholo".to_string(),
            proof_expression: "\n    OUTPUT(Offender) :- t_cholo__id(Val, Offender), Val > 1.\n"
                .to_string(),
            comment: "fail more than 1".to_string(),
            offending_columns: vec![
                "{
  \"id\": 2.0
}"
                .to_string(),
                "{
  \"id\": 3.0
}"
                .to_string(),
            ],
        },
        r#"
TABLE cholo {
    id INT,
}

DATA cholo {
    1;
    2;
    3;
}

PROOF "fail more than 1" NONE EXIST OF cholo DATALOG {
    OUTPUT(Offender) :- t_cholo__id(Val, Offender), Val > 1.
}
        "#,
    );
}

#[test]
fn test_datalog_proof_offenders_found_none() {
    assert_compiles_data(
        r#"
TABLE cholo {
    id INT,
}

DATA cholo {
    1;
    2;
    3;
}

PROOF "fail all" NONE EXIST OF cholo DATALOG {
    OUTPUT(Offender) :- t_cholo__id(7, Offender).
}
        "#,
        json!({
            "cholo": [
                {"id": 1.0},
                {"id": 2.0},
                {"id": 3.0},
            ]
        }),
    );
}

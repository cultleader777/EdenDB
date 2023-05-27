#[cfg(test)]
use crate::checker::{logic::AllData};
#[cfg(test)]
use crate::checker::errors::DatabaseValidationError;
#[cfg(test)]
use crate::db_parser::InputSource;

#[cfg(test)]
pub fn random_test_dir() -> std::path::PathBuf {
    let tmp_dir = std::env::temp_dir();
    let tdir: String = rand::Rng::sample_iter(rand::thread_rng(), &rand::distributions::Alphanumeric)
        .take(17)
        .map(char::from)
        .collect();

    let res = tmp_dir.join(tdir);
    std::fs::create_dir(&res).unwrap();
    println!("Created testing directory: {}", res.to_str().unwrap());
    res
}

#[cfg(test)]
pub fn assert_compiles_data(source: &'static str, expected_json: serde_json::Value) {
    use assert_json_diff::assert_json_eq;

    let input = &mut [InputSource {
        contents: Some(source.to_string()),
        path: "test".to_string(),
        source_dir: None,
    }];

    let parsed = crate::db_parser::parse_sources(input);
    match &parsed {
        Err(e) => {
            panic!("Error when parsing: {}", e);
        }
        Ok(_) => {}
    }
    let parsed = parsed.unwrap();
    assert!(parsed.table_definitions().len() + parsed.table_data_segments().len() > 0);
    let all_data = AllData::new(parsed);
    match all_data {
        Ok(res) => {
            let out_json = res.data_as_json();
            assert_json_eq!(expected_json, out_json)
        }
        Err(e) => {
            panic!("Expected ok, got: {}", e)
        }
    }
}

#[cfg(test)]
pub fn assert_compiles_data_paths(source: &[String], expected_json: serde_json::Value) {
    use assert_json_diff::assert_json_eq;

    let mut input = source.iter().map(|i| {
        let dir_path = i.to_string();
        let mut p = std::fs::canonicalize(dir_path).unwrap();
        let pres = p.pop();
        assert!(pres);
        let p = p.as_path().to_str().unwrap().to_string();
        InputSource {
            contents: None,
            path: i.to_string(),
            source_dir: Some(p),
        }
    }).collect::<Vec<_>>();

    let parsed = crate::db_parser::parse_sources_with_external(input.as_mut_slice());
    match &parsed {
        Err(e) => {
            panic!("Error when parsing: {}", e);
        }
        Ok(_) => {}
    }
    let parsed = parsed.unwrap();
    assert!(parsed.table_definitions().len() + parsed.table_data_segments().len() > 0);
    let all_data = AllData::new(parsed);
    match all_data {
        Ok(res) => {
            let out_json = res.data_as_json();
            assert_json_eq!(expected_json, out_json)
        }
        Err(e) => {
            panic!("Expected ok, got: {}", e)
        }
    }
}

#[cfg(test)]
pub fn assert_test_validaton_exception(
    expected_exception: DatabaseValidationError,
    source: &'static str,
) {
    let input = &mut [InputSource {
        contents: Some(source.to_string()),
        path: "test".to_string(),
        source_dir: None,
    }];

    let parsed = crate::db_parser::parse_sources(input);
    assert!(parsed.is_ok());
    let parsed = parsed.unwrap();
    assert!(parsed.table_definitions().len() + parsed.table_data_segments().len() > 0);
    let all_data = AllData::new(parsed);
    match all_data {
        Ok(_) => {
            panic!("Expected database validation error, test passed")
        }
        Err(e) => {
            assert_eq!(expected_exception, e)
        }
    }
}

#[cfg(test)]
pub fn assert_test_validaton_exception_return_error(
    source: &'static str,
) -> DatabaseValidationError {
    let input = &mut [InputSource {
        path: "test".to_string(),
        contents: Some(source.to_string()),
        source_dir: None,
    }];

    let parsed = crate::db_parser::parse_sources(input);
    assert!(parsed.is_ok());
    let parsed = parsed.unwrap();
    assert!(parsed.table_definitions().len() + parsed.table_data_segments().len() > 0);
    let all_data = AllData::new(parsed);
    match all_data {
        Ok(_) => {
            panic!("Expected database validation error, test passed")
        }
        Err(e) => {
            e
        }
    }
}
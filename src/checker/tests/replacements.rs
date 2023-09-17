#[cfg(test)]
use crate::checker::errors::DatabaseValidationError;
#[cfg(test)]
use super::common::assert_compiles_data_paths;
#[cfg(test)]
use super::common::assert_compiles_data_with_source_replacements;
#[cfg(test)]
use super::common::assert_compiles_data_paths_error_source_replacements;
#[cfg(test)]
use super::common::random_test_dir;
#[cfg(test)]
use serde_json::json;
#[cfg(test)]
use pretty_assertions::assert_eq;

#[test]
fn test_source_file_replacements_no_table() {
    let tmp_dir = random_test_dir();

    std::fs::write(
        tmp_dir.join("root.edl"),
        r#"
      TABLE test_table {
        id INT PRIMARY KEY,
        v1 TEXT,
        v2 FLOAT,
        v3 BOOL,
      }

      DATA test_table(id, v1, v2, v3) {
        1, " henlo ", 3.14, true;// salookie
        2, hey, 321, false;
      }

      DATA STRUCT test_table [ // dookie
        {
          id: 3,
          v1: " ho ",
          v2: 1.2,
          v3: true,
        },
        {
          id: 4,
          v1: here,
          v2: 1.3,
          v3: false,
        },
      ]
"#
        .replace("TMP_DIR", tmp_dir.to_str().unwrap()),
    )
    .unwrap();

    let replacements = r#"
{
  "makiki": [
    {
      "primary_key": "1",
      "replacements": {
        "id": "5",
        "v1": " holo ",
        "v2": "3.41",
        "v3": "false"
      }
    }
  ]
}
"#;

    let paths = [
      "root.edl",
    ]
        .iter()
        .map(|i| tmp_dir.join(i).to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    assert_eq!(
        assert_compiles_data_paths_error_source_replacements(
            paths.as_slice(),
            replacements,
        ),
        DatabaseValidationError::ReplacementsTargetTableDoesntExist {
            table: "makiki".to_string(),
        },
    )
}

#[test]
fn test_source_file_replacements_no_primary_key() {
    let tmp_dir = random_test_dir();

    std::fs::write(
        tmp_dir.join("root.edl"),
        r#"
      TABLE test_table {
        id INT,
        v1 TEXT,
        v2 FLOAT,
        v3 BOOL,
      }

      DATA test_table(id, v1, v2, v3) {
        1, " henlo ", 3.14, true;// salookie
        2, hey, 321, false;
      }

      DATA STRUCT test_table [ // dookie
        {
          id: 3,
          v1: " ho ",
          v2: 1.2,
          v3: true,
        },
        {
          id: 4,
          v1: here,
          v2: 1.3,
          v3: false,
        },
      ]
"#
        .replace("TMP_DIR", tmp_dir.to_str().unwrap()),
    )
    .unwrap();

    let replacements = r#"
{
  "test_table": [
    {
      "primary_key": "1",
      "replacements": {
        "id": "5",
        "v1": " holo ",
        "v2": "3.41",
        "v3": "false"
      }
    }
  ]
}
"#;

    let paths = [
      "root.edl",
    ]
        .iter()
        .map(|i| tmp_dir.join(i).to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    assert_eq!(
        assert_compiles_data_paths_error_source_replacements(
            paths.as_slice(),
            replacements,
        ),
        DatabaseValidationError::ReplacementsTargetTableDoesntHavePrimaryKey {
            table: "test_table".to_string(),
        },
    )
}

#[test]
fn test_source_file_replacement_cannot_be_for_generated_column() {
    let tmp_dir = random_test_dir();

    std::fs::write(
        tmp_dir.join("root.edl"),
        r#"
      TABLE test_table {
        id INT PRIMARY KEY,
        v1 TEXT,
        v2 FLOAT GENERATED AS { 12.0 },
        v3 BOOL,
      }

      DATA test_table(id, v1, v2, v3) {
        1, " henlo ", 3.14, true;// salookie
        2, hey, 321, false;
      }

      DATA STRUCT test_table [ // dookie
        {
          id: 3,
          v1: " ho ",
          v2: 1.2,
          v3: true,
        },
        {
          id: 4,
          v1: here,
          v2: 1.3,
          v3: false,
        },
      ]
"#
        .replace("TMP_DIR", tmp_dir.to_str().unwrap()),
    )
    .unwrap();

    let replacements = r#"
{
  "test_table": [
    {
      "primary_key": "1",
      "replacements": {
        "id": "5",
        "v1": " holo ",
        "v2": "3.41",
        "v3": "false"
      }
    }
  ]
}
"#;

    let paths = [
      "root.edl",
    ]
        .iter()
        .map(|i| tmp_dir.join(i).to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    assert_eq!(
        assert_compiles_data_paths_error_source_replacements(
            paths.as_slice(),
            replacements,
        ),
        DatabaseValidationError::ReplacementCannotBeProvidedForGeneratedColumn {
            table: "test_table".to_string(),
            replacement_primary_key: "1".to_string(),
            generated_column: "v2".to_string(),
        },
    )
}

#[test]
fn test_source_file_replacement_dupe_primary_key() {
    let tmp_dir = random_test_dir();

    std::fs::write(
        tmp_dir.join("root.edl"),
        r#"
      TABLE test_table {
        id INT PRIMARY KEY,
        v1 TEXT,
        v2 FLOAT,
        v3 BOOL,
      }

      DATA test_table(id, v1, v2, v3) {
        1, " henlo ", 3.14, true;// salookie
        2, hey, 321, false;
      }

      DATA STRUCT test_table [ // dookie
        {
          id: 3,
          v1: " ho ",
          v2: 1.2,
          v3: true,
        },
        {
          id: 4,
          v1: here,
          v2: 1.3,
          v3: false,
        },
      ]
"#
        .replace("TMP_DIR", tmp_dir.to_str().unwrap()),
    )
    .unwrap();

    let replacements = r#"
{
  "test_table": [
    {
      "primary_key": "1",
      "replacements": {
        "id": "5",
        "v1": " holo ",
        "v2": "3.41",
        "v3": "false"
      }
    },
    {
      "primary_key": "1",
      "replacements": {
        "id": "6",
        "v1": " holo ",
        "v2": "3.41",
        "v3": "false"
      }
    }
  ]
}
"#;

    let paths = [
      "root.edl",
    ]
        .iter()
        .map(|i| tmp_dir.join(i).to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    assert_eq!(
        assert_compiles_data_paths_error_source_replacements(
            paths.as_slice(),
            replacements,
        ),
        DatabaseValidationError::ReplacementsDuplicatePrimaryKeyDetected {
            table: "test_table".to_string(),
            replacement_primary_key: "1".to_string(),
        },
    )
}

#[test]
fn test_source_file_replacement_column_not_found() {
    let tmp_dir = random_test_dir();

    std::fs::write(
        tmp_dir.join("root.edl"),
        r#"
      TABLE test_table {
        id INT PRIMARY KEY,
        v1 TEXT,
        v2 FLOAT,
        v3 BOOL,
      }

      DATA test_table(id, v1, v2, v3) {
        1, " henlo ", 3.14, true;// salookie
        2, hey, 321, false;
      }

      DATA STRUCT test_table [ // dookie
        {
          id: 3,
          v1: " ho ",
          v2: 1.2,
          v3: true,
        },
        {
          id: 4,
          v1: here,
          v2: 1.3,
          v3: false,
        },
      ]
"#
        .replace("TMP_DIR", tmp_dir.to_str().unwrap()),
    )
    .unwrap();

    let replacements = r#"
{
  "test_table": [
    {
      "primary_key": "1",
      "replacements": {
        "id": "5",
        "v1": " holo ",
        "v2": "3.41",
        "v4": "false"
      }
    }
  ]
}
"#;

    let paths = [
      "root.edl",
    ]
        .iter()
        .map(|i| tmp_dir.join(i).to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    assert_eq!(
        assert_compiles_data_paths_error_source_replacements(
            paths.as_slice(),
            replacements,
        ),
        DatabaseValidationError::ReplacementsColumnNotFound {
            table: "test_table".to_string(),
            replacement_primary_key: "1".to_string(),
            column_not_found: "v4".to_string(),
            available_columns: vec!["id".to_string(), "v1".to_string(), "v2".to_string(), "v3".to_string()],
        },
    )
}

#[test]
fn test_source_file_replacement_never_used() {
    let tmp_dir = random_test_dir();

    std::fs::write(
        tmp_dir.join("root.edl"),
        r#"
      TABLE test_table {
        id INT PRIMARY KEY,
        v1 TEXT,
        v2 FLOAT,
        v3 BOOL,
      }

      DATA test_table(id, v1, v2, v3) {
        1, " henlo ", 3.14, true;// salookie
        2, hey, 321, false;
      }

      DATA STRUCT test_table [ // dookie
        {
          id: 3,
          v1: " ho ",
          v2: 1.2,
          v3: true,
        },
        {
          id: 4,
          v1: here,
          v2: 1.3,
          v3: false,
        },
      ]
"#
        .replace("TMP_DIR", tmp_dir.to_str().unwrap()),
    )
    .unwrap();

    let replacements = r#"
{
  "test_table": [
    {
      "primary_key": "10",
      "replacements": {
        "id": "5",
        "v1": " holo ",
        "v2": "3.41",
        "v3": "false"
      }
    }
  ]
}
"#;

    let paths = [
      "root.edl",
    ]
        .iter()
        .map(|i| tmp_dir.join(i).to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    assert_eq!(
        assert_compiles_data_paths_error_source_replacements(
            paths.as_slice(),
            replacements,
        ),
        DatabaseValidationError::ReplacementNeverUsed {
            table: "test_table".to_string(),
            replacement_primary_key: "10".to_string(),
            replacement_uses: 0,
            replacement_columns: vec![
                "id".to_string(),
                "v1".to_string(),
                "v2".to_string(),
                "v3".to_string(),
            ],
            replacement_values: vec![
                "5".to_string(),
                " holo ".to_string(),
                "3.41".to_string(),
                "false".to_string(),
            ],
        },
    )
}

#[test]
fn test_source_file_replacement_for_lua_data_not_allowed() {
    let tmp_dir = random_test_dir();

    std::fs::write(
        tmp_dir.join("root.edl"),
        r#"
INCLUDE LUA {
  data('test_table', {
    id = 5,
    v1 = 'something',
    v2 = 2.74,
    v3 = true,
  })
}

      TABLE test_table {
        id INT PRIMARY KEY,
        v1 TEXT,
        v2 FLOAT,
        v3 BOOL,
      }

      DATA test_table(id, v1, v2, v3) {
        1, " henlo ", 3.14, true;// salookie
        2, hey, 321, false;
      }

      DATA STRUCT test_table [ // dookie
        {
          id: 3,
          v1: " ho ",
          v2: 1.2,
          v3: true,
        },
        {
          id: 4,
          v1: here,
          v2: 1.3,
          v3: false,
        },
      ]
"#
        .replace("TMP_DIR", tmp_dir.to_str().unwrap()),
    )
    .unwrap();

    let replacements = r#"
{
  "test_table": [
    {
      "primary_key": "5",
      "replacements": {
        "id": "8",
        "v1": " holo ",
        "v2": "3.41",
        "v3": "false"
      }
    }
  ]
}
"#;

    let paths = [
      "root.edl",
    ]
        .iter()
        .map(|i| tmp_dir.join(i).to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    assert_eq!(
        assert_compiles_data_paths_error_source_replacements(
            paths.as_slice(),
            replacements,
        ),
        DatabaseValidationError::ReplacementOverLuaGeneratedValuesIsNotSupported {
            table: "test_table".to_string(),
            replacement_primary_key: "5".to_string(),
        },
    )
}

#[test]
fn test_source_file_replacement_for_parent_keys_not_allowed() {
    let tmp_dir = random_test_dir();

    std::fs::write(
        tmp_dir.join("root.edl"),
        r#"
      TABLE test_table {
        parent_id INT PRIMARY KEY,
      }

      TABLE child_table {
        child_id TEXT PRIMARY KEY CHILD OF test_table,
        v1 TEXT,
        v2 FLOAT,
        v3 BOOL,
      }

      DATA test_table(id) {
        // replacement of parent key in child
        // would break this source
        1 WITH child_table(child_id, v1, v2, v3) {
          cheld, " henlo ", 3.14, true
        };
        2;
      }

      DATA STRUCT test_table [ // dookie
        {
          parent_id: 3,
          WITH child_table {
            child_id: other_child,
            v1: " ho ",
            v2: 1.2,
            v3: true,
          }
        },
        {
          parent_id: 4,
        },
      ]
"#
        .replace("TMP_DIR", tmp_dir.to_str().unwrap()),
    )
    .unwrap();

    let replacements = r#"
{
  "child_table": [
    {
      "primary_key": "1=>cheld",
      "replacements": {
        "parent_id": "123"
      }
    }
  ]
}
"#;

    let paths = [
      "root.edl",
    ]
        .iter()
        .map(|i| tmp_dir.join(i).to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    assert_eq!(
        assert_compiles_data_paths_error_source_replacements(
            paths.as_slice(),
            replacements,
        ),
        DatabaseValidationError::ReplacementsCannotReplaceParentPrimaryKey {
            table: "child_table".to_string(),
            replacement_primary_key: "1=>cheld".to_string(),
            parent_primary_key_column: "parent_id".to_string(),
        },
    )
}

#[test]
fn test_source_file_replacements() {
    let tmp_dir = random_test_dir();

    std::fs::write(
        tmp_dir.join("root.edl"),
        r#"
      TABLE test_table {
        id INT PRIMARY KEY,
        v1 TEXT,
        v2 FLOAT,
        v3 BOOL,
      }

      DATA test_table(id, v1, v2, v3) {
        1, " henlo ", 3.14, true;// salookie
        2, hey, 321, false;
      }

      DATA STRUCT test_table [ // dookie
        {
          id: 3,
          v1: " ho ",
          v2: 1.2,
          v3: true,
        },
        {
          id: 4,
          v1: here,
          v2: 1.3,
          v3: false,
        },
      ]
"#
        .replace("TMP_DIR", tmp_dir.to_str().unwrap()),
    )
    .unwrap();

    let replacements = r#"
{
  "test_table": [
    {
      "primary_key": "1",
      "replacements": {
        "id": "5",
        "v1": " holo ",
        "v2": "3.41",
        "v3": "false"
      }
    },
    {
      "primary_key": "2",
      "replacements": {
        "id": "6",
        "v1": " hey!@#))* ",
        "v2": "4.21",
        "v3": "true"
      }
    },
    {
      "primary_key": "3",
      "replacements": {
        "id": "7",
        "v1": "hoooo",
        "v2": "44.21",
        "v3": "false"
      }
    },
    {
      "primary_key": "4",
      "replacements": {
        "id": "8",
        "v1": "@#!$#))*",
        "v2": "1.7",
        "v3": "true"
      }
    }
  ]
}
"#;

    let paths = [
      "root.edl",
    ]
        .iter()
        .map(|i| tmp_dir.join(i).to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    let output_table =
        json!({
            "test_table": [
                {"id": 5.0, "v1": " holo ", "v2": 3.41, "v3": false},
                {"id": 6.0, "v1": " hey!@#))* ", "v2": 4.21, "v3": true},
                {"id": 7.0, "v1": "hoooo", "v2": 44.21, "v3": false},
                {"id": 8.0, "v1": "@#!$#))*", "v2": 1.7, "v3": true},
            ],
        });

    assert_compiles_data_with_source_replacements(
        paths.as_slice(),
        replacements,
        &output_table,
    );

    let output = std::fs::read_to_string(tmp_dir.join("root.edl")).unwrap();
    assert_eq!(
        r#"
      TABLE test_table {
        id INT PRIMARY KEY,
        v1 TEXT,
        v2 FLOAT,
        v3 BOOL,
      }

      DATA test_table(id, v1, v2, v3) {
        5, " holo ", 3.41, false;// salookie
        6, " hey!@#))* ", 4.21, true;
      }

      DATA STRUCT test_table [ // dookie
        {
          id: 7,
          v1: "hoooo",
          v2: 44.21,
          v3: false,
        },
        {
          id: 8,
          v1: "@#!$#))*",
          v2: 1.7,
          v3: true,
        },
      ]
"#,
        output
    );

    // still compiles data and syntax is valid post replacements
    assert_compiles_data_paths(
        paths.as_slice(),
        output_table,
    );
}

#[test]
fn test_source_file_child_replacements() {
    let tmp_dir = random_test_dir();

    std::fs::write(
        tmp_dir.join("root.edl"),
        r#"
      TABLE test_table {
        id INT PRIMARY KEY,
      }

      TABLE child_table {
        child_id TEXT PRIMARY KEY CHILD OF test_table,
        v1 TEXT,
        v2 FLOAT,
        v3 BOOL,
      }

      DATA test_table(id) {
        1 WITH child_table(child_id, v1, v2, v3) {
          cheld, " henlo ", 3.14, true
        };
        2;
      }

      DATA STRUCT test_table [ // dookie
        {
          id: 3,
          WITH child_table {
            child_id: other_child,
            v1: " ho ",
            v2: 1.2,
            v3: true,
          }
        },
        {
          id: 4,
        },
      ]
"#
        .replace("TMP_DIR", tmp_dir.to_str().unwrap()),
    )
    .unwrap();

    let replacements = r#"
{
  "child_table": [
    {
      "primary_key": "1=>cheld",
      "replacements": {
        "v1": " holo ",
        "v2": "3.41",
        "v3": "false"
      }
    },
    {
      "primary_key": "3=>other_child",
      "replacements": {
        "v1": " hey!@#))* ",
        "v2": "4.21",
        "v3": "true"
      }
    }
  ]
}
"#;

    let paths = [
      "root.edl",
    ]
        .iter()
        .map(|i| tmp_dir.join(i).to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    let output_table =
        json!({
            "test_table": [
                {"id": 1.0},
                {"id": 2.0},
                {"id": 3.0},
                {"id": 4.0},
            ],
            "child_table": [
                {"id": 1.0, "child_id": "cheld", "v1": " holo ", "v2": 3.41, "v3": false},
                {"id": 3.0, "child_id": "other_child", "v1": " hey!@#))* ", "v2": 4.21, "v3": true},
            ],
        });

    assert_compiles_data_with_source_replacements(
        paths.as_slice(),
        replacements,
        &output_table,
    );

    let output = std::fs::read_to_string(tmp_dir.join("root.edl")).unwrap();
    assert_eq!(
        r#"
      TABLE test_table {
        id INT PRIMARY KEY,
      }

      TABLE child_table {
        child_id TEXT PRIMARY KEY CHILD OF test_table,
        v1 TEXT,
        v2 FLOAT,
        v3 BOOL,
      }

      DATA test_table(id) {
        1 WITH child_table(child_id, v1, v2, v3) {
          cheld, " holo ", 3.41, false
        };
        2;
      }

      DATA STRUCT test_table [ // dookie
        {
          id: 3,
          WITH child_table {
            child_id: other_child,
            v1: " hey!@#))* ",
            v2: 4.21,
            v3: true,
          }
        },
        {
          id: 4,
        },
      ]
"#,
        output
    );

    // still compiles data and syntax is valid post replacements
    assert_compiles_data_paths(
        paths.as_slice(),
        output_table,
    );
}

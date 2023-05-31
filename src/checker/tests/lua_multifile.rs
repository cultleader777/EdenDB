#[cfg(test)]
use serde_json::json;
#[cfg(test)]
use super::common::assert_compiles_data_paths;
#[cfg(test)]
use super::common::random_test_dir;

#[test]
fn lua_source_dir_constant() {
    let tmp_dir = random_test_dir();
    std::fs::create_dir(tmp_dir.join("tst_a")).unwrap();
    std::fs::create_dir(tmp_dir.join("tst_b")).unwrap();

    let src_to_write = r#"
      data('test_table', { dirname = SOURCE_DIR })
    "#;

    std::fs::write(tmp_dir.join("test.lua"), src_to_write).unwrap();
    std::fs::write(tmp_dir.join("tst_a").join("test.lua"), src_to_write).unwrap();
    std::fs::write(tmp_dir.join("tst_b").join("test.lua"), src_to_write).unwrap();

    std::fs::write(tmp_dir.join("root.edl"), r#"
      TABLE test_table {
        dirname TEXT,
      }

      INCLUDE LUA "TMP_DIR/test.lua"
      INCLUDE LUA "TMP_DIR/tst_a/test.lua"
      INCLUDE LUA "TMP_DIR/tst_b/test.lua"
    "#.replace("TMP_DIR", tmp_dir.to_str().unwrap())).unwrap();

    let paths = [
      "root.edl",
    ].iter()
        .map(|i| tmp_dir.join(i).to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    let tmp_dir = tmp_dir.to_str().unwrap();
    assert_compiles_data_paths(
        paths.as_slice(),
        json!({
            "test_table": [
                {"dirname": format!("{}", tmp_dir)},
                {"dirname": format!("{}/tst_a", tmp_dir)},
                {"dirname": format!("{}/tst_b", tmp_dir)},
            ],
        })
    );
}
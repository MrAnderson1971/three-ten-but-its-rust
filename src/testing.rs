use crate::dataset::{Value, load_dataset};
use crate::query::{Query, execute_query};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;

#[derive(Deserialize)]
pub struct Test {
    pub title: String,
    pub query: Query,
    #[serde(rename = "isQueryValid")]
    pub is_query_valid: bool,
    pub result: Vec<BTreeMap<String, Value>>,
}

#[test]
fn folder_test() {
    let dataset = load_dataset("pair.zip").unwrap();
    let paths = fs::read_dir("./data").unwrap();
    for path in paths {
        let json = fs::read_to_string(&path.unwrap().path()).unwrap();
        let test_case = serde_json::from_str::<Test>(&json).unwrap();
        println!("{}", test_case.title);
        match execute_query(&test_case.query, &dataset) {
            Ok(result) if test_case.is_query_valid => {
                let expected = BTreeSet::from_iter(test_case.result.into_iter());
                let actual = BTreeSet::from_iter(result.into_iter());
                assert_eq!(actual, expected);
            },
            Err(_) if !test_case.is_query_valid => {}
            _ => panic!("fail")
        }
    }
}

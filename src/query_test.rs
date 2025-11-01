use std::collections::HashMap;
use crate::query::{Filter, Query};

#[test]
fn test_simple() {
    let json = r#"{
    "WHERE":{
       "GT":{
          "sections_avg":97
       }
    },
    "OPTIONS":{
       "COLUMNS":[
          "sections_dept",
          "sections_avg"
       ],
       "ORDER":"sections_avg"
    }
} "#;
    let deserialized: Query = serde_json::from_str(&json).unwrap();
    let expected: HashMap<String, f32> = [("sections_avg".to_string(), 97f32)]
        .iter()
        .cloned()
        .collect();
    let actual = match deserialized.r#where.as_ref().unwrap() {
        Filter::GT { gt: g } => g,
        _ => panic!("not gt"),
    };
    assert_eq!(*actual, expected);

    let expected = ["sections_dept".to_string(), "sections_avg".to_string()];
    let actual = deserialized.options.columns.as_slice();
    assert_eq!(actual, expected.as_slice());

    assert_eq!(deserialized.options.order, Some("sections_avg".to_string()));

    println!("{:#?}", deserialized);
}

#[test]
fn test_complex() {
    let json = r#"{
    "WHERE":{
       "OR":[
          {
             "AND":[
                {
                   "GT":{
                      "ubc_avg":90
                   }
                },
                {
                   "IS":{
                      "ubc_dept":"adhe"
                   }
                }
             ]
          },
          {
             "EQ":{
                "ubc_avg":95
             }
          }
       ]
    },
    "OPTIONS":{
       "COLUMNS":[
          "ubc_dept",
          "ubc_id",
          "ubc_avg"
       ],
       "ORDER":"ubc_avg"
    }
} "#;
    let query: Query = serde_json::from_str(&json).unwrap();
    assert_eq!(
        format!("{:?}", query),
        r#"Query { where: Some(OR { or: [AND { and: [GT { gt: {"ubc_avg": 90.0} }, IS { is: {"ubc_dept": "adhe"} }] }, EQ { eq: {"ubc_avg": 95.0} }] }), options: Options { columns: ["ubc_dept", "ubc_id", "ubc_avg"], order: Some("ubc_avg") } }"#
    );
}

#[test]
#[should_panic]
fn test_no_options() {
    let json = r#"{
    "WHERE":{
       "GT":{
          "sections_avg":97
       }
    }
} "#;
    serde_json::from_str::<Query>(&json).unwrap();
}

#[test]
#[should_panic]
fn test_cmp_string() {
    let json = r#"{
    "WHERE":{
       "GT":{
          "sections_avg": "adhe"
       }
    },
    "OPTIONS":{
       "COLUMNS":[
          "sections_dept",
          "sections_avg"
       ],
       "ORDER":"sections_avg"
    }
} "#;
    serde_json::from_str::<Query>(&json).unwrap();
}

#[test]
#[should_panic]
fn test_unknown_fields() {
    let json = r#"{
    "WHERE":{
       "GT":{
          "sections_avg": "adhe"
       }
    },
    "OPTIONS":{
       "COLUMNS":[
          "sections_dept",
          "sections_avg"
       ],
       "ORDER":"sections_avg"
    },
    "HAVING": "blank"
} "#;
    serde_json::from_str::<Query>(&json).unwrap();
}

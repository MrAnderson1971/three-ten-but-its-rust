use crate::dataset::load_dataset;
use crate::query::{Filter, Query, execute_query};
use crate::types::KVPair;
use ordered_float::OrderedFloat;

#[test]
fn test_simple() {
    let json = r#"{
    "WHERE":{
       "GT":{
          "courses_avg":97
       }
    },
    "OPTIONS":{
       "COLUMNS":[
          "courses_dept",
          "courses_avg"
       ],
       "ORDER":"courses_avg"
    }
} "#;
    let deserialized: Query = serde_json::from_str(&json).unwrap();
    let expected = KVPair {
        key: "courses_avg".to_string(),
        value: OrderedFloat::from(97f32),
    };
    let actual = match deserialized.r#where {
        Filter::GT { gt: ref g } => g,
        _ => panic!("not gt"),
    };
    assert_eq!(*actual, expected);

    let expected = ["courses_dept".to_string(), "courses_avg".to_string()];
    let actual = deserialized.options.columns.as_slice();
    assert_eq!(actual, expected.as_slice());

    //assert_eq!(deserialized.options.order, Some("courses_avg".to_string()));

    println!("{:#?}", deserialized);

    let dataset = load_dataset("pair.zip").unwrap();
    let result = execute_query(&deserialized, &dataset);
    println!("result {:#?}", result)
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
                      "courses_avg":90
                   }
                },
                {
                   "IS":{
                      "courses_dept":"adhe"
                   }
                }
             ]
          },
          {
             "EQ":{
                "courses_avg":95
             }
          }
       ]
    },
    "OPTIONS":{
       "COLUMNS":[
          "courses_dept",
          "courses_id",
          "courses_avg"
       ],
       "ORDER":"courses_avg"
    }
} "#;
    let query: Query = serde_json::from_str(&json).unwrap();
    assert_eq!(
        format!("{:?}", query),
        r#"Query { where: Some(OR { or: [AND { and: [GT { gt: {"courses_avg": 90.0} }, IS { is: {"courses_dept": "adhe"} }] }, EQ { eq: {"courses_avg": 95.0} }] }), options: Options { columns: ["courses_dept", "courses_id", "courses_avg"], order: Some("courses_avg") } }"#
    );

    let dataset = load_dataset("pair.zip").unwrap();
    let result = execute_query(&query, &dataset);
    println!("{:#?}", result);
}

#[test]
#[should_panic]
fn test_no_options() {
    let json = r#"{
    "WHERE":{
       "GT":{
          "courses_avg":97
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
          "courses_avg": "adhe"
       }
    },
    "OPTIONS":{
       "COLUMNS":[
          "courses_dept",
          "courses_avg"
       ],
       "ORDER":"courses_avg"
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
          "courses_avg": "adhe"
       }
    },
    "OPTIONS":{
       "COLUMNS":[
          "courses_dept",
          "courses_avg"
       ],
       "ORDER":"courses_avg"
    },
    "HAVING": "blank"
} "#;
    serde_json::from_str::<Query>(&json).unwrap();
}

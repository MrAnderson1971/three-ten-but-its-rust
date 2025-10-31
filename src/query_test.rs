#[cfg(test)]
mod query_test {
    use crate::query::query::{Filter, Query};
    use std::collections::HashMap;

    #[test]
    fn test_simple() {
        let query = r#"{
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
        let deserialized: Query = serde_json::from_str(&query).unwrap();
        let expected: HashMap<String, f64> = [("sections_avg".to_string(), 97f64)]
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
        assert_eq!(format!("{:?}", query), r#"Query { where: Some(OR { or: [AND { and: [GT { gt: {"ubc_avg": 90.0} }, IS { is: {"ubc_dept": "adhe"} }] }, EQ { eq: {"ubc_avg": 95.0} }] }), options: Options { columns: ["ubc_dept", "ubc_id", "ubc_avg"], order: Some("ubc_avg") } }"#);
    }
}

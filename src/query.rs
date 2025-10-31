mod query {
    use serde::Deserialize;
    use std::collections::HashMap;

    #[derive(Deserialize, Debug)]
    #[serde(rename_all = "UPPERCASE")]
    pub struct Query {
        pub r#where: Option<Filter>,
        pub options: Options,
    }

    #[derive(Deserialize, Debug)]
    #[serde(rename_all = "UPPERCASE")]
    pub struct Options {
        pub columns: Vec<String>,
        pub order: Option<String>,
    }

    #[derive(Deserialize, Debug)]
    #[serde(untagged)]
    pub enum Filter {
        AND {
            #[serde(rename = "AND")]
            and: Vec<Filter>,
        },
        OR {
            #[serde(rename = "OR")]
            or: Vec<Filter>,
        },
        NOT {
            #[serde(rename = "NOT")]
            not: Box<Filter>,
        },
        LT {
            #[serde(rename = "LT")]
            lt: HashMap<String, f64>,
        },
        GT {
            #[serde(rename = "GT")]
            gt: HashMap<String, f64>,
        },
        EQ {
            #[serde(rename = "EQ")]
            eq: HashMap<String, f64>,
        },
        IS {
            #[serde(rename = "IS")]
            is: HashMap<String, String>,
        },
    }
}

#[cfg(test)]
#[path = "query_test.rs"]
mod query_test;

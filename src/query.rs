use crate::dataset::Value::{Num, Str};
use crate::dataset::{Course, Value};
use crate::errors::EngineError;
use crate::errors::EngineError::ResultToLargeError;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;

type Func<'a> = Box<dyn Fn(&Course) -> Result<bool, Box<dyn Error>> + 'a>;

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
        lt: HashMap<String, f32>,
    },
    GT {
        #[serde(rename = "GT")]
        gt: HashMap<String, f32>,
    },
    EQ {
        #[serde(rename = "EQ")]
        eq: HashMap<String, f32>,
    },
    IS {
        #[serde(rename = "IS")]
        is: HashMap<String, String>,
    },
}

fn parse_and(and: &'_ Vec<Filter>) -> Func<'_> {
    let filters: Vec<_> = and.iter().map(|filter| parse_filter(filter)).collect();
    Box::new(move |course| {
        for filter in filters.iter() {
            if !filter(course)? {
                return Ok(false);
            }
        }
        Ok(true)
    })
}

fn parse_or(or: &'_ Vec<Filter>) -> Func<'_> {
    let filters: Vec<_> = or.iter().map(|filter| parse_filter(filter)).collect();
    Box::new(move |course| {
        for filter in filters.iter() {
            if filter(course)? {
                return Ok(true);
            }
        }
        Ok(false)
    })
}

fn parse_comparison(
    args: &HashMap<String, f32>,
    course: &Course,
    predicate: impl FnOnce(f32, f32) -> bool,
    op: &'static str,
) -> Result<bool, Box<dyn Error>> {
    let (col, val) = args.iter().next().unwrap();
    match course.get(col) {
        Ok(Num(i)) => Ok(predicate(i, *val)),
        Ok(_) => Err(EngineError::TypeError {
            operation: op,
            field: col.clone(),
        }
        .into()),
        Err(e) => Err(EngineError::FieldNotFound(e).into()),
    }
}

fn parse_filter(filter: &'_ Filter) -> Func<'_> {
    match filter {
        Filter::AND { and } => parse_and(and),
        Filter::OR { or } => parse_or(or),
        Filter::NOT { not } => Box::new(|course| Ok(!parse_filter(not)(course)?)),
        Filter::LT { lt } => {
            Box::new(move |course| parse_comparison(&lt, &course, |a, b| a < b, "lt"))
        }
        Filter::GT { gt } => {
            Box::new(move |course| parse_comparison(&gt, &course, |a, b| a > b, "gt"))
        }
        Filter::EQ { eq } => Box::new(move |course| {
            parse_comparison(&eq, &course, |a, b| (a - b).abs() < 1e-4, "eq")
        }),
        Filter::IS { is } => Box::new(move |course| {
            let (col, val) = is.iter().next().unwrap();
            match course.get(col) {
                Ok(Str(s)) => Ok(s == *val),
                Ok(_) => Err(EngineError::TypeError {
                    operation: "is",
                    field: col.clone(),
                }
                .into()),
                Err(_) => Err(EngineError::FieldNotFound(col.clone()).into()),
            }
        }),
    }
}

pub fn execute_query(
    query: &Query,
    dataset: &Vec<Course>,
) -> Result<Vec<HashMap<String, Value>>, Box<dyn Error>> {
    let filter = query
        .r#where
        .as_ref()
        .map(|filter| parse_filter(&filter))
        .unwrap_or(Box::new(|_| Ok(true)));

    let mut filter_result = vec![];
    for course in dataset.iter() {
        if filter(course)? {
            filter_result.push(course.clone());
            if filter_result.len() > 5000 {
                return Err(ResultToLargeError.into())
            }
        }
    }

    let mut columns_result = vec![];
    for course in filter_result.drain(..) {
        let mut new = HashMap::new();
        for column in query.options.columns.iter() {
            new.insert(column.clone(), course.get(column)?);
        }
        columns_result.push(new);
    }

    Ok(columns_result)
}

#[cfg(test)]
#[path = "query_test.rs"]
mod query_test;

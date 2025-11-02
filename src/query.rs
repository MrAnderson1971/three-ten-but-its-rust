use crate::dataset::Value::{Num, Str};
use crate::dataset::{Course, EPSILON, Value};
use crate::errors::EngineError;
use crate::errors::EngineError::ResultToLargeError;
use ordered_float::OrderedFloat;
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;

type FilterFunc<'a> = Box<dyn Fn(&Course) -> Result<bool, Box<dyn Error>> + 'a>;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
pub struct Query {
    pub r#where: Option<Filter>,
    pub options: Options,
    pub transformations: Option<Transformations>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
pub struct Transformations {
    pub group: Vec<String>,
    pub apply: Vec<HashMap<String, String>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
pub struct Options {
    pub columns: Vec<String>,
    #[serde(flatten)]
    pub order: Option<Order>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Order {
    ONE(String),
    MANY { dir: String, keys: Vec<String> },
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
        lt: HashMap<String, OrderedFloat<f32>>,
    },
    GT {
        #[serde(rename = "GT")]
        gt: HashMap<String, OrderedFloat<f32>>,
    },
    EQ {
        #[serde(rename = "EQ")]
        eq: HashMap<String, OrderedFloat<f32>>,
    },
    IS {
        #[serde(rename = "IS")]
        is: HashMap<String, String>,
    },
}

fn parse_and(and: &'_ Vec<Filter>) -> FilterFunc<'_> {
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

fn parse_or(or: &'_ Vec<Filter>) -> FilterFunc<'_> {
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
    args: &HashMap<String, OrderedFloat<f32>>,
    course: &Course,
    predicate: impl FnOnce(OrderedFloat<f32>, OrderedFloat<f32>) -> bool,
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

fn parse_filter(filter: &'_ Filter) -> FilterFunc<'_> {
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
            parse_comparison(&eq, &course, |a, b| (a - b).abs() < EPSILON, "eq")
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

macro_rules! sort {
    ($key:ident, $a:ident, $b:ident) => {
        $a.get($key)
            .unwrap()
            .partial_cmp($b.get($key).unwrap())
            .unwrap()
    };
}

fn handle_order(
    order: &Order,
    columns_result: &mut Vec<BTreeMap<String, Value>>,
) -> Result<(), Box<dyn Error>> {
    match order {
        Order::ONE(order) => {
            let all_have_column = columns_result.iter().all(|row| row.contains_key(order));

            if !all_have_column {
                return Err(format!("Order column '{}' not found in results", order).into());
            }
            columns_result.sort_by(|a, b| sort!(order, a, b));
        }
        Order::MANY { dir, keys } => {
            let reverse = match dir.as_str() {
                "UP" => false,
                "DOWN" => true,
                _ => {
                    return Err(format!("Invalid ordering {}, expected UP or DOWN", dir).into());
                }
            };
            for key in keys.iter() {
                for row in columns_result.iter() {
                    if !row.contains_key(key) {
                        return Err(format!("Key {} not found", key).into());
                    }
                }
            }

            let sort_funcs: Vec<_> = keys
                .iter()
                .map(|key| {
                    Box::new(|a: &BTreeMap<String, Value>, b: &BTreeMap<String, Value>| {
                        if reverse {
                            sort!(key, b, a)
                        } else {
                            sort!(key, a, b)
                        }
                    })
                })
                .collect();

            columns_result.sort_by(|a, b| {
                for sort_func in sort_funcs.iter() {
                    match sort_func(a, b) {
                        Ordering::Equal => continue,
                        other => return other,
                    }
                }
                Ordering::Equal
            });
        }
    }
    Ok(())
}

pub fn execute_query(
    query: &Query,
    dataset: &Vec<Course>,
) -> Result<Vec<BTreeMap<String, Value>>, Box<dyn Error>> {
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
                return Err(ResultToLargeError.into());
            }
        }
    }

    let mut columns_result = vec![];
    for course in filter_result.drain(..) {
        let mut new = BTreeMap::new();
        for column in query.options.columns.iter() {
            new.insert(column.clone(), course.get(column)?);
        }
        columns_result.push(new);
    }

    if let Some(order) = &query.options.order {
        handle_order(order, &mut columns_result)?;
    }

    Ok(columns_result)
}

#[cfg(test)]
#[path = "query_test.rs"]
mod query_test;

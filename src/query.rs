use crate::dataset::Dataset;
use crate::dataset::Value::{Num, Str};
use crate::dataset::{EPSILON, Value};
use crate::types::KVPair;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::error::Error;

type FilterFunc<'a, D> = Box<dyn Fn(&D) -> Result<bool, Box<dyn Error>> + 'a>;

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
    pub apply: Vec<KVPair<KVPair<String>>>,
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
        lt: KVPair<OrderedFloat<f32>>,
    },
    GT {
        #[serde(rename = "GT")]
        gt: KVPair<OrderedFloat<f32>>,
    },
    EQ {
        #[serde(rename = "EQ")]
        eq: KVPair<OrderedFloat<f32>>,
    },
    IS {
        #[serde(rename = "IS")]
        is: KVPair<String>,
    },
}

fn parse_and<'a, D: Dataset + 'a>(and: &'a Vec<Filter>) -> FilterFunc<'a, D> {
    let filters: Vec<_> = and.iter().map(|filter| parse_filter(filter)).collect();
    Box::new(move |course| {
        Ok(filters
            .iter()
            .map(|filter| filter(course))
            .collect::<Result<Vec<bool>, _>>()?
            .into_iter()
            .all(|b| b))
    })
}

fn parse_or<'a, D: Dataset + 'a>(or: &'a Vec<Filter>) -> FilterFunc<'a, D> {
    let filters: Vec<_> = or.iter().map(|filter| parse_filter(filter)).collect();
    Box::new(move |course| {
        Ok(filters
            .iter()
            .map(|filter| filter(course))
            .collect::<Result<Vec<bool>, _>>()?
            .into_iter()
            .any(|b| b))
    })
}

fn parse_comparison(
    args: &KVPair<OrderedFloat<f32>>,
    course: &impl Dataset,
    predicate: impl FnOnce(OrderedFloat<f32>, OrderedFloat<f32>) -> bool,
    op: &'static str,
) -> Result<bool, Box<dyn Error>> {
    let KVPair {
        key: col,
        value: val,
    } = args;
    match course.get(col) {
        Ok(Num(i)) => Ok(predicate(i, *val)),
        Ok(_) => Err(format!("Operation {} is not valid for {}", op, col).into()),
        Err(_) => Err(format!("Field {} does not exist", col).into()),
    }
}

fn parse_filter<'a, D: Dataset + 'a>(filter: &'a Filter) -> FilterFunc<'a, D> {
    match filter {
        Filter::AND { and } => parse_and::<'a>(and),
        Filter::OR { or } => parse_or::<'a>(or),
        Filter::NOT { not } => Box::new(|course| Ok(!parse_filter(not)(course)?)),
        Filter::LT { lt } => {
            Box::new(move |course| parse_comparison(&lt, course, |a, b| a < b, "lt"))
        }
        Filter::GT { gt } => {
            Box::new(move |course| parse_comparison(&gt, course, |a, b| a > b, "gt"))
        }
        Filter::EQ { eq } => Box::new(move |course| {
            parse_comparison(&eq, course, |a, b| (a - b).abs() < EPSILON, "eq")
        }),
        Filter::IS { is } => Box::new(move |course| {
            let KVPair {
                key: col,
                value: val,
            } = is;
            match course.get(col) {
                Ok(Str(s)) => Ok(s == *val),
                Ok(_) => Err(format!(r#"Operation "is" is not valid for {}"#, col).into()),
                Err(_) => Err(format!("Field {} does not exist", col).into()),
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

fn compute_aggregate(
    mut init: OrderedFloat<f32>,
    func: impl Fn(OrderedFloat<f32>, OrderedFloat<f32>) -> OrderedFloat<f32>,
    op: &'static str,
    column: &String,
    data: &Vec<&BTreeMap<String, Value>>,
) -> Result<OrderedFloat<f32>, Box<dyn Error>> {
    for item in data {
        let Num(num) = item
            .get(column)
            .ok_or_else(|| format!("Column {} does not exist", column))?
        else {
            return Err(format!("Invalid operation {} on column {}", op, column).into());
        };
        init = func(init, *num)
    }
    Ok(init)
}

fn handle_transformations(
    transformations: &Transformations,
    columns_result: &Vec<BTreeMap<String, Value>>,
) -> Result<Vec<BTreeMap<String, Value>>, Box<dyn Error>> {
    for column in columns_result.iter() {
        for transformation in transformations.group.iter() {
            if !column.contains_key(transformation) {
                return Err(format!("Unknown group {}", transformation).into());
            }
        }
    }
    let grouped = columns_result.iter().into_group_map_by(|course| {
        transformations
            .group
            .iter()
            .map(|group| (group.clone(), course.get(group).unwrap().clone()))
            .collect::<BTreeMap<_, _>>()
    });

    // Apply aggregates to each group
    grouped
        .into_iter()
        .map(|(group_keys, items)| {
            let n = OrderedFloat(items.len() as f32);

            // Compute all aggregates and add to group result
            transformations
                .apply
                .iter()
                .try_fold(group_keys, |mut acc, aggregate| {
                    let KVPair {
                        key: apply_key,
                        value: inner,
                    } = aggregate;
                    let KVPair {
                        key: function,
                        value: column,
                    } = inner;

                    let result = match function.as_str() {
                        "COUNT" => Ok(Num(n)),
                        "AVG" => compute_aggregate(
                            OrderedFloat(0.0),
                            |acc, val| acc + val / n,
                            "avg",
                            column,
                            &items,
                        )
                        .map(|v| Num(v)),
                        "SUM" => compute_aggregate(
                            OrderedFloat(0.0),
                            |acc, val| acc + val,
                            "sum",
                            column,
                            &items,
                        )
                        .map(|v| Num(v)),
                        "MAX" => compute_aggregate(
                            OrderedFloat(f32::NEG_INFINITY),
                            |acc, val| std::cmp::max(acc, val),
                            "max",
                            column,
                            &items,
                        )
                        .map(|v| Num(v)),
                        "MIN" => compute_aggregate(
                            OrderedFloat(f32::INFINITY),
                            |acc, val| std::cmp::min(acc, val),
                            "min",
                            column,
                            &items,
                        )
                        .map(|v| Num(v)),
                        _ => Err(format!("Unknown function {}", function).into()),
                    }?;

                    acc.insert(apply_key.clone(), result);
                    Ok(acc)
                })
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()
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

pub fn execute_query<D: Dataset>(
    query: &Query,
    dataset: &Vec<D>,
) -> Result<Vec<BTreeMap<String, Value>>, Box<dyn Error>> {
    let filter = query
        .r#where
        .as_ref()
        .map(|filter| parse_filter(&filter))
        .unwrap_or(Box::new(|_| Ok(true)));

    let filter_result = dataset
        .iter()
        .filter_map(|item| match filter(item) {
            Ok(true) => Some(Ok(item.clone())),
            Ok(false) => None,
            Err(e) => Some(Err(e)),
        })
        .take(5001)
        .collect::<Result<Vec<_>, _>>()
        .and_then(|collected| {
            if collected.len() > 5000 {
                Err("Result too large".into())
            } else {
                Ok(collected)
            }
        })?;

    let mut columns_result = filter_result
        .into_iter()
        .map(|course| {
            query
                .options
                .columns
                .iter()
                .map(|column| course.get(column).map(|value| (column.clone(), value)))
                .collect::<Result<BTreeMap<String, Value>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;

    if let Some(transform) = &query.transformations {
        columns_result = handle_transformations(transform, &columns_result)?;
    }

    if let Some(order) = &query.options.order {
        handle_order(order, &mut columns_result)?;
    }

    Ok(columns_result)
}

#[cfg(test)]
#[path = "query_test.rs"]
mod query_test;

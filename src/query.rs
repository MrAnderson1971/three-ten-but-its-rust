use crate::dataset::Dataset;
use crate::dataset::Value::{Num, Str};
use crate::dataset::{EPSILON, Value};
use crate::types::KVPair;
use anyhow::anyhow;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use regex::Regex;
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::sync::{LazyLock, Mutex};

type FilterFunc<'a, D> = Box<dyn Fn(&D) -> anyhow::Result<bool> + 'a>;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
pub struct Query {
    pub r#where: Filter,
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
    EMPTY {},
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
) -> anyhow::Result<bool> {
    let KVPair {
        key: col,
        value: val,
    } = args;
    match course.get(col) {
        Ok(Num(i)) => Ok(predicate(i, *val)),
        Ok(_) => Err(anyhow!("Operation {} is not valid for {}", op, col)),
        Err(_) => Err(anyhow!("Field {} does not exist", col)),
    }
}

static REGEX_CACHE: LazyLock<
    Mutex<HashMap<String, Result<Regex, regex::Error>>>,
    fn() -> Mutex<HashMap<String, Result<Regex, regex::Error>>>,
> = LazyLock::new(|| Mutex::new(HashMap::<String, Result<Regex, regex::Error>>::new()));

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
                Ok(Str(s)) => {
                    let mut cache = REGEX_CACHE.lock().unwrap();
                    let regex = cache
                        .entry(val.clone())
                        .or_insert_with(|| Regex::new(&format!("^{}$", val)))
                        .clone()?;

                    Ok(regex.is_match(&s))
                }
                Ok(_) => Err(anyhow!(r#"Operation "is" is not valid for {}"#, col)),
                Err(_) => Err(anyhow!("Field {} does not exist", col)),
            }
        }),
        Filter::EMPTY {} => Box::new(|_| Ok(true)),
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
) -> anyhow::Result<OrderedFloat<f32>> {
    for item in data {
        let Num(num) = item
            .get(column)
            .ok_or_else(|| anyhow!("Column {} does not exist", column))?
        else {
            return Err(anyhow!("Invalid operation {} on column {}", op, column));
        };
        init = func(init, *num)
    }
    Ok(init)
}

fn handle_transformations(
    transformations: &Transformations,
    columns_result: &Vec<BTreeMap<String, Value>>,
) -> anyhow::Result<Vec<BTreeMap<String, Value>>> {
    for column in columns_result.iter() {
        for transformation in transformations.group.iter() {
            if !column.contains_key(transformation) {
                return Err(anyhow!("Unknown group {}", transformation));
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
                        "COUNT" => Ok(n),
                        "AVG" => compute_aggregate(
                            OrderedFloat(0.0),
                            |acc, val| acc + val / n,
                            "avg",
                            column,
                            &items,
                        ),
                        "SUM" => compute_aggregate(
                            OrderedFloat(0.0),
                            |acc, val| acc + val,
                            "sum",
                            column,
                            &items,
                        ),
                        "MAX" => compute_aggregate(
                            OrderedFloat(f32::NEG_INFINITY),
                            |acc, val| std::cmp::max(acc, val),
                            "max",
                            column,
                            &items,
                        ),
                        "MIN" => compute_aggregate(
                            OrderedFloat(f32::INFINITY),
                            |acc, val| std::cmp::min(acc, val),
                            "min",
                            column,
                            &items,
                        ),
                        _ => Err(anyhow!("Unknown function {}", function)),
                    }
                    .map(|result| Num(OrderedFloat::from((result * 100.0).round() / 100.0)))?;

                    acc.insert(apply_key.clone(), result);
                    Ok(acc)
                })
        })
        .collect::<anyhow::Result<Vec<_>>>()
}

fn handle_order(
    order: &Order,
    columns_result: &mut Vec<BTreeMap<String, Value>>,
) -> anyhow::Result<()> {
    match order {
        Order::ONE(order) => {
            let all_have_column = columns_result.iter().all(|row| row.contains_key(order));

            if !all_have_column {
                return Err(anyhow!("Order column '{}' not found in results", order));
            }
            columns_result.sort_by(|a, b| sort!(order, a, b));
        }
        Order::MANY { dir, keys } => {
            let reverse = match dir.as_str() {
                "UP" => false,
                "DOWN" => true,
                _ => {
                    return Err(anyhow!("Invalid ordering {}, expected UP or DOWN", dir));
                }
            };
            for key in keys.iter() {
                for row in columns_result.iter() {
                    if !row.contains_key(key) {
                        return Err(anyhow!("Key {} not found", key));
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
) -> anyhow::Result<Vec<BTreeMap<String, Value>>> {
    let filter = parse_filter(&query.r#where);

    let mut filter_result = dataset
        .into_iter()
        .filter_map(|item| -> Option<anyhow::Result<_>> {
            match filter(item) {
                Ok(true) => Some(Ok(item)),
                Ok(false) => None,
                Err(e) => Some(Err(e)),
            }
        })
        .take(5001) // one more to detect overflow
        .collect::<anyhow::Result<Vec<_>>>()
        .and_then(|collected| {
            if collected.len() > 5000 {
                Err(anyhow!("Result too large"))
            } else {
                // turn from Vec<Dataset> into Vec<BTreeMap<String, Value>>
                Ok(collected
                    .into_iter()
                    .map(|item| {
                        item.get_all()
                            .iter()
                            .map(|key| (key.to_string(), item.get(key).unwrap()))
                            .collect::<BTreeMap<_, _>>()
                    })
                    .collect::<Vec<_>>())
            }
        })?;

    if let Some(transform) = &query.transformations {
        filter_result = handle_transformations(transform, &filter_result)?;
    }

    let mut columns_result = filter_result
        .into_iter()
        .map(|course| -> anyhow::Result<BTreeMap<String, Value>> {
            let mut map = BTreeMap::new();
            for column in &query.options.columns {
                map.insert(
                    column.clone(),
                    course
                        .get(column)
                        .ok_or_else(|| anyhow!("Unknown column {}", column))?
                        .clone(),
                );
            }
            Ok(map)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    if let Some(order) = &query.options.order {
        handle_order(order, &mut columns_result)?;
    }

    Ok(columns_result)
}

#[cfg(test)]
#[path = "query_test.rs"]
mod query_test;

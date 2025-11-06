use ordered_float::OrderedFloat;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[serde(untagged)]
pub enum Value {
    Num(OrderedFloat<f32>),
    Str(String),
}

pub trait Dataset {
    fn get(&self, field_name: &str) -> Result<Value, String>;
    fn get_all(&self) -> &'static [&'static str];
}

#[derive(Debug, PartialEq)]
pub struct KVPair<T> {
    pub key: String,
    pub value: T,
}

impl<'de, T> Deserialize<'de> for KVPair<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let map = HashMap::<String, T>::deserialize(deserializer)?;

        if map.len() != 1 {
            return Err(serde::de::Error::custom(format!(
                "Expected exactly 1 entry, got {}",
                map.len()
            )));
        }

        let (key, value) = map.into_iter().next().unwrap();
        Ok(KVPair { key, value })
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum QueryResult {
    OK {
        result: Vec<BTreeMap<String, Value>>,
    },
    ERROR {
        error: String,
    },
}

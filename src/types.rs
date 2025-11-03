use serde::{Deserialize, Deserializer};
use std::collections::HashMap;

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

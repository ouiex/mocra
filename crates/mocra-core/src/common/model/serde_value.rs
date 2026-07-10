use serde::{Deserialize, Deserializer, Serializer};
use serde_json::Value;

pub mod option_value {
    use super::*;

    pub fn serialize<S>(value: &Option<Value>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize Value as String for cross-format compatibility
        match value {
            Some(v) => {
                 let s = serde_json::to_string(v).map_err(serde::ser::Error::custom)?;
                 serializer.serialize_some(&s)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: Option<String> = Option::deserialize(deserializer)?;
        match s {
            Some(str_val) => {
                let val = serde_json::from_str(&str_val).map_err(serde::de::Error::custom)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }
}

pub mod value {
    use super::*;

    pub fn serialize<S>(value: &Value, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = serde_json::to_string(value).map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let val = serde_json::from_str(&s).map_err(serde::de::Error::custom)?;
        Ok(val)
    }
}

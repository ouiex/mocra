use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
pub enum Priority {
    #[serde(rename = "low")]
    Low = 1,
    #[default]
    #[serde(rename = "normal")]
    Normal = 5,
    #[serde(rename = "high")]
    High = 10,
}

impl Priority {
    pub fn suffix(&self) -> &'static str {
        match self {
            Priority::Low => "low",
            Priority::Normal => "normal",
            Priority::High => "high",
        }
    }
}

pub trait Prioritizable {
    fn get_priority(&self) -> Priority;
}

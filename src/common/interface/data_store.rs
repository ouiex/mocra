use crate::common::model::data::DataEvent;
use std::fmt::Debug;

pub trait StoreTrait: Clone + Debug {
    fn build(&self) -> DataEvent;
}

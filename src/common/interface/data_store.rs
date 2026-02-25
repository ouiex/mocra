use crate::common::model::data::Data;
use std::fmt::Debug;

pub trait StoreTrait: Clone + Debug {
    fn build(&self) -> Data;
}

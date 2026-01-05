use std::fmt::Debug;
use crate::model::data::Data;

pub trait StoreTrait:Clone+Debug {
    fn build(&self) -> Data;
}
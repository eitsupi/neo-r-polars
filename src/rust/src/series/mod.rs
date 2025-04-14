mod aggregation;
mod arithmetic;
mod construction;
mod export;
mod general;
mod import;
use crate::prelude::*;
use savvy::{EnvironmentSexp, savvy};

#[savvy]
#[repr(transparent)]
#[derive(Clone)]
pub struct PlRSeries {
    pub series: Series,
}

impl From<Series> for PlRSeries {
    fn from(series: Series) -> Self {
        Self { series }
    }
}

impl PlRSeries {
    pub(crate) fn new(series: Series) -> Self {
        Self { series }
    }
}

impl From<EnvironmentSexp> for &PlRSeries {
    fn from(env: EnvironmentSexp) -> Self {
        let ptr = env.get(".ptr").unwrap().unwrap();
        <&PlRSeries>::try_from(ptr).unwrap()
    }
}

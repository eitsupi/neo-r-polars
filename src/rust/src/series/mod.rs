mod arithmetic;
mod construction;
mod export;
mod general;
use crate::prelude::*;
use savvy::{savvy, EnvironmentSexp};

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

impl TryFrom<EnvironmentSexp> for &PlRSeries {
    type Error = String;

    fn try_from(env: EnvironmentSexp) -> Result<Self, String> {
        let ptr = env
            .get(".ptr")
            .expect("Failed to get `.ptr` from the object")
            .ok_or("The object is not a valid polars series")?;
        <&PlRSeries>::try_from(ptr).map_err(|e| e.to_string())
    }
}

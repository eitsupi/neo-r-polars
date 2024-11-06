mod general;
mod serde;

use crate::prelude::*;
use savvy::{savvy, EnvironmentSexp};

#[savvy]
#[repr(transparent)]
#[derive(Clone)]
pub struct PlRLazyFrame {
    pub ldf: LazyFrame,
}

impl From<LazyFrame> for PlRLazyFrame {
    fn from(ldf: LazyFrame) -> Self {
        PlRLazyFrame { ldf }
    }
}

impl TryFrom<EnvironmentSexp> for &PlRLazyFrame {
    type Error = String;

    fn try_from(env: EnvironmentSexp) -> Result<Self, String> {
        let ptr = env
            .get(".ptr")
            .expect("Failed to get `.ptr` from the object")
            .ok_or("The object is not a valid polars data frame")?;
        <&PlRLazyFrame>::try_from(ptr).map_err(|e| e.to_string())
    }
}

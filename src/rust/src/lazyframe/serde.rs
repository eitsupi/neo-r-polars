use crate::{prelude::*, PlRLazyFrame, RPolarsErr};
use savvy::{savvy, Result};

#[savvy]
fn deserialize_lf(json: &str) -> Result<PlRLazyFrame> {
    let lp = serde_json::from_str::<DslPlan>(json).map_err(|_| {
        let msg = "could not deserialize input into a LazyFrame";
        RPolarsErr::Other(msg.to_string())
    })?;
    let out = LazyFrame::from(lp);
    Ok(<PlRLazyFrame>::from(out))
}

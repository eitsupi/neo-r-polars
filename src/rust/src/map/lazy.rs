use crate::{PlRDataType, PlRExpr};
use savvy::{FunctionSexp, Result};

pub fn map_single(
    rexpr: &PlRExpr,
    lambda: FunctionSexp,
    output_type: Option<PlRDataType>,
    agg_list: bool,
    // TODO: support these options
    // is_elementwise: bool,
    // returns_scalar: bool,
) -> Result<PlRExpr> {
    let output_type = output_type.map(|t| t.dt);
}

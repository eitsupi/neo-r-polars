use crate::{
    prelude::*,
    r_threads::ThreadCom,
    r_udf::{RUdf, RUdfSignature, CONFIG},
    PlRDataType, PlRExpr,
};
use savvy::{FunctionSexp, Result};

pub fn map_single(
    rexpr: &PlRExpr,
    lambda: FunctionSexp,
    return_dtype: Option<&PlRDataType>,
    agg_list: bool,
    // TODO: use these args
    _is_elementwise: bool,
    _returns_scalar: bool,
) -> Result<PlRExpr> {
    let return_dtype = return_dtype.map(|t| t.dt.clone());
    let lambda = RUdf::new(lambda);
    let func = move |col: Column| {
        let thread_com = ThreadCom::try_from_global(&CONFIG)
            .expect("polars was thread could not initiate ThreadCommunication to R");
        thread_com.send(RUdfSignature::SeriesToSeries(
            lambda.clone(),
            col.as_materialized_series().clone(),
        ));
        let s: Series = thread_com.recv().try_into().unwrap();
        Ok(Some(s.into_column()))
    };

    let output_map = GetOutput::map_field(move |field| match &return_dtype {
        Some(dt) => Ok(Field::new(field.name().clone(), dt.clone())),
        None => Ok(field.clone()),
    });

    if agg_list {
        Ok(rexpr.inner.clone().map_list(func, output_map).into())
    } else {
        Ok(rexpr.inner.clone().map(func, output_map).into())
    }
}

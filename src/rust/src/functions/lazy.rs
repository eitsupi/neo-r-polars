use crate::{prelude::*, PlRExpr, PlRSeries, RPolarsErr};
use polars::lazy::dsl;
use savvy::{savvy, ListSexp, RawSexp, Result, StringSexp};

#[savvy]
pub fn as_struct(exprs: ListSexp) -> Result<PlRExpr> {
    let exprs = <Wrap<Vec<Expr>>>::try_from(exprs)?.0;
    if exprs.is_empty() {
        return Err(savvy::Error::from(
            "expected at least 1 expression in `as_struct`",
        ));
    }
    Ok(dsl::as_struct(exprs).into())
}

#[savvy]
pub fn field(names: StringSexp) -> Result<PlRExpr> {
    Ok(dsl::Expr::Field(names.iter().map(|name| name.into()).collect()).into())
}

#[savvy]
pub fn col(name: &str) -> Result<PlRExpr> {
    Ok(dsl::col(name).into())
}

#[savvy]
pub fn cols(names: StringSexp) -> Result<PlRExpr> {
    let names = names.iter().collect::<Vec<_>>();
    Ok(dsl::cols(names).into())
}

#[savvy]
pub fn dtype_cols(dtypes: ListSexp) -> Result<PlRExpr> {
    let dtypes = <Wrap<Vec<DataType>>>::try_from(dtypes)?.0;
    Ok(dsl::dtype_cols(&dtypes).into())
}

#[savvy]
pub fn lit_from_bool(value: bool) -> Result<PlRExpr> {
    Ok(dsl::lit(value).into())
}

#[savvy]
pub fn lit_from_i32(value: i32) -> Result<PlRExpr> {
    Ok(dsl::lit(value).into())
}

#[savvy]
pub fn lit_from_f64(value: f64) -> Result<PlRExpr> {
    Ok(dsl::lit(value).into())
}

#[savvy]
pub fn lit_from_str(value: &str) -> Result<PlRExpr> {
    Ok(dsl::lit(value).into())
}

#[savvy]
pub fn lit_from_raw(value: RawSexp) -> Result<PlRExpr> {
    Ok(dsl::lit(value.as_slice()).into())
}

#[savvy]
pub fn lit_null() -> Result<PlRExpr> {
    Ok(dsl::lit(Null {}).into())
}

#[savvy]
pub fn lit_from_series(value: &PlRSeries) -> Result<PlRExpr> {
    Ok(dsl::lit(value.series.clone()).into())
}

#[savvy]
pub fn lit_from_series_first(value: &PlRSeries) -> Result<PlRExpr> {
    let s = value.series.clone();
    let av = s
        .get(0)
        .map_err(RPolarsErr::from)?
        .into_static();
    Ok(dsl::lit(Scalar::new(s.dtype().clone(), av)).into())
}

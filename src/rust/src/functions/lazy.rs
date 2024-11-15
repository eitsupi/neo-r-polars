use crate::{prelude::*, PlRDataFrame, PlRExpr, PlRLazyFrame, PlRSeries, RPolarsErr};
use polars::functions;
use polars::lazy::dsl;
use savvy::{savvy, ListSexp, NumericSexp, RawSexp, Result, StringSexp};

macro_rules! set_unwrapped_or_0 {
    ($($var:ident),+ $(,)?) => {
        $(let $var = $var.map(|e| e.inner).unwrap_or(dsl::lit(0));)+
    };
}

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
pub fn datetime(
    year: PlRExpr,
    month: PlRExpr,
    day: PlRExpr,
    time_unit: &str,
    ambiguous: PlRExpr,
    hour: Option<PlRExpr>,
    minute: Option<PlRExpr>,
    second: Option<PlRExpr>,
    microsecond: Option<PlRExpr>,
    time_zone: Option<&str>,
) -> Result<PlRExpr> {
    let year = year.inner;
    let month = month.inner;
    let day = day.inner;
    set_unwrapped_or_0!(hour, minute, second, microsecond);
    let ambiguous = ambiguous.inner;
    let time_unit = <Wrap<TimeUnit>>::try_from(time_unit)?.0;
    let time_zone = time_zone.map(|x| x.into());
    let args = DatetimeArgs {
        year,
        month,
        day,
        hour,
        minute,
        second,
        microsecond,
        time_unit,
        time_zone,
        ambiguous,
    };
    Ok(dsl::datetime(args).into())
}

#[savvy]
pub fn duration(
    time_unit: &str,
    weeks: Option<PlRExpr>,
    days: Option<PlRExpr>,
    hours: Option<PlRExpr>,
    minutes: Option<PlRExpr>,
    seconds: Option<PlRExpr>,
    milliseconds: Option<PlRExpr>,
    microseconds: Option<PlRExpr>,
    nanoseconds: Option<PlRExpr>,
) -> Result<PlRExpr> {
    set_unwrapped_or_0!(
        weeks,
        days,
        hours,
        minutes,
        seconds,
        milliseconds,
        microseconds,
        nanoseconds,
    );
    let time_unit = <Wrap<TimeUnit>>::try_from(time_unit)?.0;
    let args = DurationArgs {
        weeks,
        days,
        hours,
        minutes,
        seconds,
        milliseconds,
        microseconds,
        nanoseconds,
        time_unit,
    };
    Ok(dsl::duration(args).into())
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
pub fn index_cols(indices: NumericSexp) -> Result<PlRExpr> {
    let n = <Wrap<Vec<i64>>>::try_from(indices)?.0;
    let out = if n.len() == 1 {
        dsl::nth(n[0])
    } else {
        dsl::index_cols(n)
    }
    .into();
    Ok(out)
}

#[savvy]
pub fn first() -> Result<PlRExpr> {
    Ok(dsl::first().into())
}

#[savvy]
pub fn last() -> Result<PlRExpr> {
    Ok(dsl::last().into())
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
    let av = s.get(0).map_err(RPolarsErr::from)?.into_static();
    Ok(dsl::lit(Scalar::new(s.dtype().clone(), av)).into())
}

#[savvy]
pub fn concat_list(s: ListSexp) -> Result<PlRExpr> {
    let s = <Wrap<Vec<Expr>>>::from(s).0;
    let expr = dsl::concat_list(s).map_err(RPolarsErr::from)?;
    Ok(expr.into())
}

#[savvy]
pub fn concat_df_diagonal(dfs: ListSexp) -> Result<PlRDataFrame> {
    let dfs = <Wrap<Vec<DataFrame>>>::try_from(dfs)?.0;

    let df = functions::concat_df_diagonal(&dfs).map_err(RPolarsErr::from)?;
    Ok(df.into())
}

#[savvy]
pub fn concat_df_horizontal(dfs: ListSexp) -> Result<PlRDataFrame> {
    let dfs = <Wrap<Vec<DataFrame>>>::try_from(dfs)?.0;

    let df = functions::concat_df_horizontal(&dfs, true).map_err(RPolarsErr::from)?;
    Ok(df.into())
}

#[savvy]
pub fn concat_lf(
    lfs: ListSexp,
    rechunk: bool,
    parallel: bool,
    to_supertypes: bool,
) -> Result<PlRLazyFrame> {
    let lfs = <Wrap<Vec<LazyFrame>>>::try_from(lfs)?.0;

    let lf = dsl::concat(
        lfs,
        UnionArgs {
            rechunk,
            parallel,
            to_supertypes,
            ..Default::default()
        },
    )
    .map_err(RPolarsErr::from)?;
    Ok(lf.into())
}

#[savvy]
pub fn concat_lf_horizontal(lfs: ListSexp, parallel: bool) -> Result<PlRLazyFrame> {
    let lfs = <Wrap<Vec<LazyFrame>>>::try_from(lfs)?.0;

    let args = UnionArgs {
        rechunk: false, // No need to rechunk with horizontal concatenation
        parallel,
        to_supertypes: false,
        ..Default::default()
    };
    let lf = dsl::functions::concat_lf_horizontal(lfs, args).map_err(RPolarsErr::from)?;
    Ok(lf.into())
}

#[savvy]
pub fn concat_lf_diagonal(
    lfs: ListSexp,
    rechunk: bool,
    parallel: bool,
    to_supertypes: bool,
) -> Result<PlRLazyFrame> {
    let lfs = <Wrap<Vec<LazyFrame>>>::try_from(lfs)?.0;

    let lf = dsl::functions::concat_lf_diagonal(
        lfs,
        UnionArgs {
            rechunk,
            parallel,
            to_supertypes,
            ..Default::default()
        },
    )
    .map_err(RPolarsErr::from)?;
    Ok(lf.into())
}

#[savvy]
pub fn concat_expr(e: ListSexp, rechunk: bool) -> Result<PlRExpr> {
    let e = <Wrap<Vec<Expr>>>::try_from(e)?.0;
    let e = dsl::functions::concat_expr(e, rechunk).map_err(RPolarsErr::from)?;
    Ok(e.into())
}

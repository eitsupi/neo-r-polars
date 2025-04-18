use crate::{PlRSeries, RPolarsErr, Wrap};
use polars::datatypes::DataType;
use polars::prelude::{QuantileMethod, Scalar};
use polars_core::prelude::AnyValue;
use savvy::{NumericScalar, Result, Sexp, savvy};

fn scalar_to_r(x: Scalar) -> Result<Sexp> {
    match x.as_any_value() {
        AnyValue::Int8(x) => return (x as i32).try_into(),
        AnyValue::Int16(x) => return (x as i32).try_into(),
        AnyValue::Int32(x) => return x.try_into(),
        AnyValue::Int64(x) => return (x as f64).try_into(),
        AnyValue::Float32(x) => return (x as f64).try_into(),
        AnyValue::Float64(x) => return x.try_into(),
        AnyValue::Boolean(x) => return x.try_into(),
        AnyValue::String(x) => return x.try_into(),
        AnyValue::StringOwned(x) => return x.as_str().try_into(),
        _ => unreachable!("Not implemented for dtype {}", x.as_any_value().dtype()),
    }
}

#[savvy]
impl PlRSeries {
    fn min(&self) -> Result<Sexp> {
        let scal = self.series.min_reduce().map_err(RPolarsErr::from)?;
        scalar_to_r(scal)
    }

    fn max(&self) -> Result<Sexp> {
        let scal = self.series.max_reduce().map_err(RPolarsErr::from)?;
        scalar_to_r(scal)
    }

    fn mean(&self) -> Result<Sexp> {
        let scal = match self.series.dtype() {
            DataType::Boolean => self.series.cast(&DataType::UInt8).unwrap().mean_reduce(),
            // For non-numeric output types we require mean_reduce.
            dt if dt.is_temporal() => self.series.mean_reduce(),
            _ => return self.series.mean().unwrap().try_into(),
        };

        scalar_to_r(scal)
    }

    fn median(&self) -> Result<Sexp> {
        let scal = match self.series.dtype() {
            DataType::Boolean => self.series.cast(&DataType::UInt8).unwrap().median_reduce(),
            // For non-numeric output types we require median_reduce.
            dt if dt.is_temporal() => self.series.median_reduce(),
            _ => return self.series.median().unwrap().try_into(),
        };

        scalar_to_r(scal.map_err(RPolarsErr::from)?)
    }

    fn product(&self) -> Result<Sexp> {
        let scal = self.series.product().map_err(RPolarsErr::from)?;
        scalar_to_r(scal)
    }

    fn quantile(&self, quantile: f64, interpolation: &str) -> Result<Sexp> {
        let interpolation = <Wrap<QuantileMethod>>::try_from(interpolation)?.0;
        let scal = self
            .series
            .quantile_reduce(quantile, interpolation)
            .map_err(RPolarsErr::from)?;
        scalar_to_r(scal)
    }

    fn std(&self, ddof: NumericScalar) -> Result<Sexp> {
        let ddof = <Wrap<u8>>::try_from(ddof)?.0;
        let scal = self.series.std_reduce(ddof).map_err(RPolarsErr::from)?;
        scalar_to_r(scal)
    }

    fn var(&self, ddof: NumericScalar) -> Result<Sexp> {
        let ddof = <Wrap<u8>>::try_from(ddof)?.0;
        let scal = self.series.var_reduce(ddof).map_err(RPolarsErr::from)?;
        scalar_to_r(scal)
    }

    fn sum(&self) -> Result<Sexp> {
        let scal = self.series.sum_reduce().map_err(RPolarsErr::from)?;
        scalar_to_r(scal)
    }

    fn first(&self) -> Result<Sexp> {
        let scal = self.series.first();
        scalar_to_r(scal)
    }

    fn last(&self) -> Result<Sexp> {
        let scal = self.series.last();
        scalar_to_r(scal)
    }

    fn bitwise_and(&self) -> Result<Sexp> {
        let scal = self.series.and_reduce().map_err(RPolarsErr::from)?;
        scalar_to_r(scal)
    }

    fn bitwise_or(&self) -> Result<Sexp> {
        let scal = self.series.or_reduce().map_err(RPolarsErr::from)?;
        scalar_to_r(scal)
    }

    fn bitwise_xor(&self) -> Result<Sexp> {
        let scal = self.series.xor_reduce().map_err(RPolarsErr::from)?;
        scalar_to_r(scal)
    }
}

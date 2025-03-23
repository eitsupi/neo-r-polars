use crate::PlRDataType;
use savvy::{savvy, Result, Sexp};

#[savvy]
pub fn dtype_str_repr(dtype: &PlRDataType) -> Result<Sexp> {
    dtype.dt.clone().to_string().try_into()
}

// A module for converting functions defined on the R side.
// In the case of Python, it does not exist because there is a special feature in polars-plan.

use crate::{prelude::*, r_threads::ThreadCom, PlRSeries};
use savvy::{
    ffi::SEXP,
    protect::{insert_to_preserved_list, release_from_preserved_list},
    FunctionArgs, FunctionSexp, Sexp, TypedSexp,
};
use state::InitCell;
use std::sync::{Arc, Mutex, RwLock};

// TODO: comments
#[derive(Clone)]
struct UnsafeToken(SEXP);
unsafe impl Send for UnsafeToken {}
unsafe impl Sync for UnsafeToken {}

pub struct UnsafeFunctionSexp(pub FunctionSexp);
unsafe impl Send for UnsafeFunctionSexp {}
unsafe impl Sync for UnsafeFunctionSexp {}

#[derive(Clone)]
pub struct RUdf {
    pub function: Arc<Mutex<UnsafeFunctionSexp>>,
    token: UnsafeToken,
}

impl RUdf {
    pub fn new(function: FunctionSexp) -> Self {
        let token = insert_to_preserved_list(function.inner());
        Self {
            function: Arc::new(Mutex::new(UnsafeFunctionSexp(function))),
            token: UnsafeToken(token),
        }
    }
}

impl Drop for RUdf {
    fn drop(&mut self) {
        release_from_preserved_list(self.token.0);
    }
}

// define any possible signature of R lambdas
// Copied from https://github.com/pola-rs/r-polars/blob/9572aef7b3c067ffebe124e61d22279674c17871/src/rust/src/concurrent.rs
pub enum RUdfSignature {
    // function with input 1 Series mapped to output 1 Series
    SeriesToSeries(RUdf, Series),
}

#[derive(Debug)]
pub enum RUdfReturn {
    Series(Series),
}

type ThreadComStorage = InitCell<RwLock<Option<ThreadCom<RUdfSignature, RUdfReturn>>>>;
// TODO: more appropriate name
pub static CONFIG: ThreadComStorage = InitCell::new();

impl RUdfSignature {
    pub fn eval(self) -> Result<RUdfReturn, Box<dyn std::error::Error>> {
        match self {
            RUdfSignature::SeriesToSeries(r_udf, series) => {
                let r_udf = (*r_udf.function).lock().unwrap();
                let mut args = FunctionArgs::new();
                args.add("series", <PlRSeries>::from(series))?;
                let res = <Sexp>::from(r_udf.0.call(args)?);
                let s = match res.into_typed() {
                    TypedSexp::Environment(env) => <&PlRSeries>::from(env).series.clone(),
                    _ => {
                        return Err("Expected a Series".into());
                    }
                };
                Ok(RUdfReturn::Series(s))
            }
        }
    }
}

impl TryFrom<RUdfReturn> for Series {
    type Error = String;

    fn try_from(value: RUdfReturn) -> Result<Self, Self::Error> {
        match value {
            RUdfReturn::Series(s) => Ok(s),
        }
    }
}

// A module for converting functions defined on the R side.
// In the case of Python, it does not exist because there is a special feature in polars-plan.

use savvy::{
    ffi::SEXP,
    protect::{insert_to_preserved_list, release_from_preserved_list},
    FunctionSexp,
};
use std::sync::{Arc, Mutex};

// TODO: comments
struct UnsafeToken(SEXP);
unsafe impl Send for UnsafeToken {}
unsafe impl Sync for UnsafeToken {}
struct UnsafeFunctionSexp(FunctionSexp);
unsafe impl Send for UnsafeFunctionSexp {}
unsafe impl Sync for UnsafeFunctionSexp {}

pub struct RUdf {
    function: Arc<Mutex<UnsafeFunctionSexp>>,
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

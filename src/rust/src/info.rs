use savvy::{savvy, Result, Sexp};

#[savvy]
fn rust_polars_version() -> Result<Sexp> {
    polars::VERSION.try_into()
}

#[savvy]
fn thread_pool_size() -> Result<Sexp> {
    (polars_core::POOL.current_num_threads() as i32).try_into()
}

### Helper to detect whether an expectation runs in a test where data is in
### lazy mode.
is_in_lazy_test <- function() {
  nzchar(Sys.getenv("POLARS_IN_LAZY_TEST")) && Sys.getenv("POLARS_IN_LAZY_TEST") == "TRUE"
}

### Those helpers are equivalent to their counterparts without the "_lazy"
### suffix but they run on lazyframes
###
### They shouldn't be used manually. Instead they are automatically inserted in
### some test files by the code in setup.R.
expect_equal_lazy <- function(x, y, ...) {
  if (inherits(x, "polars_lazy_frame")) {
    x <- x$collect()
  }
  if (inherits(y, "polars_lazy_frame")) {
    y <- y$collect()
  }
  expect_equal(x, y, ...)
}

expect_error_lazy <- function(current, pattern = ".*", ...) {
  expect_error(current$collect(), pattern, ...)
}

expect_snapshot_lazy <- function(current, ...) {
  expect_snapshot(current$collect(), ...)
}

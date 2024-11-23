#' Compare the query result with LazyFrame and DataFrame
#'
#' Inspired by `compare_dplyr_binding` of the arrow package.
#' @param object A polars query, must be started with `.input`.
#' See the examples for details.
#' @param input R object will be converted to a DataFrame or LazyFrame
#' by `as_polars_df` or `as_polars_lf`.
#' @param expected A polars DataFrame, the expected result of the query.
#' @examples
#' expect_query_equal(
#'   .input$select("foo"),
#'   pl$DataFrame(foo = NULL, bar = NULL),
#'   pl$DataFrame(foo = NULL)
#' )
#' @noRd
expect_query_equal <- function(object, input, expected) {
  query <- rlang::enquo(object)
  out_lazy <- rlang::eval_tidy(query, rlang::new_data_mask(rlang::env(.input = as_polars_lf(input))))$collect()
  out_eager <- rlang::eval_tidy(query, rlang::new_data_mask(rlang::env(.input = as_polars_df(input))))

  expect_equal(out_lazy, expected)
  expect_equal(out_eager, expected)

  invisible(NULL)
}

#' Compare the query error with LazyFrame and DataFrame
#'
#' Same as `expect_query_equal()`, but for `expect_error()`.
#' @param object A polars query, must be started with `.input`.
#' See `expect_query_equal()` for details.
#' @param input R object will be converted to a DataFrame or LazyFrame
#' by `as_polars_df()` or `as_polars_lf()`.
#' @param regexp passed to `expect_error()`.
#' @param class passed to `expect_error()`.
#' @param ... passed to `expect_error()`.
#' @noRd
expect_query_error <- function(object, input, regexp = NULL, class = NULL, ...) {
  query <- rlang::enquo(object)
  expect_error(
    rlang::eval_tidy(query, rlang::new_data_mask(rlang::env(.input = as_polars_lf(input))))$collect(),
    regexp = regexp,
    class = class,
    ...
  )
  expect_error(
    rlang::eval_tidy(query, rlang::new_data_mask(rlang::env(.input = as_polars_df(input)))),
    regexp = regexp,
    class = class,
    ...
  )

  invisible(NULL)
}

#' Mix of `expect_query_equal()` and `expect_query_error()`
#'
#' The query only succeeds for DataFrame, but fails for LazyFrame.
expect_eager_equal_lazy_error <- function(object, input, expected, regexp = NULL, class = NULL, ...) {
  query <- rlang::enquo(object)
  out_eager <- rlang::eval_tidy(query, rlang::new_data_mask(rlang::env(.input = as_polars_df(input))))

  expect_equal(out_eager, expected)
  expect_error(
    rlang::eval_tidy(query, rlang::new_data_mask(rlang::env(.input = as_polars_lf(input))))$collect(),
    regexp = regexp,
    class = class,
    ...
  )
  invisible(NULL)
}

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

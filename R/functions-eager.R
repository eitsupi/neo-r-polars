# TODO: implement `how = "align"`
#' Combine multiple DataFrames, LazyFrames, or Series into a single object
#'
#' @inheritParams rlang::check_dots_empty0
#' @param items DataFrames, LazyFrames, or Series to concatenate.
#' @param how Strategy to concatenate items. Must be one of:
#' * `"vertical"`: applies multiple vstack operations;
#' * `"vertical_relaxed"`: same as `"vertical"`, but additionally coerces
#'   columns to their common supertype if they are mismatched (eg: Int32 to
#'   Int64);
#' * `"diagonal"`: finds a union between the column schemas and fills missing
#'   column values with `null`;
#' * `"diagonal_relaxed"`: same as `"diagonal"`, but additionally coerces
#'   columns to their common supertype if they are mismatched (eg: Int32 to
#'   Int64);
#' * `"horizontal"`: stacks Series from DataFrames horizontally and fills with
#'   `null` if the lengths don’t match;
#' * `"align"`: Combines frames horizontally, auto-determining the common key
#'   columns and aligning rows using the same logic as `align_frames`; this
#'   behaviour is patterned after a full outer join, but does not handle
#'   column-name collision. (If you need more control, you should use a
#'   suitable join method instead).
#'
#' Series only support the `"vertical"` strategy.
#' @param rechunk Make sure that the result data is in contiguous memory.
#' @param parallel Only relevant for LazyFrames. This determines if the
#' concatenated lazy computations may be executed in parallel.
#'
#' @return The same class (`polars_data_frame`, `polars_lazy_frame` or
#' `polars_series`) as the input.
#' @examples
#' # default is 'vertical' strategy
#' df1 <- pl$DataFrame(a = 1L, b = 3L)
#' df2 <- pl$DataFrame(a = 2L, b = 4L)
#' pl$concat(c(df1, df2))
#'
#' # 'a' is coerced to float64
#' df1 <- pl$DataFrame(a = 1L, b = 3L)
#' df2 <- pl$DataFrame(a = 2, b = 4L)
#' pl$concat(c(df1, df2), how = "vertical_relaxed")
#'
#' df_h1 <- pl$DataFrame(l1 = 1:2, l2 = 3:4)
#' df_h2 <- pl$DataFrame(r1 = 5:6, r2 = 7:8, r3 = 9:10)
#' pl$concat(c(df_h1, df_h2), how = "horizontal")
#'
#' # use 'diagonal' strategy to fill empty column values with nulls
#' df1 <- pl$DataFrame(a = 1L, b = 3L)
#' df2 <- pl$DataFrame(a = 2L, c = 4L)
#' pl$concat(c(df1, df2), how = "diagonal")
#'
#' df_a1 <- pl$DataFrame(id = 1:2, x = 3:4)
#' df_a2 <- pl$DataFrame(id = 2:3, y = 5:6)
#' df_a3 <- pl$DataFrame(id = c(1L, 3L), z = 7:8)
#' pl$concat(c(df_a1, df_a2, df_a3), how = "align")
pl__concat <- function(
    items,
    ...,
    how = "vertical",
    rechunk = FALSE,
    parallel = TRUE) {
  wrap({
    check_dots_empty0(...)

    if (length(items) == 0L) {
      abort("`items` cannot be empty.")
    }
    all_df_lf_series <-
      all(vapply(items, is_polars_df, FUN.VALUE = logical(1))) ||
        all(vapply(items, is_polars_lf, FUN.VALUE = logical(1))) ||
        all(vapply(items, is_polars_series, FUN.VALUE = logical(1)))
    if (!all_df_lf_series) {
      abort("All elements in `items` must be of the same class (Polars DataFrame, LazyFrame, Series, or Expr).")
    }

    first <- items[[1]]


    if (length(items) == 1L && (is_polars_df(first) || is_polars_series(first) || is_polars_lf(first))) {
      return(first)
    }

    if (how == "align") {
      if (!inherits(first, c("polars_data_frame", "polars_lazy_frame"))) {
        abort("'align' strategy is only supported on DataFrames and LazyFrames.")
      }

      # TODO: "align"
    }

    out <- if (is_polars_df(first)) {
      how <- arg_match0(
        how,
        values = c("vertical", "vertical_relaxed", "diagonal", "diagonal_relaxed", "horizontal")
      )
      switch(how,
        "vertical" = {
          items |>
            lapply(\(x) x$`_df`) |>
            concat_df() |>
            wrap()
        },
        "vertical_relaxed" = {
          res <- items |>
            lapply(\(x) x$lazy()$`_ldf`) |>
            concat_lf(
              rechunk = rechunk,
              parallel = parallel,
              to_supertypes = TRUE
            ) |>
            wrap()
          res$collect(no_optimization = TRUE)
        },
        "diagonal" = {
          items |>
            lapply(\(x) x$`_df`) |>
            concat_df_diagonal() |>
            wrap()
        },
        "diagonal_relaxed" = {
          res <- items |>
            lapply(\(x) x$lazy()$`_ldf`) |>
            concat_lf_diagonal(
              rechunk = rechunk,
              parallel = parallel,
              to_supertypes = TRUE
            ) |>
            wrap()
          res$collect(no_optimization = TRUE)
        },
        "horizontal" = {
          items |>
            lapply(\(x) x$`_df`) |>
            concat_df_horizontal() |>
            wrap()
        }
      )
    } else if (is_polars_lf(first)) {
      switch(how,
        "vertical" = ,
        "vertical_relaxed" = {
          items |>
            lapply(\(x) x$`_ldf`) |>
            concat_lf(
              rechunk = rechunk,
              parallel = parallel,
              to_supertypes = endsWith(how, "relaxed")
            ) |>
            wrap()
        },
        "diagonal" = ,
        "diagonal_relaxed" = {
          items |>
            lapply(\(x) x$`_ldf`) |>
            concat_lf_diagonal(
              rechunk = rechunk,
              parallel = parallel,
              to_supertypes = endsWith(how, "relaxed")
            ) |>
            wrap()
        },
        "horizontal" = {
          items |>
            lapply(\(x) x$`_ldf`) |>
            concat_lf_horizontal(parallel = parallel) |>
            wrap()
        }
      )
    } else if (is_polars_series(first)) {
      if (how == "vertical") {
        items |>
          lapply(\(x) x$`_df`) |>
          concat_series() |>
          wrap()
      } else {
        abort("Series only supports 'vertical' concat strategy.")
      }
    } else if (is_polars_expr(first)) {
      items |>
        lapply(\(x) x$`_rexpr`) |>
        concat_expr() |>
        wrap()
    } else {
      abort(
        sprintf("`items` only accepts polars DataFrames, LazyFrames, Series, and Expr. Found '%s'.", class(first)[1])
      )
    }

    if (isTRUE(rechunk)) {
      out <- out$rechunk()
    }
    out
  })
}

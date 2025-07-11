# TODO: link to data type doc
#' Create a Polars DataFrame from an R object
#'
#' The [as_polars_df()] function creates a [polars DataFrame][DataFrame] from various R objects.
#' Because [Polars DataFrame][DataFrame] can be converted to a [struct type][pl__Struct] [Series]
#' and vice versa, objects that are converted to a [struct type][pl__Struct] type [Series] by
#' [as_polars_series()] are supported by this function.
#'
#' ## Default S3 method
#'
#' Basically, this method is a shortcut for `as_polars_series(x, ...)$struct$unnest()`.
#' Before converting the object to a [Series], the [infer_polars_dtype()] function is used
#' to check if the object can be converted to a [struct dtype][pl__Struct].
#'
#' ## S3 method for [list]
#'
#' - The argument `...` (except `name`) is passed to [as_polars_series()]
#'   for each element of the list.
#' - All elements of the list must be converted to [Series] by [as_polars_series()].
#' - All of the [Series] must be converted to the same length, except for the case of length 1,
#'   which will be recycled to match the length of the other [Series]
#'   if they have a length other than 1.
#' - The name of the each element is used as the column name of the [DataFrame].
#'   For unnamed elements, the column name will be an empty string `""` or if the element is
#'   a [Series],
#'   the column name will be the name of the [Series].
#'
#' ## S3 method for [data.frame]
#'
#' - The argument `...` (except `name`) is passed to [as_polars_series()] for each column.
#' - All columns must be converted to the same length of [Series] by [as_polars_series()].
#'
#' ## S3 method for [polars_series][Series]
#'
#' This is a shortcut for [`<Series>$to_frame()`][series__to_frame] or
#' [`<Series>$struct$unnest()`][series_struct_unnest], depending on the `from_struct` argument
#' and the [Series] data type.
#' The `column_name` argument is passed to the `name` argument of the
#' [`$to_frame()`][series__to_frame] method.
#'
#' ## S3 method for [polars_lazy_frame][LazyFrame]
#'
#' This is a shortcut for [`<LazyFrame>$collect()`][lazyframe__collect].
#' @inherit pl__DataFrame return
#' @inheritParams as_polars_series
#' @inheritParams lazyframe__collect
#' @param column_name A character or `NULL`. If not `NULL`,
#' name/rename the [Series] column in the new [DataFrame].
#' If `NULL`, the column name is taken from the [Series] name.
#' @param from_struct A logical. If `TRUE` (default) and the [Series] data type is a struct,
#' the [`<Series>$struct$unnest()`][series_struct_unnest] method is used to create a [DataFrame]
#' from the struct [Series]. In this case, the `column_name` argument is ignored.
#' @seealso
#' - [`as.list(<polars_data_frame>)`][as.list.polars_data_frame]: Export the DataFrame as an R list.
#' - [`as.data.frame(<polars_data_frame>)`][as.data.frame.polars_data_frame]:
#'   Export the DataFrame as an R data frame.
#' @examples
#' # list
#' as_polars_df(list(a = 1:2, b = c("foo", "bar")))
#'
#' # data.frame
#' as_polars_df(data.frame(a = 1:2, b = c("foo", "bar")))
#'
#' # polars_series
#' s_int <- as_polars_series(1:2, "a")
#' s_struct <- as_polars_series(
#'   data.frame(a = 1:2, b = c("foo", "bar")),
#'   "struct"
#' )
#'
#' ## Use the Series as a column
#' as_polars_df(s_int)
#' as_polars_df(s_struct, column_name = "values", from_struct = FALSE)
#'
#' ## Unnest the struct data
#' as_polars_df(s_struct)
#' @export
as_polars_df <- function(x, ...) {
  UseMethod("as_polars_df")
}

#' @rdname as_polars_df
#' @export
as_polars_df.default <- function(x, ...) {
  dtype <- try_fetch(
    infer_polars_dtype(x, ...),
    error = function(cnd) {
      abort(
        c(
          "This object can't be converted to a Polars Series, and hence to a Polars DataFrame.",
          `*` = sprintf(
            "%s can't be converted to a Polars Series by `as_polars_series()`.",
            obj_type_friendly(x)
          ),
          i = "The object must be converted to a struct type Series by `as_polars_series()` first."
        ),
        parent = cnd
      )
    }
  )
  if (inherits(dtype, "polars_dtype_struct")) {
    as_polars_series(x, ...)$struct$unnest()
  } else {
    abort(
      c(
        "This object is not supported for the default method of `as_polars_df()`.",
        `*` = sprintf(
          "It requires `x` to be Series with struct type, got: %s.",
          format(dtype, abbreviated = TRUE)
        ),
        i = "Use `infer_polars_dtype()` to check the data type of the object."
      )
    )
  }
}

#' @rdname as_polars_df
#' @export
as_polars_df.polars_series <- function(
  x,
  ...,
  column_name = NULL,
  from_struct = TRUE
) {
  if (isTRUE(from_struct) && inherits(x$dtype, "polars_dtype_struct")) {
    x$struct$unnest()
  } else {
    x$to_frame(name = column_name)
  }
}

#' @rdname as_polars_df
#' @export
as_polars_df.polars_data_frame <- function(x, ...) {
  x
}

#' @rdname as_polars_df
#' @export
as_polars_df.polars_group_by <- function(x, ...) {
  x$df
}

#' @rdname as_polars_df
#' @export
as_polars_df.polars_lazy_frame <- function(
  x,
  ...,
  type_coercion = TRUE,
  predicate_pushdown = TRUE,
  projection_pushdown = TRUE,
  simplify_expression = TRUE,
  slice_pushdown = TRUE,
  comm_subplan_elim = TRUE,
  comm_subexpr_elim = TRUE,
  cluster_with_columns = TRUE,
  no_optimization = FALSE,
  engine = c("auto", "in-memory", "streaming")
) {
  x$collect(
    type_coercion = type_coercion,
    predicate_pushdown = predicate_pushdown,
    projection_pushdown = projection_pushdown,
    simplify_expression = simplify_expression,
    slice_pushdown = slice_pushdown,
    comm_subplan_elim = comm_subplan_elim,
    comm_subexpr_elim = comm_subexpr_elim,
    cluster_with_columns = cluster_with_columns,
    no_optimization = no_optimization,
    engine = engine
  )
}

#' @rdname as_polars_df
#' @export
as_polars_df.list <- function(x, ...) {
  .args <- list2(...)
  # Should not pass the `name` argument
  .args$name <- NULL
  list_of_series <- lapply(x, \(column) eval(call2("as_polars_series", column, !!!.args)))

  # Series with length 1 should be recycled
  unique_lengths <- unique(lengths(list_of_series))
  n_lengths <- length(unique_lengths)

  list_of_plr_series <- if (n_lengths <= 1L) {
    list_of_series |>
      lapply(\(series) series$`_s`)
  } else {
    n_rows <- max(unique_lengths[unique_lengths != 1L])
    list_of_series |>
      lapply(
        \(series) {
          if (length(series) == 1L) {
            # Recycle the series with length 1
            pl$select(pl$repeat_(series, n_rows))$to_series()$`_s`
          } else {
            series$`_s`
          }
        }
      )
  }

  list_of_plr_series |>
    PlRDataFrame$init() |>
    wrap()
}

#' @rdname as_polars_df
#' @export
as_polars_df.data.frame <- as_polars_df.list

#' @rdname as_polars_df
#' @export
as_polars_df.NULL <- function(x, ...) {
  wrap({
    if (missing(x)) {
      abort("The `x` argument of `as_polars_df()` can't be missing")
    }
    PlRDataFrame$init(list())
  })
}

#' Infer polars data type corresponding to given R object
#' @export
infer_polars_dtype <- function(x, ...) {
  UseMethod("infer_polars_dtype")
}

infer_polars_dtype_default_impl <- function(x, ...) {
  as_polars_series(x[0L]) |>
    infer_polars_dtype(...)
}

#' @rdname infer_polars_dtype
#' @export
infer_polars_dtype.default <- function(x, ...) {
  if (is.atomic(x)) {
    infer_polars_dtype_default_impl(x, ...)
  } else {
    abort(
      sprintf("Unsupported class for `infer_polars_dtype()`: %s", toString(class(x))),
      call = parent.frame()
    )
  }
}

#' @rdname infer_polars_dtype
#' @export
infer_polars_dtype.polars_series <- function(x, ...) {
  x$dtype
}

#' @rdname infer_polars_dtype
#' @export
infer_polars_dtype.polars_data_frame <- function(x, ...) {
  pl$Struct(!!!x$collect_schema())
}

#' @rdname infer_polars_dtype
#' @export
infer_polars_dtype.polars_lazy_frame <- infer_polars_dtype.polars_data_frame

#' @rdname infer_polars_dtype
#' @export
infer_polars_dtype.POSIXlt <- infer_polars_dtype_default_impl

#' @rdname infer_polars_dtype
#' @export
infer_polars_dtype.NULL <- function(x, ...) {
  if (missing(x)) {
    abort("The `x` argument of `infer_polars_dtype()` can't be missing")
  }
  pl$Null
}

#' @rdname infer_polars_dtype
#' @export
infer_polars_dtype.list <- function(x, ..., strict = FALSE) {
  wrap({
    child_type <- lapply(x, \(child) {
      if (is.null(child)) {
        NULL
      } else {
        infer_polars_dtype(child, ..., strict = strict)$`_dt`
      }
    }) |>
      PlRDataType$infer_supertype(strict = strict)

    PlRDataType$new_list(child_type)
  })
}

#' @rdname infer_polars_dtype
#' @export
infer_polars_dtype.AsIs <- function(x, ...) {
  class(x) <- setdiff(class(x), "AsIs")
  infer_polars_dtype(x, ...)
}

#' @rdname infer_polars_dtype
#' @export
infer_polars_dtype.data.frame <- function(x, ...) {
  pl$Struct(!!!lapply(x, \(col) infer_polars_dtype(col, ...)))
}

#' @rdname infer_polars_dtype
#' @export
infer_polars_dtype.vctrs_vctr <- infer_polars_dtype_default_impl

#' @rdname infer_polars_dtype
#' @export
# Because the `ptype` attribute records `list()` when the elements are lists,
# so the solution to read `ptype` is incomplete and not used here.
infer_polars_dtype.vctrs_list_of <- infer_polars_dtype.list

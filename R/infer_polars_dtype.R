#' Infer polars data type corresponding to given R object
#' @export
infer_polars_dtype <- function(x, ...) {
  UseMethod("infer_polars_dtype")
}

#' @rdname infer_polars_dtype
#' @export
infer_polars_dtype.default <- function(x, ...) {
  if (is.atomic(x)) {
    as_polars_series(x[0L]) |>
      infer_polars_dtype()
  } else {
    abort(
      sprintf("Unsupported class for `as_polars_series()`: %s", toString(class(x))),
      call = parent.frame()
    )
  }
}

#' @rdname infer_polars_dtype
#' @export
infer_polars_dtype.polars_dtype <- function(x, ...) {
  x
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
infer_polars_dtype.NULL <- function(x, ...) {
  if (missing(x)) {
    abort("The `x` argument of `infer_polars_dtype()` can't be missing")
  }
  pl$Null
}

# TODO: should implement a function to get super type on the Rust side
#' @rdname infer_polars_dtype
#' @export
infer_polars_dtype.list <- function(x, ..., strict = FALSE) {
  abort("TODO")
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
  pl$Struct(!!!lapply(x, infer_polars_dtype))
}

#' @rdname infer_polars_dtype
#' @export
infer_polars_dtype.vctrs_vctr <- function(x, ...) {
  as_polars_series(x[0L]) |>
    infer_polars_dtype(...)
}

#' @rdname infer_polars_dtype
#' @export
# Because the `ptype` attribute records `list()` when the elements are lists,
# so the solution to read `ptype` is incomplete and not used here.
infer_polars_dtype.vctrs_list_of <- infer_polars_dtype.list

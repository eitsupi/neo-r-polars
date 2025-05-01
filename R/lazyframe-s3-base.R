# TODO: mimic the Python's one
#' @export
print.polars_lazy_frame <- function(x, ...) {
  cat(sprintf("<polars_lazy_frame at %s>\n", obj_address(x)))
  invisible(x)
}

#' @export
dim.polars_lazy_frame <- function(x) c(NA_integer_, length(x$collect_schema()))

#' @export
length.polars_lazy_frame <- function(x) length(x$collect_schema())

#' @export
names.polars_lazy_frame <- function(x) names(x$collect_schema())

#' @export
#' @rdname s3-as.list
as.list.polars_lazy_frame <- as.list.polars_data_frame

#' @export
#' @rdname s3-as.data.frame
as.data.frame.polars_lazy_frame <- as.data.frame.polars_data_frame

#' @exportS3Method utils::head
head.polars_lazy_frame <- head.polars_data_frame

#' @exportS3Method utils::tail
tail.polars_lazy_frame <- tail.polars_data_frame


# Try to match `tibble` behavior as much as possible, following
# https://tibble.tidyverse.org/articles/invariants.html#column-subsetting
# TODO: add document
#' @export
`[.polars_lazy_frame` <- function(x, i, j, ..., drop = FALSE) {
  # useful for error messages below
  i_arg <- substitute(i)
  j_arg <- substitute(j)

  if (isTRUE(drop)) {
    warn(c(`!` = "`drop = TRUE` is not supported for LazyFrame."))
  }
  if (!missing(i)) {
    n_real_args <- nargs() - !missing(drop)
    if (n_real_args > 2) {
      abort(
        c(
          `!` = "Cannot subset rows of a LazyFrame with `[`.",
          i = "There are several functions that can be used to get a specific range of rows.",
          `*` = "`$slice()` can be used to get a slice of rows.",
          `*` = "`$gather_every()` can be used to take every nth row.",
          `*` = "`$reverse()` can be used to reverse the order of rows."
        )
      )
    } else {
      j <- i
      j_arg <- i_arg
      i <- NULL
    }
  } else {
    i <- NULL
  }

  # We must put `i` as an empty arg so that both missing(i) and nargs() work
  # in the `polars_data_frame` method.
  `[.polars_data_frame`(x, , j = j, drop = FALSE)
}

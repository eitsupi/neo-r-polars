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
`[.polars_lazy_frame` <- function(x, i, j, drop = FALSE) {
  cols <- names(x)

  if (isTRUE(drop)) {
    warn(
      c(
        `!` = "`drop` argument ignored when subsetting a LazyFrame.",
        i = "Collect the LazyFrame first."
      )
    )
    warn <- FALSE
  }
  if (!missing(i) || !is_null(i)) {
    if (!missing(j)) {
      abort(
        c(
          `!` = "Cannot subset rows of a LazyFrame with `[`.",
          i = "Either use `$slice()` or collect the LazyFrame first."
        )
      )
    } else {
      j <- i
      i <- NULL
    }
  }

  # useful for error messages below
  j_arg <- substitute(j)

  if (is_null(i) && is_null(j)) {
    return(x)
  }

  #### Columns -----------------------------------------------------

  # check accepted types for subsetting columns
  if (!is_null(j) && !is_bare_character(j) && !is_bare_numeric(j) && !is_bare_logical(j)) {
    abort(
      c(
        sprintf("Can't subset columns with `%s`.", deparse(j_arg)),
        i = sprintf(
          "`%s` must be logical, numeric, or character, not %s.",
          deparse(j_arg),
          obj_type_friendly(j)
        )
      )
    )
  }

  if (is_bare_numeric(j) && !rlang::is_integerish(j)) {
    abort(
      c(
        sprintf("Can't subset columns with `%s`.", deparse(j_arg)),
        x = "Can't convert from `j` <double> to <integer> due to loss of precision."
      )
    )
  }

  # NA values are accepted for rows but not columns.
  if (anyNA(j)) {
    na_val <- which(is.na(j))
    abort(
      c(
        sprintf("Can't subset columns with `%s`.", deparse(j_arg)),
        x = sprintf("Subscript `%s` can't contain missing values.", deparse(j_arg)),
        x = sprintf(
          "It has missing value(s) at location %s.",
          oxford_comma(na_val, final = "and")
        )
      )
    )
  }

  if (!is_null(j)) {
    # Can be:
    # - numeric but cannot beyond the number of columns, and cannot mix positive
    #   and negative indices
    # - logical but must be of length 1 or number of columns
    # - character
    if (is_bare_numeric(j)) {
      wrong_locs <- j[j > length(cols)]
      if (length(wrong_locs) > 0) {
        abort(
          c(
            "Can't subset columns past the end.",
            i = sprintf("Location(s) %s don't exist.", oxford_comma(wrong_locs, final = "and")),
            i = sprintf("There are only %s columns.", length(cols))
          )
        )
      }
      if (all(j < 0)) {
        j <- setdiff(seq_len(length(cols)), abs(j))
      } else if (any(j < 0) && any(j > 0)) {
        sign_start <- sign(j[j != 0])[1]
        loc <- if (sign_start == -1) {
          which(sign(j) == 1)[1]
        } else if (sign_start == 1) {
          which(sign(j) == -1)[1]
        }
        abort(
          c(
            `!` = sprintf("Can't subset columns with `%s`.", deparse(j_arg)),
            x = "Negative and positive locations can't be mixed.",
            i = sprintf(
              "Subscript `%s` has a %s value at location %s.",
              deparse(j_arg),
              if (sign_start == 1) "negative" else "positive",
              loc
            )
          )
        )
      }
      to_select <- cols[j]
    } else if (is_bare_character(j)) {
      to_select <- j
    } else if (is_bare_logical(j)) {
      if (length(j) == 1) {
        to_select <- cols
      } else if (length(j) == length(cols)) {
        to_select <- cols[j]
      } else {
        abort(
          c(
            `!` = sprintf("Can't subset columns with `%s`.", deparse(j_arg)),
            i = sprintf(
              "Logical subscript `%s` must be size 1 or %s, not %s",
              deparse(j_arg),
              length(cols),
              length(j)
            )
          )
        )
      }
    }

    # Don't do this too early because x[, TRUE, drop = TRUE] mustn't drop if
    # x has more than 1 column
    if (length(to_select) > 1) {
      drop <- FALSE
    }

    x <- x$select(to_select)
  }

  if (isTRUE(drop)) {
    x$get_columns()[[1]]
  } else {
    x
  }
}

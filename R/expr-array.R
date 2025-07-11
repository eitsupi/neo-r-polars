# The env for storing all expr arr methods
polars_expr_arr_methods <- new.env(parent = emptyenv())

namespace_expr_arr <- function(x) {
  self <- new.env(parent = emptyenv())
  self$`_rexpr` <- x$`_rexpr`

  class(self) <- c(
    "polars_namespace_expr_arr",
    "polars_namespace_expr",
    "polars_object"
  )
  self
}


#' Compute the sum of the sub-arrays
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   values = list(c(1, 2), c(3, 4), c(NA, 6))
#' )$cast(pl$Array(pl$Float64, 2))
#' df$with_columns(sum = pl$col("values")$arr$sum())
expr_arr_sum <- function() {
  self$`_rexpr`$arr_sum() |>
    wrap()
}

#' Compute the max value of the sub-arrays
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   values = list(c(1, 2), c(3, 4), c(NA, NA))
#' )$cast(pl$Array(pl$Float64, 2))
#' df$with_columns(max = pl$col("values")$arr$max())
expr_arr_max <- function() {
  self$`_rexpr`$arr_max() |>
    wrap()
}

#' Compute the min value of the sub-arrays
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   values = list(c(1, 2), c(3, 4), c(NA, NA))
#' )$cast(pl$Array(pl$Float64, 2))
#' df$with_columns(min = pl$col("values")$arr$min())
expr_arr_min <- function() {
  self$`_rexpr`$arr_min() |>
    wrap()
}

#' Compute the median value of the sub-arrays
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   values = list(c(2, 1, 4), c(8.4, 3.2, 1)),
#' )$cast(pl$Array(pl$Float64, 3))
#' df$with_columns(median = pl$col("values")$arr$median())
expr_arr_median <- function() {
  self$`_rexpr`$arr_median() |>
    wrap()
}

#' Compute the standard deviation of the sub-arrays
#'
#' @inheritParams dataframe__std
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   values = list(c(2, 1, 4), c(8.4, 3.2, 1)),
#' )$cast(pl$Array(pl$Float64, 3))
#' df$with_columns(std = pl$col("values")$arr$std())
expr_arr_std <- function(ddof = 1) {
  self$`_rexpr`$arr_std(ddof) |>
    wrap()
}

#' Compute the variance of the sub-arrays
#'
#' @inheritParams dataframe__var
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   values = list(c(2, 1, 4), c(8.4, 3.2, 1)),
#' )$cast(pl$Array(pl$Float64, 3))
#' df$with_columns(var = pl$col("values")$arr$var())
expr_arr_var <- function(ddof = 1) {
  self$`_rexpr`$arr_var(ddof) |>
    wrap()
}

#' Sort values in every sub-array
#'
#' @inheritParams rlang::args_dots_empty
#' @inheritParams expr__sort
#' @examples
#' df <- pl$DataFrame(
#'   values = list(c(2, 1), c(3, 4), c(NA, 6))
#' )$cast(pl$Array(pl$Float64, 2))
#' df$with_columns(sort = pl$col("values")$arr$sort(nulls_last = TRUE))
expr_arr_sort <- function(..., descending = FALSE, nulls_last = FALSE) {
  wrap({
    check_dots_empty0(...)
    self$`_rexpr`$arr_sort(descending, nulls_last)
  })
}

#' Reverse values in every sub-array
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   values = list(c(1, 2), c(3, 4), c(NA, 6))
#' )$cast(pl$Array(pl$Float64, 2))
#' df$with_columns(reverse = pl$col("values")$arr$reverse())
expr_arr_reverse <- function() {
  self$`_rexpr`$arr_reverse() |>
    wrap()
}

#' Get the unique values in every sub-array
#'
#' @inheritParams rlang::args_dots_empty
#' @inheritParams expr__unique
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   values = list(c(1, 1, 2), c(4, 4, 4), c(NA, 6, 7)),
#' )$cast(pl$Array(pl$Float64, 3))
#' df$with_columns(unique = pl$col("values")$arr$unique())
expr_arr_unique <- function(..., maintain_order = FALSE) {
  wrap({
    check_dots_empty0(...)
    self$`_rexpr`$arr_unique(maintain_order)
  })
}


#' Get the value by index in every sub-array
#'
#' This allows to extract one value per array only. Values are 0-indexed (so
#' index `0` would return the first item of every sub-array) and negative values
#' start from the end (so index `-1` returns the last item).
#'
#' @inherit expr_list_get params return
#' @param index An Expr or something coercible to an Expr, that must return a
#'   single index.
#' @examples
#' df <- pl$DataFrame(
#'   values = list(c(1, 2), c(3, 4), c(NA, 6)),
#'   idx = c(1, NA, 3)
#' )$cast(values = pl$Array(pl$Float64, 2))
#' df$with_columns(
#'   using_expr = pl$col("values")$arr$get("idx"),
#'   val_0 = pl$col("values")$arr$get(0),
#'   val_minus_1 = pl$col("values")$arr$get(-1),
#'   val_oob = pl$col("values")$arr$get(10)
#' )
expr_arr_get <- function(index, ..., null_on_oob = TRUE) {
  wrap({
    check_dots_empty0(...)
    self$`_rexpr`$arr_get(as_polars_expr(index)$`_rexpr`, null_on_oob)
  })
}


#' Check if sub-arrays contain the given item
#'
#' @inherit as_polars_expr return
#' @inheritParams expr_list_contains
#' @examples
#' df <- pl$DataFrame(
#'   values = list(0:2, 4:6, c(NA, NA, NA)),
#'   item = c(0L, 4L, 2L),
#' )$cast(values = pl$Array(pl$Float64, 3))
#' df$with_columns(
#'   with_expr = pl$col("values")$arr$contains(pl$col("item")),
#'   with_lit = pl$col("values")$arr$contains(1)
#' )
expr_arr_contains <- function(item, ..., nulls_equal = TRUE) {
  wrap({
    check_dots_empty0(...)
    self$`_rexpr`$arr_contains(as_polars_expr(item, as_lit = TRUE)$`_rexpr`, nulls_equal)
  })
}

#' Join elements in every sub-array
#'
#' Join all string items in a sub-array and place a separator between them. This
#' only works if the inner type of the array is `String`.
#'
#' @param separator String to separate the items with. Can be an Expr. Strings
#'   are not parsed as columns.
#' @inheritParams rlang::args_dots_empty
#' @inheritParams pl__concat_str
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   values = list(c("a", "b", "c"), c("x", "y", "z"), c("e", NA, NA)),
#'   separator = c("-", "+", "/"),
#' )$cast(values = pl$Array(pl$String, 3))
#' df$with_columns(
#'   join_with_expr = pl$col("values")$arr$join(pl$col("separator")),
#'   join_with_lit = pl$col("values")$arr$join(" "),
#'   join_ignore_null = pl$col("values")$arr$join(" ", ignore_nulls = TRUE)
#' )
expr_arr_join <- function(separator, ..., ignore_nulls = FALSE) {
  wrap({
    check_dots_empty0(...)
    self$`_rexpr`$arr_join(as_polars_expr(separator, as_lit = TRUE)$`_rexpr`, ignore_nulls)
  })
}

#' Retrieve the index of the minimum value in every sub-array
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   values = list(1:2, 2:1)
#' )$cast(pl$Array(pl$Int32, 2))
#' df$with_columns(
#'   arg_min = pl$col("values")$arr$arg_min()
#' )
expr_arr_arg_min <- function() {
  self$`_rexpr`$arr_arg_min() |>
    wrap()
}

#' Retrieve the index of the maximum value in every sub-array
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   values = list(1:2, 2:1)
#' )$cast(pl$Array(pl$Int32, 2))
#' df$with_columns(
#'   arg_max = pl$col("values")$arr$arg_max()
#' )
expr_arr_arg_max <- function() {
  self$`_rexpr`$arr_arg_max() |>
    wrap()
}

#' Evaluate whether all boolean values are true for every sub-array
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   values = list(c(TRUE, TRUE), c(FALSE, TRUE), c(FALSE, FALSE), c(NA, NA)),
#' )$cast(pl$Array(pl$Boolean, 2))
#' df$with_columns(all = pl$col("values")$arr$all())
expr_arr_all <- function() {
  self$`_rexpr`$arr_all() |>
    wrap()
}

#' Evaluate whether any boolean value is true for every sub-array
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   values = list(c(TRUE, TRUE), c(FALSE, TRUE), c(FALSE, FALSE), c(NA, NA)),
#' )$cast(pl$Array(pl$Boolean, 2))
#' df$with_columns(any = pl$col("values")$arr$any())
expr_arr_any <- function() {
  self$`_rexpr`$arr_any() |>
    wrap()
}

#' Shift values in every sub-array by the given number of indices
#'
#' @inheritParams dataframe__shift
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   values = list(1:3, c(2L, NA, 5L)),
#'   idx = 1:2,
#' )$cast(values = pl$Array(pl$Int32, 3))
#' df$with_columns(
#'   shift_by_expr = pl$col("values")$arr$shift(pl$col("idx")),
#'   shift_by_lit = pl$col("values")$arr$shift(2)
#' )
expr_arr_shift <- function(n = 1) {
  self$`_rexpr`$arr_shift(as_polars_expr(n, as_lit = TRUE)$`_rexpr`) |>
    wrap()
}


#' Convert an Array column into a List column with the same inner data type
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   a = list(c(1, 2), c(3, 4))
#' )$cast(pl$Array(pl$Int8, 2))
#'
#' df$with_columns(
#'   list = pl$col("a")$arr$to_list()
#' )
expr_arr_to_list <- function() {
  self$`_rexpr`$arr_to_list() |>
    wrap()
}

#' Convert the Series of type Array to a Series of type Struct
#'
#' @param fields `r lifecycle::badge("experimental")`
#'   `NULL` (default) or character vector of field names, or a function that
#'   takes an integer index and returns character.
#'   If the name and number of the desired fields is known in advance,
#'   character vector of field names can be given, which will be assigned by index.
#'   Otherwise, to dynamically assign field names, a custom function can be used;
#'   if neither are set, fields will be `field_0`, `field_1`...
#'   See the examples for details.
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   n = list(c(0, 1, 2), c(3, 4, 5)),
#'   .schema_overrides = list(n = pl$Array(pl$Int8, 3))
#' )
#'
#' df$with_columns(struct = pl$col("n")$arr$to_struct())
#'
#' # Convert array to struct with field name assignment by function/index:
#' df$select(pl$col("n")$arr$to_struct(\(idx) paste0("n", idx)))$unnest("n")
#'
#' # Convert array to struct with field name assignment by index from character:
#' df$select(pl$col("n")$arr$to_struct(c("a", "b", "c")))$unnest("n")
expr_arr_to_struct <- function(fields = NULL) {
  wrap({
    if (is_character(fields)) {
      wrap(self$`_rexpr`$arr_to_struct())$struct$rename_fields(fields)
    } else {
      name_gen <- if (is.null(fields)) {
        NULL
      } else {
        fields <- as_function(fields)
        \(idx) fields(idx)
      }
      self$`_rexpr`$arr_to_struct(name_gen)
    }
  })
}

#' Count how often a value occurs in every sub-array
#'
#' @param element An Expr or something coercible to an Expr that produces a
#' single value.
#' @inherit as_polars_expr return
#'
#' @examples
#' df <- pl$DataFrame(
#'   values = list(c(1, 2), c(1, 1), c(2, 2))
#' )$cast(pl$Array(pl$Int64, 2))
#' df$with_columns(number_of_twos = pl$col("values")$arr$count_matches(2))
expr_arr_count_matches <- function(element) {
  self$`_rexpr`$arr_count_matches(as_polars_expr(element, as_lit = TRUE)$`_rexpr`) |>
    wrap()
}

#' Explode array in separate rows
#'
#' Returns a column with a separate row for every array element.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   a = list(c(1, 2, 3), c(4, 5, 6))
#' )$cast(pl$Array(pl$Int64, 3))
#' df$select(pl$col("a")$arr$explode())
expr_arr_explode <- function() {
  self$`_rexpr`$explode() |>
    wrap()
}

#' Get the first value of the sub-arrays
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   a = list(c(1, 2, 3), c(4, 5, 6))
#' )$cast(pl$Array(pl$Int64, 3))
#' df$with_columns(first = pl$col("a")$arr$first())
expr_arr_first <- function() {
  self$get(0, null_on_oob = TRUE) |>
    wrap()
}

#' Get the last value of the sub-arrays
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   a = list(c(1, 2, 3), c(4, 5, 6))
#' )$cast(pl$Array(pl$Int64, 3))
#' df$with_columns(last = pl$col("a")$arr$last())
expr_arr_last <- function() {
  self$get(-1, null_on_oob = TRUE) |>
    wrap()
}

#' Count the number of unique values in every sub-array
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   a = list(c(1, 1, 2), c(2, 3, 4))
#' )$cast(pl$Array(pl$Int64, 3))
#' df$with_columns(n_unique = pl$col("a")$arr$n_unique())
expr_arr_n_unique <- function() {
  self$`_rexpr`$arr_n_unique() |>
    wrap()
}

# The env for storing all expr cat methods
polars_expr_cat_methods <- new.env(parent = emptyenv())

namespace_expr_cat <- function(x) {
  self <- new.env(parent = emptyenv())
  self$`_rexpr` <- x$`_rexpr`

  class(self) <- c(
    "polars_namespace_expr_cat",
    "polars_namespace_expr",
    "polars_object"
  )
  self
}

#' Get the categories stored in this data type
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   cats = factor(c("z", "z", "k", "a", "b")),
#'   vals = factor(c(3, 1, 2, 2, 3))
#' )
#' df
#'
#' df$select(
#'   pl$col("cats")$cat$get_categories()
#' )
#' df$select(
#'   pl$col("vals")$cat$get_categories()
#' )
expr_cat_get_categories <- function() {
  self$`_rexpr`$cat_get_categories() |>
    wrap()
}

#' Set Ordering
#'
#' @description
#' `r lifecycle::badge("deprecated")`
#' Determine how this categorical series should be sorted.
#'
#' @param ordering string either 'physical' or 'lexical'
#' - `"physical"`: use the physical representation of the categories to
#'   determine the order (default).
#' - `"lexical"`: use the string values to determine the order.
#'
#' @inherit as_polars_expr return
expr_cat_set_ordering <- function(ordering) {
  wrap({
    deprecate_warn(
      c(
        `!` = "$cat$set_ordering() is deprecated.",
        i = "Use `pl$Categorical(<ordering>)` when initiating or with `$cast()` instead."
      )
    )
    ordering <- arg_match0(ordering, values = c("lexical", "physical"))
    self$`_rexpr`$cat_set_ordering(ordering)
  })
}

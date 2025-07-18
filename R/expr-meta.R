# The env for storing all expr meta methods
polars_expr_meta_methods <- new.env(parent = emptyenv())

namespace_expr_meta <- function(x) {
  self <- new.env(parent = emptyenv())
  self$`_rexpr` <- x$`_rexpr`

  class(self) <- c(
    "polars_namespace_expr_meta",
    "polars_namespace_expr",
    "polars_object"
  )
  self
}

#' Indicate if this expression expands into multiple expressions
#'
#' @inherit as_polars_expr return
#' @examples
#' e <- pl$col("a", "b")$name$suffix("_foo")
#' e$meta$has_multiple_outputs()
expr_meta_has_multiple_outputs <- function() {
  self$`_rexpr`$meta_has_multiple_outputs() |>
    wrap()
}

#' Get the column name that this expression would produce
#'
#' It may not always be possible to determine the output name as that can
#' depend on the schema of the context; in that case this will raise an error
#' if `raise_if_undetermined = TRUE` (the default), and return `NA` otherwise.
#'
#' @inheritParams rlang::args_dots_empty
#' @param raise_if_undetermined If `TRUE` (default), raise an error if the
#' output name cannot be determined. Otherwise return `NA`.
#' @inherit as_polars_expr return
#' @examples
#' e <- pl$col("foo") * pl$col("bar")
#' e$meta$output_name()
#'
#' e_filter <- pl$col("foo")$filter(pl$col("bar") == 13)
#' e_filter$meta$output_name()
#'
#' e_sum_over <- pl$col("foo")$sum()$over("groups")
#' e_sum_over$meta$output_name()
#'
#' e_sum_slice <- pl$col("foo")$sum()$slice(pl$len() - 10, pl$col("bar"))
#' e_sum_slice$meta$output_name()
#'
#' pl$len()$meta$output_name()
expr_meta_output_name <- function(..., raise_if_undetermined = TRUE) {
  wrap({
    check_dots_empty0(...)

    if (isTRUE(raise_if_undetermined)) {
      self$`_rexpr`$meta_output_name()
    } else {
      tryCatch(
        self$`_rexpr`$meta_output_name(),
        error = function(e) {
          NA_character_
        }
      )
    }
  })
}

#' Undo any renaming operation like `alias` or `name$keep`
#'
#' @inherit as_polars_expr return
#' @examples
#' e <- pl$col("foo")$alias("bar")
#' e$meta$undo_aliases()$meta$eq(pl$col("foo"))
#'
#' e <- pl$col("foo")$sum()$over("bar")
#' e$name$keep()$meta$undo_aliases()$meta$eq(e)
expr_meta_undo_aliases <- function() {
  self$`_rexpr`$meta_undo_aliases() |>
    wrap()
}

expr_meta__selector_add <- function(other) {
  self$`_rexpr`$`_meta_selector_add`(other$`_rexpr`) |>
    wrap()
}

expr_meta__selector_and <- function(other) {
  self$`_rexpr`$`_meta_selector_and`(other$`_rexpr`) |>
    wrap()
}

expr_meta__selector_sub <- function(other) {
  self$`_rexpr`$`_meta_selector_sub`(other$`_rexpr`) |>
    wrap()
}

expr_meta__selector_xor <- function(other) {
  self$`_rexpr`$`_meta_selector_xor`(other$`_rexpr`) |>
    wrap()
}

expr_meta__as_selector <- function() {
  self$`_rexpr`$`_meta_as_selector`() |>
    wrap()
}

#' Serialize this expression to a string in binary or JSON format
#'
#' @inheritParams rlang::args_dots_empty
#' @param format The format in which to serialize. Must be one of:
#' * `"binary"` (default): serialize to binary format (bytes).
#' * `"json"`: serialize to JSON format (string).
#'
#' @details
#' Serialization is not stable across Polars versions: a LazyFrame serialized
#' in one Polars version may not be deserializable in another Polars version.
#'
#' @inherit as_polars_expr return
#' @examplesIf requireNamespace("jsonlite", quietly = TRUE)
#' # Serialize the expression into a binary representation.
#' expr <- pl$col("foo")$sum()$over("bar")
#' bytes <- expr$meta$serialize()
#' rawToChar(bytes)
#'
#' pl$deserialize_expr(bytes)
#'
#' # Serialize into json
#' expr$meta$serialize(format = "json") |>
#'   jsonlite::prettify()
expr_meta_serialize <- function(..., format = c("binary", "json")) {
  wrap({
    check_dots_empty0(...)
    format <- arg_match0(format, c("binary", "json"))
    # fmt: skip
    switch(format,
      binary = self$`_rexpr`$serialize_binary(),
      json = self$`_rexpr`$serialize_json(),
      abort("Unreachable")
    )
  })
}

#' Indicate if this expression is the same as another expression
#'
#' @param other Expression to compare with.
#' @inherit as_polars_expr return
#' @examples
#' foo_bar <- pl$col("foo")$alias("bar")
#' foo <- pl$col("foo")
#' foo_bar$meta$eq(foo)
#'
#' foo_bar2 <- pl$col("foo")$alias("bar")
#' foo_bar$meta$eq(foo_bar2)
expr_meta_eq <- function(other) {
  self$`_rexpr`$meta_eq(as_polars_expr(other)$`_rexpr`) |>
    wrap()
}

#' Indicate if this expression is not the same as another expression
#'
#' @inheritParams expr_meta_eq
#' @inherit as_polars_expr return
#' @examples
#' foo_bar <- pl$col("foo")$alias("bar")
#' foo <- pl$col("foo")
#' foo_bar$meta$ne(foo)
#'
#' foo_bar2 <- pl$col("foo")$alias("bar")
#' foo_bar$meta$ne(foo_bar2)
expr_meta_ne <- function(other) {
  !self$eq(other)
}

#' Get a list with the root column name
#'
#' @inherit as_polars_expr return
#' @examples
#' e <- pl$col("foo") * pl$col("bar")
#' e$meta$root_names()
#'
#' e_filter <- pl$col("foo")$filter(pl$col("bar") == 13)
#' e_filter$meta$root_names()
#'
#' e_sum_over <- pl$sum("foo")$over("groups")
#' e_sum_over$meta$root_names()
#'
#' e_sum_slice <- pl$sum("foo")$slice(pl$len() - 10, pl$col("bar"))
#' e_sum_slice$meta$root_names()
expr_meta_root_names <- function() {
  self$`_rexpr`$meta_root_names()
}

#' Format the expression as a tree
#'
#' @inheritParams rlang::args_dots_empty
#' @param as_dot `r lifecycle::badge("experimental")`
#'   If `TRUE`, show the dot syntax that can be used in other
#'   packages, such as `DiagrammeR`.
#' @param schema An optional schema. Must be `NULL` or a named list of
#'   [DataType].
#' @return
#' A string, either with the tree itself (if `as_dot = FALSE`) or with the
#' corresponding GraphViz code (if `as_dot = TRUE`).
#'
#' @examples
#' my_expr <- (pl$col("foo") * pl$col("bar"))$sum()$over(pl$col("ham")) / 2
#' cat(my_expr$meta$tree_format())
#'
#' \dontrun{
#' # This output can be displayed with DiagrammeR for instance
#' graph <- my_expr$meta$tree_format(as_dot = TRUE)
#' DiagrammeR::grViz(graph)
#' }
expr_meta_tree_format <- function(..., as_dot = FALSE, schema = NULL) {
  wrap({
    check_dots_empty0(...)
    check_list_of_polars_dtype(schema, allow_null = TRUE)

    if (!is.null(schema)) {
      schema <- parse_into_list_of_datatypes(!!!schema)
    }
    self$`_rexpr`$compute_tree_format(as_dot = as_dot, schema = schema)
  })
}


#' Indicate if this expression only selects columns (optionally with aliasing)
#'
#' This can include bare columns, column matches by regex or dtype, selectors
#' and exclude ops, and (optionally) column/expression aliasing.
#'
#' @inheritParams rlang::args_dots_empty
#' @param allow_aliasing If `FALSE` (default), any aliasing is not considered
#' pure column selection. Set `TRUE` to allow for column selection that also
#' includes aliasing.
#'
#' @return A logical value.
#' @examples
#' e <- pl$col("foo")
#' e$meta$is_column_selection()
#'
#' e <- pl$col("foo")$alias("bar")
#' e$meta$is_column_selection()
#'
#' e$meta$is_column_selection(allow_aliasing = TRUE)
#'
#' e <- pl$col("foo") * pl$col("bar")
#' e$meta$is_column_selection()
#'
#' e <- cs$starts_with("foo")
#' e$meta$is_column_selection()
expr_meta_is_column_selection <- function(..., allow_aliasing = FALSE) {
  wrap({
    check_dots_empty0(...)
    self$`_rexpr`$meta_is_column_selection(allow_aliasing)
  })
}

#' Indicate if this expression is a basic (non-regex) unaliased column
#'
#' @inherit expr_meta_is_column_selection return
#' @examples
#' e <- pl$col("foo")
#' e$meta$is_column()
#'
#' e <- pl$col("foo") * pl$col("bar")
#' e$meta$is_column()
#'
#' e <- pl$col("^col\\.*\\d+$")
#' e$meta$is_column()
expr_meta_is_column <- function() {
  wrap({
    self$`_rexpr`$meta_is_column()
  })
}

#' Indicate if this expression expands to columns that match a regex pattern
#'
#' @inherit expr_meta_is_column_selection return
#' @examples
#' e <- pl$col("^.*$")$name$prefix("foo_")
#' e$meta$is_regex_projection()
expr_meta_is_regex_projection <- function() {
  wrap({
    self$`_rexpr`$meta_is_regex_projection()
  })
}

#' Pop the latest expression and return the input(s) of the popped expression
#'
#' @inherit as_polars_expr return
#' @inheritParams rlang::args_dots_empty
#' @param schema An optional schema. Must be `NULL` or a named list of
#'   [DataType].
#' @examples
#' e <- pl$col("foo") + pl$col("bar")
#' first <- e$meta$pop()[[1]]
#'
#' first$meta$eq(pl$col("bar"))
#' first$meta$eq(pl$col("foo"))
expr_meta_pop <- function(..., schema = NULL) {
  wrap({
    check_dots_empty0(...)
    check_list_of_polars_dtype(schema, allow_null = TRUE)

    if (!is.null(schema)) {
      schema <- parse_into_list_of_datatypes(!!!schema)
    }

    self$`_rexpr`$meta_pop(schema = schema) |>
      lapply(\(ptr) {
        .savvy_wrap_PlRExpr(ptr) |>
          wrap()
      })
  })
}

#' Parse dynamic dots into a list of expressions (PlRExpr, not polars-expr)
#' @noRd
parse_into_list_of_expressions <- function(..., `__structify` = FALSE) {
  dots <- list2(...)
  call <- caller_env()
  try_fetch(
    lapply(dots, \(x) as_polars_expr(x, structify = `__structify`)$`_rexpr`),
    error = function(cnd) {
      msg <- cnd$parent$message
      if (!is.null(msg) && grepl("Passing Polars expression objects to", msg)) {
        abort(
          c(
            "`...` doesn't accept inputs of type list with Polars expressions.",
            "i" = "Perhaps you forgot to use `!!!` on the input(s), e.g. `!!!my_list`?"
          ),
          call = call
        )
      } else {
        zap()
      }
    }
  )
}

.structify_expression <- function(expr) {
  unaliased_expr <- expr$meta$undo_aliases()
  if (unaliased_expr$meta$has_multiple_outputs()) {
    expr_name <- expr$meta$output_name(raise_if_undetermined = FALSE)
    if (is_na(expr_name)) {
      pl$struct(expr)
    } else {
      pl$struct(unaliased_expr)$alias(expr_name)
    }
  } else {
    expr
  }
}

#' Parse dynamic dots into a single expression (PlRExpr, not polars-expr)
#' @noRd
# nolint start: object_length_linter
parse_predicates_constraints_into_expression <- function(...) {
  check_dots_unnamed()

  expr <- list2(...) |>
    lapply(as_polars_expr) |>
    Reduce(`&`, x = _)

  expr$`_rexpr`
}
# nolint end

#' @export
`$.polars_expr` <- function(x, name) {
  member_names <- ls(x, all.names = TRUE)
  method_names <- names(polars_expr__methods)

  if (name %in% member_names) {
    env_get(x, name)
  } else if (name %in% method_names) {
    fn <- polars_expr__methods[[name]]
    self <- x
    environment(fn) <- environment()
    fn
  } else {
    NextMethod()
  }
}

#' @exportS3Method utils::.DollarNames
.DollarNames.polars_expr <- function(x, pattern = "") {
  member_names <- ls(x, all.names = TRUE)
  method_names <- names(polars_expr__methods)

  all_names <- union(member_names, method_names)
  filtered_names <- findMatches(pattern, all_names)

  filtered_names[!startsWith(filtered_names, "_")]
}

#' @export
print.polars_expr <- function(x, ...) {
  x$`_rexpr`$as_str() |>
    writeLines()
  invisible(x)
}

#' @export
`[.polars_struct_namespace` <- function(x, i, ...) {
  if (is.numeric(i)) {
    x$field_by_index(i)
  } else if (is.character(i)) {
    x$field(i)
  } else {
    abort(sprintf("expected type numeric or character for `i`, got %s", typeof(i)))
  }
}

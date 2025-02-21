#' @export
`$.polars_expr` <- function(x, name) {
  member_names <- ls(x)
  method_names <- names(polars_expr__methods)

  if (name %in% member_names) {
    get(name, envir = x)
  } else if (name %in% method_names) {
    fn <- polars_expr__methods[[name]]
    self <- x
    environment(fn) <- environment()
    fn
  } else {
    NextMethod()
  }
}

#' @export
`.DollarNames.polars_expr` <- function(x, pattern = "") {
  member_names <- ls(x)
  method_names <- names(polars_expr__methods)

  all_names <- union(member_names, method_names)
  filtered_names <- all_names[grep(pattern, all_names)]

  filtered_names[!startsWith(filtered_names, "_")]
}

#' @export
print.polars_expr <- function(x, ...) {
  x$`_rexpr`$print()
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

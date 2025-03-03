#' @export
`$.polars_dtype` <- function(x, name) {
  member_names <- ls(x, all.names = TRUE)
  method_names <- names(polars_datatype__methods)

  if (name %in% member_names) {
    env_get(x, name)
  } else if (name %in% method_names) {
    fn <- polars_datatype__methods[[name]]
    self <- x
    environment(fn) <- environment()
    fn
  } else {
    NextMethod()
  }
}

#' @exportS3Method utils::.DollarNames
.DollarNames.polars_dtype <- function(x, pattern = "") {
  member_names <- ls(x, all.names = TRUE)
  method_names <- names(polars_datatype__methods)

  all_names <- union(member_names, method_names)
  filtered_names <- findMatches(pattern, all_names)

  filtered_names[!startsWith(filtered_names, "_")]
}

#' @export
`$.polars_dtype_enum` <- function(x, name) {
  # Enum only method `union`
  if (identical(name, "union")) {
    fn <- function(other) {
      wrap({
        check_polars_dtype(other)
        if (!inherits(other, "polars_dtype_enum")) {
          abort("`other` must be a Enum data type")
        }

        PlRDataType$new_enum(unique(c(self$categories, other$categories)))
      })
    }
    self <- x
    environment(fn) <- environment()
    fn
  } else {
    NextMethod()
  }
}

#' @exportS3Method utils::.DollarNames
.DollarNames.polars_dtype_enum <- function(x, pattern = "") {
  member_names <- ls(x, all.names = TRUE)
  # Enum only method `union`
  method_names <- c("union", names(polars_datatype__methods))

  all_names <- union(member_names, method_names)
  filtered_names <- findMatches(pattern, all_names)

  filtered_names[!startsWith(filtered_names, "_")]
}

#' @export
print.polars_dtype <- function(x, ...) {
  x$`_dt`$as_str() |>
    writeLines()
  invisible(x)
}

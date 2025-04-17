#' Get and reset polars options
#'
#' @description `polars_options()` returns a list of options for polars. Options
#' can be set with [`options()`]. Note that **options must be prefixed with
#' "polars."**, e.g to modify the option `strictly_immutable` you need to pass
#' `options(polars.strictly_immutable =)`. See below for a description of all
#' options.
#'
#' `polars_options_reset()` brings all polars options back to their default
#' value.
#'
#' @details The following options are available (in alphabetical order, with the
#'   default value in parenthesis):
#'
#' * `int64_conversion` (`"double"`): How should Int64 values be handled when
#'   converting a polars object to R?
#'    * `"double"` converts the integer values to double.
#'    * `"bit64"` uses `bit64::as.integer64()` to do the conversion (requires
#'   the package `bit64` to be attached).
#'    * `"character"` converts Int64 values to character.
#' * `df_knitr_print` (TODO: possible values??)
#'
#' @return
#' `polars_options()` returns a named list where the names are option names and
#' values are option values.
#'
#' `polars_options_reset()` doesn't return anything.
#'
#' @export
#' @examplesIf requireNamespace("withr", quietly = TRUE)
#' polars_options()
#' withr::with_options(
#'   list(polars.int64_conversion = "character"),
#'   polars_options()
#' )
polars_options <- function() {
  out <- list(
    df_knitr_print = getOption("polars.df_knitr_print") %||% "auto",
    int64_conversion = getOption("polars.int64_conversion") %||% "double"
  )

  arg_match0(out[["df_knitr_print"]], c("auto"), arg_nm = "df_knitr_print") # TODO: complete possible values
  arg_match0(
    out[["int64_conversion"]],
    c("double", "character", "integer", "integer64"),
    arg_nm = "int64_conversion"
  )
  if (out[["int64_conversion"]] == "integer64" && !"bit64" %in% .packages()) {
    abort(r"(package `bit64` must be attached to use `int64_conversion = "integer64"`.)")
  }
  structure(out, class = "polars_options")
}

#' @rdname polars_options
#' @export
polars_options_reset <- function() {
  options(
    list(
      polars.df_knitr_print = "auto",
      polars.int64_conversion = "double"
    )
  )
}

#' @noRd
#' @export
print.polars_options <- function(x, ...) {
  # Copied from the arrow package
  # https://github.com/apache/arrow/blob/6f3bd2524c2abe3a4a278fc1c62fc5c49b56cab3/r/R/arrow-info.R#L149-L157
  print_key_values <- function(title, vals, ...) {
    df <- data.frame(vals, ...)
    names(df) = ""

    cat(title, ":\n========", sep = "")
    print(df)
    cat("\nSee `?polars_options` for the definition of all options.")
  }

  print_key_values("Options", unlist(x))
}
# TODO: add options and functions about global string cache

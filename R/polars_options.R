#' Get and reset polars options
#'
#' @description
#' `polars_options()` returns a list of options for polars. Options
#' can be set with [`options()`]. Note that **options must be prefixed with
#' "polars."**, e.g to modify the option `conversion_int64` you need to pass
#' `options(polars.conversion_int64 =)`. See below for a description of all
#' options.
#'
#' `polars_options_reset()` brings all polars options back to their default
#' value.
#'
#' @details The following options are available (in alphabetical order, with the
#'   default value in parenthesis):
#'
#' * `conversion_int64` (`"double"`): How should Int64 values be handled when
#'   converting a polars object to R?
#'    * `"double"` converts the integer values to double.
#'    * `"integer"` converts to the R's [integer] type. If the value is out of
#'      the range of R's integer type, export as [NA_integer_].
#'    * `"integer64"` uses `bit64::as.integer64()` to do the conversion
#'      (requires the package `bit64` to be attached).
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
#'   list(polars.conversion_int64 = "character"),
#'   polars_options()
#' )
polars_options <- function() {
  out <- list(
    df_knitr_print = getOption("polars.df_knitr_print") %||% "auto",
    conversion_int64 = getOption("polars.conversion_int64") %||% "double"
  )

  arg_match0(out[["df_knitr_print"]], c("auto"), arg_nm = "df_knitr_print") # TODO: complete possible values
  arg_match0(
    out[["conversion_int64"]],
    c("double", "character", "integer", "integer64"),
    arg_nm = "conversion_int64"
  )
  if (out[["conversion_int64"]] == "integer64" && !"bit64" %in% .packages()) {
    abort(r"(package `bit64` must be attached to use `conversion_int64 = "integer64"`.)")
  }
  structure(out, class = "polars_options")
}

#' @rdname polars_options
#' @export
polars_options_reset <- function() {
  options(
    list(
      polars.df_knitr_print = "auto",
      polars.conversion_int64 = "double"
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

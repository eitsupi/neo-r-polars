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
#' * for all `conversion_*` options, see arguments of [to_r_vector()][series__to_r_vector].
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
    conversion_int64 = getOption("polars.conversion_int64") %||% "double",
    conversion_uint8 = getOption("polars.conversion_uint8") %||% "integer",
    conversion_date = getOption("polars.conversion_date") %||% "Date",
    conversion_time = getOption("polars.conversion_time") %||% "hms",
    conversion_decimal = getOption("polars.conversion_decimal") %||% "double",
    conversion_ambiguous = getOption("polars.conversion_ambiguous") %||% "raise",
    conversion_non_existent = getOption("polars.conversion_non_existent") %||% "raise"
  )

  arg_match0(out[["df_knitr_print"]], c("auto"), arg_nm = "df_knitr_print") # TODO: complete possible values
  arg_match0(
    out[["conversion_int64"]],
    c("double", "character", "integer", "integer64"),
    arg_nm = "conversion_int64"
  )
  arg_match0(out[["conversion_uint8"]], c("integer", "raw"), arg_nm = "conversion_uint8")
  arg_match0(out[["conversion_date"]], c("Date", "IDate"), arg_nm = "conversion_date")
  arg_match0(out[["conversion_time"]], c("hms", "ITime"), arg_nm = "conversion_time")
  arg_match0(out[["conversion_decimal"]], c("double", "character"), arg_nm = "conversion_decimal")
  arg_match0(
    out[["conversion_ambiguous"]],
    c("raise", "earliest", "latest", "null"),
    arg_nm = "conversion_ambiguous"
  )
  arg_match0(
    out[["conversion_non_existent"]],
    c("raise", "null"),
    arg_nm = "conversion_non_existent"
  )
  check_option_required_package(out[["conversion_int64"]], "conversion_int64", "integer64", "bit64")
  check_option_required_package(out[["conversion_date"]], "conversion_date", "IDate", "data.table")
  check_option_required_package(out[["conversion_time"]], "conversion_time", "hms", "hms")
  check_option_required_package(out[["conversion_time"]], "conversion_time", "ITime", "data.table")
  structure(out, class = "polars_options")
}

#' @rdname polars_options
#' @export
polars_options_reset <- function() {
  options(
    list(
      polars.df_knitr_print = "auto",
      polars.conversion_int64 = "double",
      polars.conversion_uint8 = "integer",
      polars.conversion_date = "Date",
      polars.conversion_time = "hms",
      polars.conversion_decimal = "double",
      polars.conversion_ambiguous = "raise",
      polars.conversion_non_existent = "raise"
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

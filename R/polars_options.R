#' Get and reset polars options
#'
#' @description
#' `polars_options()` returns a list of options for polars. Options
#' can be set with [`options()`]. Note that **options must be prefixed with
#' "polars."**, e.g to modify the option `to_r_vector_int64` you need to pass
#' `options(polars.to_r_vector_int64 =)`. See below for a description of all
#' options.
#'
#' `polars_options_reset()` brings all polars options back to their default
#' value.
#'
#' @details The following options are available (in alphabetical order, with the
#'   default value in parenthesis):
#'
#' * for all `to_r_vector_*` options, see arguments of [to_r_vector()][series__to_r_vector].
#' * `df_knitr_print` (TODO: possible values??)
#'
#' @return
#' `polars_options()` returns a named list where the names are option names and
#' values are option values.
#'
#' `polars_options_reset()` doesn't return anything.
#'
#' @export
#' @examplesIf requireNamespace("withr", quietly = TRUE) && requireNamespace("hms", quietly = TRUE)
#' library(hms)
#' polars_options()
#' withr::with_options(
#'   list(polars.to_r_vector_int64 = "character"),
#'   polars_options()
#' )
polars_options <- function() {
  out <- list(
    df_knitr_print = getOption("polars.df_knitr_print") %||% "auto",
    to_r_vector_int64 = getOption("polars.to_r_vector_int64") %||% "double",
    to_r_vector_uint8 = getOption("polars.to_r_vector_uint8") %||% "integer",
    to_r_vector_date = getOption("polars.to_r_vector_date") %||% "Date",
    to_r_vector_time = getOption("polars.to_r_vector_time") %||% "hms",
    to_r_vector_decimal = getOption("polars.to_r_vector_decimal") %||% "double",
    to_r_vector_ambiguous = getOption("polars.to_r_vector_ambiguous") %||% "raise",
    to_r_vector_non_existent = getOption("polars.to_r_vector_non_existent") %||% "raise"
  )

  arg_match0(out[["df_knitr_print"]], c("auto"), arg_nm = "df_knitr_print") # TODO: complete possible values
  arg_match0(
    out[["to_r_vector_int64"]],
    c("double", "character", "integer", "integer64"),
    arg_nm = "to_r_vector_int64"
  )
  arg_match0(out[["to_r_vector_uint8"]], c("integer", "raw"), arg_nm = "to_r_vector_uint8")
  arg_match0(out[["to_r_vector_date"]], c("Date", "IDate"), arg_nm = "to_r_vector_date")
  arg_match0(out[["to_r_vector_time"]], c("hms", "ITime"), arg_nm = "to_r_vector_time")
  arg_match0(out[["to_r_vector_decimal"]], c("double", "character"), arg_nm = "to_r_vector_decimal")
  arg_match0(
    out[["to_r_vector_ambiguous"]],
    c("raise", "earliest", "latest", "null"),
    arg_nm = "to_r_vector_ambiguous"
  )
  arg_match0(
    out[["to_r_vector_non_existent"]],
    c("raise", "null"),
    arg_nm = "to_r_vector_non_existent"
  )
  check_option_required_package(
    out[["to_r_vector_int64"]],
    "to_r_vector_int64",
    "integer64",
    "bit64"
  )
  check_option_required_package(
    out[["to_r_vector_date"]],
    "to_r_vector_date",
    "IDate",
    "data.table"
  )
  check_option_required_package(out[["to_r_vector_time"]], "to_r_vector_time", "hms", "hms")
  check_option_required_package(
    out[["to_r_vector_time"]],
    "to_r_vector_time",
    "ITime",
    "data.table"
  )
  structure(out, class = "polars_options")
}

#' @rdname polars_options
#' @export
polars_options_reset <- function() {
  options(
    list(
      polars.df_knitr_print = "auto",
      polars.to_r_vector_int64 = "double",
      polars.to_r_vector_uint8 = "integer",
      polars.to_r_vector_date = "Date",
      polars.to_r_vector_time = "hms",
      polars.to_r_vector_decimal = "double",
      polars.to_r_vector_ambiguous = "raise",
      polars.to_r_vector_non_existent = "raise"
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

#' @param x Argument passed in calling function, e.g. `int64`.
#' @param is_missing Is `x` missing in the calling function?
#' @param default The default of `x` in the calling function
#' @noRd
use_option_if_missing <- function(x, is_missing, default) {
  nm <- deparse(substitute(x))
  if (is_missing) {
    x <- getOption(paste0("polars.to_r_vector_", nm), default)
    if (!identical(x, default)) {
      inform(
        sprintf(
          '`%s` is overridden by the option "%s" with %s',
          nm,
          paste0("polars.to_r_vector_", nm),
          rlang:::obj_type_friendly(x)
        ),
      )
    }
  }
  x
}

#' @param x Option value set by user.
#' @param option_name Name of the option set by user, e.g. `"to_r_vector_int64"`.
#' @param option_value Option value that requires checking for package presence,
#' e.g. `"integer64"`
#' @param pkg Name of package that must be present to use this `option_value`,
#' e.g. `"bit64".`
#' @noRd
check_option_required_package <- function(x, option_name, option_value, pkg) {
  if (x == option_value && !pkg %in% .packages()) {
    abort(
      paste0(
        "package `",
        pkg,
        "` must be attached to use `",
        option_name,
        " = \"",
        option_value,
        "\"`."
      ),
      call = caller_env()
    )
  }
}

# TODO: add options and functions about global string cache

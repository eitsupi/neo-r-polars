#' Get and reset polars options
#'
#' @description
#' `polars_options()` returns a list of options for polars. Options
#' can be set with [`options()`]. Note that **options must be prefixed with
#' "polars."**, e.g to modify the option `from_int64` you need to pass
#' `options(polars.from_int64 =)`. See below for a description of all
#' options.
#'
#' `polars_options_reset()` brings all polars options back to their default
#' value.
#'
#' @details The following options are available (in alphabetical order, with the
#'   default value in parenthesis):
#'
#' * for all `from_*` options, see arguments of [to_r_vector()][series__to_r_vector].
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
#'   list(polars.from_int64 = "character"),
#'   polars_options()
#' )
polars_options <- function() {
  out <- list(
    df_knitr_print = getOption("polars.df_knitr_print") %||% "auto",
    from_int64 = getOption("polars.from_int64") %||% "double",
    from_uint8 = getOption("polars.from_uint8") %||% "integer",
    from_date = getOption("polars.from_date") %||% "Date",
    from_time = getOption("polars.from_time") %||% "hms",
    from_decimal = getOption("polars.from_decimal") %||% "double",
    from_ambiguous = getOption("polars.from_ambiguous") %||% "raise",
    from_non_existent = getOption("polars.from_non_existent") %||% "raise"
  )

  # TODO: complete possible values
  arg_match0(out[["df_knitr_print"]], c("auto"), arg_nm = "df_knitr_print")
  arg_match0(
    out[["from_int64"]],
    c("double", "character", "integer", "integer64"),
    arg_nm = "from_int64"
  )
  arg_match0(out[["from_uint8"]], c("integer", "raw"), arg_nm = "from_uint8")
  arg_match0(out[["from_date"]], c("Date", "IDate"), arg_nm = "from_date")
  arg_match0(out[["from_time"]], c("hms", "ITime"), arg_nm = "from_time")
  arg_match0(out[["from_decimal"]], c("double", "character"), arg_nm = "from_decimal")
  arg_match0(
    out[["from_ambiguous"]],
    c("raise", "earliest", "latest", "null"),
    arg_nm = "from_ambiguous"
  )
  arg_match0(
    out[["from_non_existent"]],
    c("raise", "null"),
    arg_nm = "from_non_existent"
  )
  structure(out, class = "polars_options")
}

#' @rdname polars_options
#' @export
polars_options_reset <- function() {
  options(
    list(
      polars.df_knitr_print = "auto",
      polars.from_int64 = "double",
      polars.from_uint8 = "integer",
      polars.from_date = "Date",
      polars.from_time = "hms",
      polars.from_decimal = "double",
      polars.from_ambiguous = "raise",
      polars.from_non_existent = "raise"
    )
  )
}

#' @noRd
#' @export
print.polars_options <- function(x, ...) {
  # Copied from the arrow package
  # nolint https://github.com/apache/arrow/blob/6f3bd2524c2abe3a4a278fc1c62fc5c49b56cab3/r/R/arrow-info.R#L149-L157
  print_key_values <- function(title, vals, ...) {
    df <- data.frame(vals, ...)
    names(df) <- ""

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
    x <- getOption(paste0("polars.from_", nm), default)
    if (!identical(x, default)) {
      inform(
        sprintf(
          '`%s` is overridden by the option "%s" with %s',
          nm,
          paste0("polars.from_", nm),
          rlang:::obj_type_friendly(x)
        ),
      )
    }
  }
  x
}

# TODO: add options and functions about global string cache

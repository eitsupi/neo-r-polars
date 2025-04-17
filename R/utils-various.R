#' @noRd
extend_bool <- function(value, n_match, value_name, match_name) {
  check_logical(value, arg = value_name, call = caller_env())

  if (length(value) == 1L) {
    rep_len(value, n_match)
  } else if (length(value) != n_match) {
    abort(
      # TODO: error message improvement
      sprintf(
        "the length of `%s` (%d) does not match the length of `%s` (%d)",
        value_name,
        length(value),
        match_name,
        n_match
      )
    )
  } else {
    value
  }
}

#' @param x Argument passed in calling function, e.g. `int64`.
#' @param is_missing Is `x` missing in the calling function?
#' @param default The default of `x` in the calling function
#' @noRd
use_option_if_missing <- function(x, is_missing, default) {
  nm <- deparse(substitute(x))
  if (is_missing) {
    x <- getOption(paste0("polars.conversion_", nm), default)
    if (x != default) {
      inform(paste0("Using `", nm, " = \"", x, "\"`."))
    }
  }
  x
}

#' @param x Option value set by user.
#' @param option_name Name of the option set by user, e.g. `"conversion_int64"`.
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

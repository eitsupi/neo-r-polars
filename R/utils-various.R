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

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
        value_name, length(value), match_name, n_match
      )
    )
  } else {
    value
  }
}

#' Used in ewm_* functions
#' @noRd
prepare_alpha <- function(com, span, half_life, alpha) {
  check_exclusive(com, span, half_life, alpha, .call = caller_env())

  if (!missing(com)) {
    check_number_decimal(com, min = 0)
    alpha <- 1 / (1 + com)
  } else if (!missing(span)) {
    check_number_decimal(span, min = 1)
    alpha <- 2 / (span + 1)
  } else if (!missing(half_life)) {
    check_number_decimal(half_life, min = 0)
    alpha <- 1 - exp(-log(2) / half_life)
  } else if (!missing(alpha)) {
    # Can't use "min" arg in check_number_decimal() since requirement is > 0
    check_number_decimal(alpha)
    if (alpha <= 0 || alpha > 1) {
      abort("`alpha` must be between greater than 0 and lower or equal to 1.", call = caller_env())
    }
  }

  alpha
}

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
    if (com < 0) {
      abort("`com` must be >= 0.", call = caller_env())
    }
    alpha <- 1 / (1 + com)
  } else if (!missing(span)) {
    if (span < 1) {
      abort("`span` must be >= 1.", call = caller_env())
    }
    alpha <- 2 / (span + 1)
  } else if (!missing(half_life)) {
    if (half_life < 0) {
      abort("`half_life` must be >= 0.", call = caller_env())
    }
    alpha <- 1 - exp(-log(2) / half_life)
  } else if (!missing(alpha)) {
    if (alpha <= 0 || alpha > 1) {
      abort("`half_life` must be between greater than 0 and lower or equal to 1.", call = caller_env())
    }
  }

  alpha
}

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

#' @noRd
check_named_list <- function(
    x,
    ...,
    allow_null = FALSE,
    arg = caller_arg(x),
    call = caller_env()) {
  if (is.null(x) && allow_null) {
    return(invisible(NULL))
  }
  if (!is.list(x) || is.null(names(x)) || any(names(x) == "")) {
    what <- "a named list"
  } else {
    return(invisible(NULL))
  }
  stop_input_type(
    x,
    what,
    ...,
    arg = arg,
    call = call
  )
}

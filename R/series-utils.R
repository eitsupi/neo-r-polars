MUST_RETURN_SCALAR <- c("max", "min", "sum", "median", "mean")

expr_wrap_function_factory <- function(fn, self, namespace = NULL, name = NULL) {
  `_s` <- self$`_s`
  environment(fn) <- environment()

  # Override `self` with the column expression or namespace
  if (is.null(namespace)) {
    self <- pl$col(`_s`$name())
  } else {
    self <- pl$col(`_s`$name())[[namespace]]
  }

  new_fn <- function() {
    wrap({
      expr <- do.call(fn, as.list(match.call()[-1]), envir = parent.frame())
      out <- wrap(`_s`)$to_frame()$select(expr)$to_series()
      if (!is.null(name) && name %in% MUST_RETURN_SCALAR) {
        out <- out$to_r_vector()
      }
      out
    })
  }

  formals(new_fn) <- formals(fn)

  new_fn
}

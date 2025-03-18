# The env for storing rolling_group_by methods
polars_sql_context__methods <- new.env(parent = emptyenv())

wrap_to_sql_context <- function(
  frames = NULL,
  register_globals = FALSE
) {
  self <- new.env(parent = emptyenv())
  self$`_ctxt` <- PlRSQLContext |>
    .savvy_wrap_PlRSQLContext()
  self$frames <- frames
  self$register_globals <- register_globals

  class(self) <- c("polars_sql_context", "polars_object")
  self
}

pl__SQLContext <- function(frames = NULL, register_globals = FALSE) {
  wrap_to_sql_context(frames = frames, register_globals = register_globals)
}

ensure_lazyframe <- function(obj) {
  if (is_polars_df(obj)) {
    obj$lazy()
  } else if (is_polars_lf(obj)) {
    obj
  } else if (is_convertible_to_polars_series(obj)) {
    as_polars_series(obj)$to_frame()$lazy()
  } else {
    abort("Cannot convert object to LazyFrame")
  }
}

sql_context__register <- function(name, frame = NULL) {
  wrap({
    frame <- if (!is.null(frame)) {
      ensure_lazyframe(frame)
    } else {
      pl$LazyFrame()
    }
    self$`_ctxt`$register(name, frame$`_ldf`)
    self
  })
}

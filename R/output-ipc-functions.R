#' Evaluate the query in streaming mode and write to an Arrow IPC file
#'
#' @inherit lazyframe__sink_parquet description params return
#' @inheritParams rlang::args_dots_empty
#' @param compression Determines the compression algorithm.
#' Must be one of:
#' - `"uncompressed"` or `NULL`: Write an uncompressed Arrow file.
#' - `"lz4"`: Fast compression/decompression.
#' - `"zstd"` (default): Good compression performance.
#'
#' @examples
#' tmpf <- tempfile(fileext = ".arrow")
#' as_polars_lf(mtcars)$sink_ipc(tmpf)
#' pl$scan_ipc(tmpf)$collect()
lazyframe__sink_ipc <- function(
  path,
  ...,
  compression = c("zstd", "lz4", "uncompressed"),
  maintain_order = TRUE,
  type_coercion = TRUE,
  `_type_check` = TRUE,
  predicate_pushdown = TRUE,
  projection_pushdown = TRUE,
  simplify_expression = TRUE,
  slice_pushdown = TRUE,
  collapse_joins = TRUE,
  no_optimization = FALSE,
  storage_options = NULL,
  retries = 2
) {
  wrap({
    check_dots_empty0(...)
    compression <- arg_match0(
      compression %||% "uncompressed",
      values = c("zstd", "lz4", "uncompressed")
    )

    lf <- set_sink_optimizations(
      self,
      type_coercion = type_coercion,
      `_type_check` = `_type_check`,
      predicate_pushdown = predicate_pushdown,
      projection_pushdown = projection_pushdown,
      simplify_expression = simplify_expression,
      slice_pushdown = slice_pushdown,
      collapse_joins = collapse_joins,
      no_optimization = no_optimization
    )

    lf$sink_ipc(
      path = path,
      compression = compression,
      maintain_order = maintain_order,
      storage_options = storage_options,
      retries = retries
    )

    invisible(self)
  })
}

#' Write to Arrow IPC binary stream or Feather file.
#'
#' @inherit lazyframe__sink_parquet description params return
#' @inheritParams rlang::args_dots_empty
#' @inheritParams lazyframe__sink_ipc
#'
#' @param compat_level Use a specific compatibility level when exporting
#' Polars' internal data structures.
#'
#' @inherit write_csv return
#' @examples
#' tmpf <- tempfile()
#' as_polars_df(mtcars)$write_ipc(tmpf)
#' pl$read_ipc(tmpf)
dataframe__write_ipc <- function(
  path,
  ...,
  compression = c("zstd", "lz4", "uncompressed"),
  compat_level = c("newest", "oldest"),
  storage_options = NULL,
  retries = 2
) {
  wrap({
    check_dots_empty0(...)
    compression <- arg_match0(
      compression %||% "uncompressed",
      values = c("zstd", "lz4", "uncompressed")
    )
    compat_level <- arg_match0(compat_level, values = c("newest", "oldest"))

    self$`_df`$write_ipc(
      path = path,
      compression = compression,
      compat_level = compat_level,
      storage_options = storage_options,
      retries = retries
    )

    invisible(self)
  })
}

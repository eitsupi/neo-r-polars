# TODO: add write_ipc

#' Evaluate the query in streaming mode and write to an IPC file
#'
#' @inherit lazyframe__sink_parquet description params return
#' @inheritParams rlang::args_dots_empty
#' @param compression Must be one of:
#' * `"lz4"`: fast compression/decompression.
#' * `"zstd"`: good compression performance.
#'
#' If `NULL`, it uses `"zstd"`.
#'
#' @examples
#' tmpf <- tempfile()
#' as_polars_lf(mtcars)$sink_ipc(tmpf)
#' pl$scan_ipc(tmpf)$collect()
lazyframe__sink_ipc <- function(
  path,
  ...,
  compression = c("uncompressed", "zstd", "lz4"),
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
    compression <- compression %||% "uncompressed"
    compression <- arg_match0(compression, values = c("uncompressed", "lz4", "zstd"))

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

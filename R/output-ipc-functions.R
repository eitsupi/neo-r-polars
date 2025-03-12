# TODO: add write_ipc and sink_ipc

#' Write to Arrow IPC binary stream or Feather file.
#'
#' @inherit lazyframe__sink_parquet description params return
#' @inheritParams rlang::args_dots_empty
#' @param compression Must be one of:
#' * `"lz4"`: fast compression/decompression.
#' * `"zstd"`: good compression performance.
#'
#' If `NULL`, it uses `"zstd"`.
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
  compression = c("zstd", "lz4"),
  compat_level = c("newest", "oldest"),
  storage_options = NULL,
  retries = 2
) {
  wrap({
    check_dots_empty0(...)
    compression <- compression %||% "zstd"
    compression <- arg_match0(compression, values = c("lz4", "zstd"))
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

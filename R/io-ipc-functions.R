#' Lazily read from an Arrow IPC (Feather v2) file or multiple files via glob
#' patterns
#'
#' This allows the query optimizer to push down predicates and projections to
#' the scan level, thereby potentially reducing memory overhead.
#'
#' @inheritParams rlang::args_dots_empty
#' @inheritParams pl_scan_parquet
#' @param columns Columns to select. Accepts a vector of column indices
#' (starting at zero) or a vector of column names.
#' @param memory_map A logical. If `TRUE`, try to memory map the file. This can
#' greatly improve performance on repeated queries as the OS may cache pages.
#' Only uncompressed Arrow IPC files can be memory mapped.
#' @param hive_partitioning Infer statistics and schema from Hive partitioned
#' sources and use them to prune reads. If `NULL` (default), it is automatically
#' enabled when a single directory is passed, and otherwise disabled.
#' @param hive_schema A list containing the column names and data types of the
#' columns by which the data is partitioned, e.g.
#' `list(a = pl$String, b = pl$Float32)`. If `NULL` (default), the schema of
#' the Hive partitions is inferred.
#' @param try_parse_hive_dates Whether to try parsing hive values as date /
#' datetime types.
#' @param include_file_paths Character value indicating the column name that
#' will include the path of the source file(s).
#'
#' @details
#' If `memory_map` is set, the bytes on disk are mapped 1:1 to memory. That
#' means that you cannot write to the same filename, e.g.
#' `pl$read_ipc("my_file.arrow")$write_ipc("my_file.arrow")` will fail.
#'
#' @rdname IO_scan_ipc
#' @examplesIf requireNamespace("arrow", quietly = TRUE) && arrow::arrow_with_dataset()
#' temp_dir <- tempfile()
#' # Write a hive-style partitioned arrow file dataset
#' arrow::write_dataset(
#'   mtcars,
#'   temp_dir,
#'   partitioning = c("cyl", "gear"),
#'   format = "arrow",
#'   hive_style = TRUE
#' )
#' list.files(temp_dir, recursive = TRUE)
#'
#' # If the path is a folder, Polars automatically tries to detect partitions
#' # and includes them in the output
#' pl$scan_ipc(temp_dir)$collect()
#'
#' # We can also impose a schema to the partition
#' pl$scan_ipc(temp_dir, hive_schema = list(cyl = pl$String, gear = pl$Int32))$collect()
pl__scan_ipc <- function(
    source,
    ...,
    columns = NULL,
    n_rows = NULL,
    cache = TRUE,
    rechunk = FALSE,
    row_index_name = NULL,
    row_index_offset = 0L,
    storage_options = NULL,
    memory_map = TRUE,
    retries = 2,
    file_cache_ttl = NULL,
    hive_partitioning = NULL,
    hive_schema = NULL,
    try_parse_hive_dates = TRUE,
    include_file_paths = NULL) {
  check_dots_empty0(...)
  if (!is_null(columns) && !is_character(columns) && !is_integerish(columns)) {
    abort("`columns` must be a character vector, an integerish vector, or `NULL`.")
  }
  if (length(hive_schema) > 0) {
    hive_schema <- parse_into_list_of_datatypes(!!!hive_schema)
  }
  out <- PlRLazyFrame$new_from_ipc(
    path = source,
    n_rows = n_rows,
    cache = cache,
    rechunk = rechunk,
    retries = retries,
    file_cache_ttl = file_cache_ttl,
    cloud_options = storage_options,
    row_index_name = row_index_name,
    row_index_offset = row_index_offset,
    hive_partitioning = hive_partitioning,
    hive_schema = hive_schema,
    try_parse_hive_dates = try_parse_hive_dates,
    include_file_paths = include_file_paths
  ) |>
    wrap()
  if (!is_null(columns)) {
    if (is_integerish(columns)) {
      out <- out$select(pl$nth(columns))
    } else if (is_character(columns)) {
      out <- out$select(columns)
    }
  }
  out
}


#' Read into a DataFrame from Arrow IPC (Feather v2) file
#'
#' @inherit pl_read_csv return
#' @inheritParams pl_scan_ipc
#' @rdname IO_read_ipc
#' @examplesIf requireNamespace("arrow", quietly = TRUE) && arrow::arrow_with_dataset()
#' temp_dir <- tempfile()
#' # Write a hive-style partitioned arrow file dataset
#' arrow::write_dataset(
#'   mtcars,
#'   temp_dir,
#'   partitioning = c("cyl", "gear"),
#'   format = "arrow",
#'   hive_style = TRUE
#' )
#' list.files(temp_dir, recursive = TRUE)
#'
#' # If the path is a folder, Polars automatically tries to detect partitions
#' # and includes them in the output
#' pl$read_ipc(temp_dir)
#'
#' # We can also impose a schema to the partition
#' pl$read_ipc(temp_dir, hive_schema = list(cyl = pl$String, gear = pl$Int32))
pl__read_ipc <- function(
    source,
    ...,
    columns = NULL,
    n_rows = NULL,
    cache = TRUE,
    rechunk = FALSE,
    row_index_name = NULL,
    row_index_offset = 0L,
    storage_options = NULL,
    memory_map = TRUE,
    retries = 2,
    file_cache_ttl = NULL,
    hive_partitioning = NULL,
    hive_schema = NULL,
    try_parse_hive_dates = TRUE,
    include_file_paths = NULL) {
  wrap({
    .args <- as.list(environment())
    do.call(pl__scan_ipc, .args)$collect()
  })
}

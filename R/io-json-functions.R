#' Lazily read from a local or cloud-hosted ndjson file (or files)
#'
#' @inherit pl__scan_ipc description
#'
#' @inherit pl__LazyFrame return
#' @inheritParams rlang::args_dots_empty
#' @param source Path(s) to a file or directory. When needing to authenticate
#' for scanning cloud locations, see the `storage_options` parameter.
#' @inheritParams pl__scan_parquet
#' @inheritParams pl__scan_csv
#'
#' @examplesIf requireNamespace("withr", quietly = TRUE)
#' # Write a Parquet file than we can then import as DataFrame
#' temp_file <- withr::local_tempfile(fileext = ".ndjson")
#' as_polars_df(mtcars)$write_ndjson(temp_file)
#'
#' pl$scan_ndjson(temp_file)$collect()
#'
#' # Write a hive-style partitioned ndjson dataset
#' temp_dir <- withr::local_tempdir()
#' as_polars_df(mtcars)$write_ndjson(temp_dir, partition_by = c("cyl", "gear"))
#' list.files(temp_dir, recursive = TRUE)
#'
#' # If the path is a folder, Polars automatically tries to detect partitions
#' # and includes them in the output
#' pl$scan_ndjson(temp_dir)$collect()
pl__scan_ndjson <- function(
    source,
    ...,
    schema = NULL,
    schema_overrides = NULL,
    infer_schema_length = 100,
    batch_size = 1024,
    n_rows = NULL,
    low_memory = FALSE,
    rechunk = FALSE,
    row_index_name = NULL,
    row_index_offset = 0L,
    ignore_errors = FALSE,
    storage_options = NULL,
    retries = 2,
    file_cache_ttl = NULL,
    include_file_paths = NULL) {
  check_dots_empty0(...)
  check_character(source, allow_na = FALSE)
  if (length(source) == 0) {
    abort("`source` must have length > 0.")
  }
  check_list_of_polars_dtype(schema, allow_null = TRUE)
  check_list_of_polars_dtype(schema_overrides, allow_null = TRUE)

  if (!is.null(schema)) {
    schema <- parse_into_list_of_datatypes(!!!schema)
  }
  if (!is.null(schema_overrides)) {
    schema_overrides <- parse_into_list_of_datatypes(!!!schema_overrides)
  }

  PlRLazyFrame$new_from_ndjson(
    source = source,
    schema = schema,
    schema_overrides = schema_overrides,
    infer_schema_length = infer_schema_length,
    batch_size = batch_size,
    n_rows = n_rows,
    low_memory = low_memory,
    rechunk = rechunk,
    row_index_name = row_index_name,
    row_index_offset = row_index_offset,
    ignore_errors = ignore_errors,
    storage_options = storage_options,
    retries = retries,
    file_cache_ttl = file_cache_ttl,
    include_file_paths = include_file_paths
  ) |>
    wrap()
}


#' Read into a DataFrame from Parquet file
#'
#' @inherit pl__DataFrame return
#' @inheritParams pl__scan_ndjson
#' @examplesIf requireNamespace("withr", quietly = TRUE)
#' # Write a Parquet file than we can then import as DataFrame
#' temp_file <- withr::local_tempfile(fileext = ".ndjson")
#' as_polars_df(mtcars)$write_ndjson(temp_file)
#'
#' pl$read_ndjson(temp_file)
#'
#' # Write a hive-style partitioned ndjson dataset
#' temp_dir <- withr::local_tempdir()
#' as_polars_df(mtcars)$write_ndjson(temp_dir, partition_by = c("cyl", "gear"))
#' list.files(temp_dir, recursive = TRUE)
#'
#' # If the path is a folder, Polars automatically tries to detect partitions
#' # and includes them in the output
#' pl$read_ndjson(temp_dir)
pl__read_ndjson <- function(
    source,
    ...,
    schema = NULL,
    schema_overrides = NULL,
    infer_schema_length = 100,
    batch_size = 1024,
    n_rows = NULL,
    low_memory = FALSE,
    rechunk = FALSE,
    row_index_name = NULL,
    row_index_offset = 0L,
    ignore_errors = FALSE,
    storage_options = NULL,
    retries = 2,
    file_cache_ttl = NULL,
    include_file_paths = NULL) {
  check_dots_empty0(...)
  .args <- as.list(environment())
  do.call(pl__scan_ndjson, .args)$collect() |>
    wrap()
}

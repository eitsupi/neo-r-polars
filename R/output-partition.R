# The env for storing the partitioning scheme methods
polars_partitioning_scheme__methods <- new.env(parent = emptyenv())

#' @export
wrap.PlRPartitioning <- function(x, ...) {
  self <- new.env(parent = emptyenv())
  self$`_r_partitioning` <- x

  makeActiveBinding("_base_path", function() self$`_r_partitioning`$base_path(), self)

  class(self) <- c("polars_partitioning_scheme", "polars_object")
  self
}

pl__PartitionMaxSize <- function(
  base_path,
  ...,
  max_size,
  per_partition_sort_by = NULL
) {
  wrap({
    check_dots_empty0(...)

    if (!is.null(per_partition_sort_by)) {
      per_partition_sort_by <- parse_into_list_of_expressions(!!!per_partition_sort_by)
    }

    PlRPartitioning$new_max_size(
      base_path = base_path,
      max_size = max_size,
      per_partition_sort_by = per_partition_sort_by
    )
  })
}

pl__PartitionByKey <- function(
  base_path,
  ...,
  by,
  include_key = TRUE,
  per_partition_sort_by = NULL
) {
  wrap({
    check_dots_empty0(...)

    by <- parse_into_list_of_expressions(!!!by)
    if (!is.null(per_partition_sort_by)) {
      per_partition_sort_by <- parse_into_list_of_expressions(!!!per_partition_sort_by)
    }

    PlRPartitioning$new_by_key(
      base_path = base_path,
      by = by,
      include_key = include_key,
      per_partition_sort_by = per_partition_sort_by
    )
  })
}

pl__PartitionParted <- function(
  base_path,
  ...,
  by,
  include_key = TRUE,
  per_partition_sort_by = NULL
) {
  wrap({
    check_dots_empty0(...)

    by <- parse_into_list_of_expressions(!!!by)
    if (!is.null(per_partition_sort_by)) {
      per_partition_sort_by <- parse_into_list_of_expressions(!!!per_partition_sort_by)
    }

    PlRPartitioning$new_parted(
      base_path = base_path,
      by = by,
      include_key = include_key,
      per_partition_sort_by = per_partition_sort_by
    )
  })
}

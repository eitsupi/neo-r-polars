% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/output-partition.R
\name{pl__PartitionByKey}
\alias{pl__PartitionByKey}
\alias{PartitionByKey}
\alias{pl__PartitionMaxSize}
\alias{PartitionMaxSize}
\alias{pl__PartitionParted}
\alias{PartitionParted}
\alias{polars_partitioning_scheme}
\title{Partitioning scheme to write files}
\usage{
pl__PartitionByKey(
  base_path,
  ...,
  by,
  include_key = TRUE,
  per_partition_sort_by = NULL
)

pl__PartitionMaxSize(base_path, ..., max_size, per_partition_sort_by = NULL)

pl__PartitionParted(
  base_path,
  ...,
  by,
  include_key = TRUE,
  per_partition_sort_by = NULL
)
}
\arguments{
\item{base_path}{The base path for the output files.
Use the \code{mkdir} option of the \verb{sink_*} methods to ensure directories
in the path are created.}

\item{...}{These dots are for future extensions and must be empty.}

\item{by}{Something can be coerced to a list of \link[=polars_expr]{expressions}.
Used to partition by.}

\item{include_key}{A bool indicating whether to include the key columns in the output files.}

\item{per_partition_sort_by}{Something can be coerced to a list of \link[=polars_expr]{expressions},
or \code{NULL} (default). Used  to sort over within each partition.
Note that this might increase the memory consumption needed for each partition.}

\item{max_size}{An integer-ish value indicating the maximum size in rows of
each of the generated files.}
}
\description{
\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#experimental}{\figure{lifecycle-experimental.svg}{options: alt='[Experimental]'}}}{\strong{[Experimental]}}

Partitioning schemes are used to write multiple files with \verb{sink_*} methods.
\itemize{
\item \code{\link[=polars_partitioning_scheme]{pl$PartitionByKey()}}: Split by the values of keys.
The amount of files that can be written is not limited. However,
when writing beyond a certain amount of files, the data for the remaining partitions
is buffered before writing to the file.
\item \code{\link[=polars_partitioning_scheme]{pl$PartitionMaxSize()}}: Split with a maximum size.
If the size reaches the maximum size, it is closed and a new file is opened.
\item \code{\link[=polars_partitioning_scheme]{pl$PartitionParted()}}: This is a specialized version of
\code{\link[=polars_partitioning_scheme]{pl$PartitionByKey()}}.
Whereas \code{\link[=polars_partitioning_scheme]{pl$PartitionByKey()}} accepts data in any order,
this scheme expects the input data to be pre-grouped or pre-sorted.
This scheme suffers a lot less overhead, but may not be always applicable.
Each new value of the key expressions starts a new partition,
therefore repeating the same value multiple times may overwrite previous partitions.
}
}
\examples{
\dontshow{if (requireNamespace("withr", quietly = TRUE)) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}
# Partitioning by columns
temp_dir_1 <- withr::local_tempdir()
as_polars_lf(mtcars)$sink_parquet(
  pl$PartitionByKey(
    temp_dir_1,
    by = c("cyl", "am"),
    include_key = FALSE,
  ),
  mkdir = TRUE
)
list.files(temp_dir_1, recursive = TRUE)

# Partitioning by max row size
temp_dir_2 <- withr::local_tempdir()
as_polars_lf(mtcars)$sink_csv(
  pl$PartitionMaxSize(
    temp_dir_2,
    max_size = 10,
  ),
  mkdir = TRUE
)

files <- list.files(temp_dir_2, full.names = TRUE)
files
lapply(files, \(x) nrow(read.csv(x)))
\dontshow{\}) # examplesIf}
}

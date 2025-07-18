% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/output-ipc.R
\name{dataframe__write_ipc}
\alias{dataframe__write_ipc}
\title{Write to Arrow IPC file.}
\usage{
dataframe__write_ipc(
  path,
  ...,
  compression = c("zstd", "lz4", "uncompressed"),
  compat_level = c("newest", "oldest"),
  storage_options = NULL,
  retries = 2
)
}
\arguments{
\item{path}{A character. File path to which the file should be written.}

\item{...}{These dots are for future extensions and must be empty.}

\item{compression}{The compression method. Must be one of:
\itemize{
\item \code{"lz4"}: fast compression/decompression.
\item \code{"uncompressed"}
\item \code{"snappy"}: this guarantees that the parquet file will be compatible with
older parquet readers.
\item \code{"gzip"}
\item \code{"lzo"}
\item \code{"brotli"}
\item \code{"zstd"}: good compression performance.
}}

\item{compat_level}{Determines the compatibility level when exporting
Polars' internal data structures. When specifying a new compatibility level,
Polars exports its internal data structures that might not be interpretable by
other Arrow implementations. The level can be specified as the name
(e.g., \code{"newest"}) or as a scalar integer (Currently, \code{0} or \code{1} is supported).
\itemize{
\item \code{"newest"} \ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#experimental}{\figure{lifecycle-experimental.svg}{options: alt='[Experimental]'}}}{\strong{[Experimental]}} (default):
Use the highest level, currently same as \code{1} (Low compatibility).
\item \code{"oldest"}: Same as \code{0} (High compatibility).
}}

\item{storage_options}{Named vector containing options that indicate how to
connect to a cloud provider. The cloud providers currently supported are
AWS, GCP, and Azure.
See supported keys here:
\itemize{
\item \href{https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html}{aws}
\item \href{https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html}{gcp}
\item \href{https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html}{azure}
\item Hugging Face (\verb{hf://}): Accepts an API key under the token parameter
\code{c(token = YOUR_TOKEN)} or by setting the \code{HF_TOKEN} environment
variable.
}

If \code{storage_options} is not provided, Polars will try to infer the
information from environment variables.}

\item{retries}{Number of retries if accessing a cloud instance fails.}
}
\value{
\code{NULL} invisibly.
}
\description{
\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#experimental}{\figure{lifecycle-experimental.svg}{options: alt='[Experimental]'}}}{\strong{[Experimental]}}

This allows streaming results that are larger than RAM to be written to disk.
}
\examples{
tmpf <- tempfile()
as_polars_df(mtcars)$write_ipc(tmpf)
pl$read_ipc(tmpf)
}

% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-binary.R
\name{expr_bin_decode}
\alias{expr_bin_decode}
\title{Decode values using the provided encoding}
\usage{
expr_bin_decode(encoding, ..., strict = TRUE)
}
\arguments{
\item{encoding}{A character, \code{"hex"} or \code{"base64"}. The encoding to use.}

\item{...}{These dots are for future extensions and must be empty.}

\item{strict}{Raise an error if the underlying value cannot be decoded,
otherwise mask out with a \code{null} value.}
}
\value{
A polars \link{expression}
}
\description{
Decode values using the provided encoding
}
\examples{
df <- pl$DataFrame(
  name = c("black", "yellow", "blue"),
  code_hex = as_polars_series(c("000000", "ffff00", "0000ff"))$cast(pl$Binary),
  code_base64 = as_polars_series(c("AAAA", "//8A", "AAD/"))$cast(pl$Binary)
)

df$with_columns(
  decoded_hex = pl$col("code_hex")$bin$decode("hex"),
  decoded_base64 = pl$col("code_base64")$bin$decode("base64")
)

# Set `strict = FALSE` to set invalid values to `null` instead of raising an error.
df <- pl$DataFrame(
  colors = as_polars_series(c("000000", "ffff00", "invalid_value"))$cast(pl$Binary)
)
df$select(pl$col("colors")$bin$decode("hex", strict = FALSE))
}

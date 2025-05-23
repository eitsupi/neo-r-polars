% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-binary.R
\name{expr_bin_contains}
\alias{expr_bin_contains}
\title{Check if binaries contain a binary substring}
\usage{
expr_bin_contains(literal)
}
\arguments{
\item{literal}{The binary substring to look for.}
}
\value{
A polars \link{expression}
}
\description{
Check if binaries contain a binary substring
}
\examples{
colors <- pl$DataFrame(
  name = c("black", "yellow", "blue"),
  code = as_polars_series(c("x00x00x00", "xffxffx00", "x00x00xff"))$cast(pl$Binary),
  lit = as_polars_series(c("x00", "xffx00", "xffxff"))$cast(pl$Binary)
)

colors$select(
  "name",
  contains_with_lit = pl$col("code")$bin$contains("xff"),
  contains_with_expr = pl$col("code")$bin$contains(pl$col("lit"))
)
}

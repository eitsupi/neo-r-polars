% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-expr.R
\name{expr__is_nan}
\alias{expr__is_nan}
\title{Check if elements are NaN}
\usage{
expr__is_nan()
}
\value{
A polars \link{expression}
}
\description{
Floating point \code{NaN} (Not A Number) should not be confused with missing data
represented as \code{NA} (in R) or \code{null} (in Polars).
}
\examples{
df <- pl$DataFrame(
  a = c(1, 2, NA, 1, 5),
  b = c(1, 2, NaN, 1, 5)
)
df$with_columns(
  a_nan = pl$col("a")$is_nan(),
  b_nan = pl$col("b")$is_nan()
)
}

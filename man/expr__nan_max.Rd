% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-expr.R
\name{expr__nan_max}
\alias{expr__nan_max}
\title{Get the maximum value with NaN}
\usage{
expr__nan_max()
}
\value{
A polars \link{expression}
}
\description{
This returns \code{NaN} if there are any.
}
\examples{
pl$DataFrame(x = c(1, NA, 3, NaN, Inf))$
  with_columns(nan_max = pl$col("x")$nan_max())
}

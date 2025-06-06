% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-expr.R
\name{expr__cosh}
\alias{expr__cosh}
\title{Compute hyperbolic cosine}
\usage{
expr__cosh()
}
\value{
A polars \link{expression}
}
\description{
Compute hyperbolic cosine
}
\examples{
pl$DataFrame(a = c(-1, acosh(2), 0, 1, NA))$
  with_columns(cosh = pl$col("a")$cosh())
}

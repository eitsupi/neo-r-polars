% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-expr.R
\name{expr__first}
\alias{expr__first}
\title{Get the first value}
\usage{
expr__first()
}
\value{
A polars \link{expression}
}
\description{
Get the first value
}
\examples{
pl$DataFrame(x = 3:1)$with_columns(first = pl$col("x")$first())
}

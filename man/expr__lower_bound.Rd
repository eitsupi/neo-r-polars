% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-expr.R
\name{expr__lower_bound}
\alias{expr__lower_bound}
\title{Calculate the lower bound}
\usage{
expr__lower_bound()
}
\value{
A polars \link{expression}
}
\description{
Returns a unit Series with the lowest value possible for the dtype of this
expression.
}
\examples{
df <- pl$DataFrame(a = 1:3)
df$select(pl$col("a")$lower_bound())
}

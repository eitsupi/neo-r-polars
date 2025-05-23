% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-expr.R
\name{expr__arg_max}
\alias{expr__arg_max}
\title{Get the index of the maximal value}
\usage{
expr__arg_max()
}
\value{
A polars \link{expression}
}
\description{
Get the index of the maximal value
}
\examples{
df <- pl$DataFrame(a = c(20, 10, 30))
df$select(pl$col("a")$arg_max())
}

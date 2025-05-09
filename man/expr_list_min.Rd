% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-list.R
\name{expr_list_min}
\alias{expr_list_min}
\title{Compute the miminum value in every sub-list}
\usage{
expr_list_min()
}
\value{
A polars \link{expression}
}
\description{
Compute the miminum value in every sub-list
}
\examples{
df <- pl$DataFrame(values = list(c(1, 2, 3, NA), c(2, 3), NA))
df$with_columns(min = pl$col("values")$list$min())
}

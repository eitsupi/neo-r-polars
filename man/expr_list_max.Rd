% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-list.R
\name{expr_list_max}
\alias{expr_list_max}
\title{Compute the maximum value in every sub-list}
\usage{
expr_list_max()
}
\value{
A polars \link{expression}
}
\description{
Compute the maximum value in every sub-list
}
\examples{
df <- pl$DataFrame(values = list(c(1, 2, 3, NA), c(2, 3), NA))
df$with_columns(max = pl$col("values")$list$max())
}

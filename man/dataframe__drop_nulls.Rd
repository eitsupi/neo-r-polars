% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dataframe-frame.R
\name{dataframe__drop_nulls}
\alias{dataframe__drop_nulls}
\title{Drop all rows that contain null values}
\usage{
dataframe__drop_nulls(...)
}
\arguments{
\item{...}{<\code{\link[rlang:dyn-dots]{dynamic-dots}}> Column name(s) for which null
values are considered. If empty (default), use all columns.}
}
\value{
A polars \link{DataFrame}
}
\description{
The original order of the remaining rows is preserved.
}
\examples{
df <- pl$DataFrame(
  foo = 1:3,
  bar = c(6L, NA, 8L),
  ham = c("a", "b", NA)
)

# The default behavior of this method is to drop rows where any single value
# of the row is null.
df$drop_nulls()

# This behaviour can be constrained to consider only a subset of columns, as
# defined by name or with a selector. For example, dropping rows if there is
# a null in any of the integer columns:
df$drop_nulls(cs$integer())
}

% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dataframe-frame.R
\name{dataframe__unstack}
\alias{dataframe__unstack}
\title{Unstack a long table to a wide form without doing an aggregation}
\usage{
dataframe__unstack(
  ...,
  step,
  how = c("vertical", "horizontal"),
  fill_values = NULL
)
}
\arguments{
\item{...}{<\code{\link[rlang:dyn-dots]{dynamic-dots}}> Column name(s) and selector(s)
to include in the operation. If empty, use all columns.}

\item{step}{Number of rows in the unstacked frame.}

\item{how}{Direction of the unstack. Must be one of \code{"vertical"} or
\code{"horizontal"}.}

\item{fill_values}{Fill values that don't fit the new size with this value.
This can be a scalar value or a named list of the sort
\verb{list(<column_name> = <fill_value>)}. See examples.}
}
\value{
A polars \link{DataFrame}
}
\description{
This can be much faster than a pivot, because it can skip the grouping phase.
}
\examples{
df <- pl$DataFrame(x = LETTERS[1:8], y = 1:8)$with_columns(
  z = pl$int_ranges(pl$col("y"), pl$col("y") + 2, dtype = pl$UInt8)
)
df

df$unstack(step = 4, how = "vertical")
df$unstack(step = 2, how = "horizontal")
df$unstack(cs$numeric(), step = 5, fill_values = 0)
df$unstack("x", "y", step = 5, fill_values = list(y = 999, x = "foo"))
}

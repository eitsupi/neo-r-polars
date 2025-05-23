% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dataframe-frame.R
\name{dataframe__is_unique}
\alias{dataframe__is_unique}
\title{Get a mask of all unique rows in this DataFrame.}
\usage{
dataframe__is_unique()
}
\value{
A \link[=Series]{polars Series}
}
\description{
Get a mask of all unique rows in this DataFrame.
}
\examples{
df <- pl$DataFrame(
  a = c(1, 2, 3, 1),
  b = c("x", "y", "z", "x")
)
df$is_unique()

# This mask can be used to visualize the unique lines like this:
df$filter(df$is_unique())
}

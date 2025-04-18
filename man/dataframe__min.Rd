% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dataframe-frame.R
\name{dataframe__min}
\alias{dataframe__min}
\title{Aggregate the columns in the DataFrame to their minimum value}
\usage{
dataframe__min()
}
\value{
A polars \link{DataFrame}
}
\description{
Aggregate the columns in the DataFrame to their minimum value
}
\examples{
df <- pl$DataFrame(a = 1:4, b = c(1, 2, 1, 1))
df$min()
}

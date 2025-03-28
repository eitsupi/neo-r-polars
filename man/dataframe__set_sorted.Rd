% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dataframe-frame.R
\name{dataframe__set_sorted}
\alias{dataframe__set_sorted}
\title{Indicate that one or multiple columns are sorted}
\usage{
dataframe__set_sorted(column, ..., descending = FALSE)
}
\arguments{
\item{column}{Column that is sorted.}

\item{...}{These dots are for future extensions and must be empty.}

\item{descending}{Whether the columns are sorted in descending order.}
}
\value{
A polars \link{DataFrame}
}
\description{
This can speed up future operations, but it can lead to incorrect results if
the data is \strong{not} sorted! Use with care!
}
\examples{
# We mark the data as sorted by "age" but this is not the case!
# It is up to the user to ensure that the column is actually sorted.
df1 <- pl$DataFrame(
  name = c("steve", "elise", "bob"),
  age = c(42, 44, 18)
)$set_sorted("age")

df1$flags
}

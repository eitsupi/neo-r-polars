% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/lazyframe-frame.R
\name{lazyframe__set_sorted}
\alias{lazyframe__set_sorted}
\title{Indicate that one or multiple columns are sorted}
\usage{
lazyframe__set_sorted(column, ..., descending = FALSE)
}
\arguments{
\item{column}{Column that is sorted.}

\item{...}{These dots are for future extensions and must be empty.}

\item{descending}{Whether the columns are sorted in descending order.}
}
\value{
A polars \link{LazyFrame}
}
\description{
This can speed up future operations, but it can lead to incorrect results if
the data is \strong{not} sorted! Use with care!
}

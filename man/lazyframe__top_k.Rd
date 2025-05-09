% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/lazyframe-frame.R
\name{lazyframe__top_k}
\alias{lazyframe__top_k}
\title{Return the \code{k} largest rows}
\usage{
lazyframe__top_k(k, ..., by, reverse = FALSE)
}
\arguments{
\item{k}{Number of rows to return.}

\item{...}{These dots are for future extensions and must be empty.}

\item{by}{Column(s) used to determine the bottom rows. Accepts expression
input. Strings are parsed as column names.}

\item{reverse}{Consider the \code{k} smallest elements of the \code{by} column(s)
(instead of the \code{k} largest). This can be specified per column by passing a
sequence of booleans.}
}
\value{
A polars \link{LazyFrame}
}
\description{
Non-null elements are always preferred over null elements, regardless of the
value of \code{reverse}. The output is not guaranteed to be in any particular
order, call \code{sort()} after this function if you wish the output to be sorted.
}
\examples{
lf <- pl$LazyFrame(
  a = c("a", "b", "a", "b", "b", "c"),
  b = c(2, 1, 1, 3, 2, 1)
)

# Get the rows which contain the 4 largest values in column b.
lf$top_k(4, by = "b")$collect()

# Get the rows which contain the 4 largest values when sorting on column a
# and b
lf$top_k(4, by = c("a", "b"))$collect()
}

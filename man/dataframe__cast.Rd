% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dataframe-frame.R
\name{dataframe__cast}
\alias{dataframe__cast}
\title{Cast DataFrame column(s) to the specified dtype}
\usage{
dataframe__cast(..., .strict = TRUE)
}
\arguments{
\item{...}{<\code{\link[rlang:dyn-dots]{dynamic-dots}}> Either a datatype to which
all columns will be cast, or a list where the names are column names and the
values are the datatypes to convert to.}

\item{.strict}{If \code{TRUE} (default), throw an error if a cast could not be
done (for instance, due to an overflow). Otherwise, return \code{null}.}
}
\value{
A polars \link{DataFrame}
}
\description{
This allows to convert all columns to a datatype or to convert only specific
columns. Contrarily to the Python implementation, it is not possible to
convert all columns of a specific datatype to another datatype.
}
\examples{
df <- pl$DataFrame(
  foo = 1:3,
  bar = c(6, 7, 8),
  ham = as.Date(c("2020-01-02", "2020-03-04", "2020-05-06"))
)

# Cast only some columns
df$cast(foo = pl$Float32, bar = pl$UInt8)

# Cast all columns to the same type
df$cast(pl$String)
}

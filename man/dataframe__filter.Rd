% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dataframe-frame.R
\name{dataframe__filter}
\alias{dataframe__filter}
\title{Filter rows of a DataFrame}
\usage{
dataframe__filter(...)
}
\arguments{
\item{...}{<\code{\link[rlang:dyn-dots]{dynamic-dots}}> Expression that evaluates to
a boolean Series.}
}
\value{
A polars \link{DataFrame}
}
\description{
The original order of the remaining rows is preserved. Rows where the filter
does not evaluate to \code{TRUE} are discarded, including nulls.
}
\examples{
df <- as_polars_df(iris)

df$filter(pl$col("Sepal.Length") > 5)

# This is equivalent to
# df$filter(pl$col("Sepal.Length") > 5 & pl$col("Petal.Width") < 1)
df$filter(pl$col("Sepal.Length") > 5, pl$col("Petal.Width") < 1)

# rows where condition is NA are dropped
iris2 <- iris
iris2[c(1, 3, 5), "Species"] <- NA
df <- as_polars_df(iris2)

df$filter(pl$col("Species") == "setosa")
}

% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-expr.R
\name{expr__quantile}
\alias{expr__quantile}
\title{Get quantile value(s)}
\usage{
expr__quantile(
  quantile,
  interpolation = c("nearest", "higher", "lower", "midpoint", "linear", "equiprobable")
)
}
\arguments{
\item{quantile}{Quantile between 0.0 and 1.0.}

\item{interpolation}{Interpolation method. Must be one of \code{"nearest"},
\code{"higher"}, \code{"lower"}, \code{"midpoint"}, \code{"linear"}.}
}
\value{
A polars \link{expression}
}
\description{
Get quantile value(s)
}
\examples{
df <- pl$DataFrame(a = 0:5)
df$select(pl$col("a")$quantile(0.3))
df$select(pl$col("a")$quantile(0.3, interpolation = "higher"))
df$select(pl$col("a")$quantile(0.3, interpolation = "lower"))
df$select(pl$col("a")$quantile(0.3, interpolation = "midpoint"))
df$select(pl$col("a")$quantile(0.3, interpolation = "linear"))
df$select(pl$col("a")$quantile(0.3, interpolation = "equiprobable"))
}

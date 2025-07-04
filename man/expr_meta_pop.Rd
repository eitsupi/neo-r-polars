% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-meta.R
\name{expr_meta_pop}
\alias{expr_meta_pop}
\title{Pop the latest expression and return the input(s) of the popped expression}
\usage{
expr_meta_pop()
}
\value{
A polars \link{expression}
}
\description{
Pop the latest expression and return the input(s) of the popped expression
}
\examples{
e <- pl$col("foo") + pl$col("bar")
first <- e$meta$pop()[[1]]

first$meta$eq(pl$col("bar"))
first$meta$eq(pl$col("foo"))
}

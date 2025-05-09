% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-expr.R
\name{expr__sin}
\alias{expr__sin}
\title{Compute sine}
\usage{
expr__sin()
}
\value{
A polars \link{expression}
}
\description{
Compute sine
}
\examples{
pl$DataFrame(a = c(0, pi / 2, pi, NA))$
  with_columns(sine = pl$col("a")$sin())
}

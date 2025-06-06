% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/lazyframe-group_by.R
\name{lazygroupby__min}
\alias{lazygroupby__min}
\title{Reduce the groups to the minimal value}
\usage{
lazygroupby__min()
}
\value{
A polars \link{LazyFrame}
}
\description{
Reduce the groups to the minimal value
}
\examples{
lf <- pl$LazyFrame(
  grp = c("c", "c", "a", "c", "a", "b"),
  x = c(0.5, 0.5, 4, 10, 13, 14),
  y = 1:6,
  z = c(TRUE, TRUE, FALSE, TRUE, FALSE, TRUE)
)
lf$collect()

lf$group_by("grp")$min()$collect()
}

% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/lazyframe-group_by.R
\name{lazygroupby__max}
\alias{lazygroupby__max}
\title{Reduce the groups to the maximal value}
\usage{
lazygroupby__max()
}
\value{
A polars \link{LazyFrame}
}
\description{
Reduce the groups to the maximal value
}
\examples{
lf <- pl$LazyFrame(
  grp = c("c", "c", "a", "c", "a", "b"),
  x = c(0.5, 0.5, 4, 10, 13, 14),
  y = 1:6,
  z = c(TRUE, TRUE, FALSE, TRUE, FALSE, TRUE)
)
lf$collect()

lf$group_by("grp")$max()$collect()
}

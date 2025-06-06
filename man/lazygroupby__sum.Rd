% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/lazyframe-group_by.R
\name{lazygroupby__sum}
\alias{lazygroupby__sum}
\title{Return the sum per group}
\usage{
lazygroupby__sum()
}
\value{
A polars \link{LazyFrame}
}
\description{
Return the sum per group
}
\examples{
lf <- pl$LazyFrame(
  grp = c("c", "c", "a", "c", "a", "b"),
  x = c(0.5, 0.5, 4, 10, 13, 14),
  y = 1:6,
  z = c(TRUE, TRUE, FALSE, TRUE, FALSE, TRUE)
)
lf$collect()

lf$group_by("grp")$sum()$collect()
}

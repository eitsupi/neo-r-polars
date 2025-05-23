% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-expr.R
\name{expr__hist}
\alias{expr__hist}
\title{Bin values into buckets and count their occurrences}
\usage{
expr__hist(
  bins = NULL,
  ...,
  bin_count = NULL,
  include_category = FALSE,
  include_breakpoint = FALSE
)
}
\arguments{
\item{bins}{Discretizations to make. If \code{NULL} (default), we determine the
boundaries based on the data.}

\item{...}{These dots are for future extensions and must be empty.}

\item{bin_count}{If no bins provided, this will be used to determine the
distance of the bins.}

\item{include_category}{Include a column that shows the intervals as
categories.}

\item{include_breakpoint}{Include a column that indicates the upper
breakpoint.}
}
\value{
A polars \link{expression}
}
\description{
\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#experimental}{\figure{lifecycle-experimental.svg}{options: alt='[Experimental]'}}}{\strong{[Experimental]}}
}
\examples{
df <- pl$DataFrame(a = c(1, 3, 8, 8, 2, 1, 3))
df$select(pl$col("a")$hist(bins = 1:3))
df$select(
  pl$col("a")$hist(
    bins = 1:3, include_category = TRUE, include_breakpoint = TRUE
  )
)
}

% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-expr.R
\name{expr__cumulative_eval}
\alias{expr__cumulative_eval}
\title{Return the cumulative count of the non-null values in the column}
\usage{
expr__cumulative_eval(expr, ..., min_samples = 1)
}
\arguments{
\item{expr}{Expression to evaluate.}

\item{...}{These dots are for future extensions and must be empty.}

\item{min_samples}{Number of valid values (i.e. \code{length - null_count}) there
should be in the window before the expression is evaluated.}
}
\value{
A polars \link{expression}
}
\description{
\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#experimental}{\figure{lifecycle-experimental.svg}{options: alt='[Experimental]'}}}{\strong{[Experimental]}}
}
\details{
This can be really slow as it can have \code{O(n^2)} complexity. Don’t use this
for operations that visit all elements.
}
\examples{
df <- pl$DataFrame(values = 1:5)
df$with_columns(
  pl$col("values")$cumulative_eval(
    pl$element()$first() - pl$element()$last()**2
  )
)
}

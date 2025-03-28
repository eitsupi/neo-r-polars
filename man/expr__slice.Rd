% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-expr.R
\name{expr__slice}
\alias{expr__slice}
\title{Get a slice of this expression}
\usage{
expr__slice(offset, length = NULL)
}
\arguments{
\item{offset}{Numeric or expression, zero-indexed. Indicates where to start
the slice. A negative value is one-indexed and starts from the end.}

\item{length}{Maximum number of elements contained in the slice. If \code{NULL}
(default), all rows starting at the offset will be selected.}
}
\value{
A polars \link{expression}
}
\description{
Get a slice of this expression
}
\examples{
# as head
pl$DataFrame(a = 0:100)$select(
  pl$all()$slice(0, 6)
)

# as tail
pl$DataFrame(a = 0:100)$select(
  pl$all()$slice(-6, 6)
)

pl$DataFrame(a = 0:100)$select(
  pl$all()$slice(80)
)
}

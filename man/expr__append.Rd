% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-expr.R
\name{expr__append}
\alias{expr__append}
\title{Append expressions}
\usage{
expr__append(other, ..., upcast = TRUE)
}
\arguments{
\item{other}{Expression to append.}

\item{...}{These dots are for future extensions and must be empty.}

\item{upcast}{If \code{TRUE} (default), cast both Series to the same supertype.}
}
\value{
A polars \link{expression}
}
\description{
Append expressions
}
\examples{
df <- pl$DataFrame(a = 8:10, b = c(NA, 4, 4))
df$select(pl$all()$head(1)$append(pl$all()$tail(1)))
}

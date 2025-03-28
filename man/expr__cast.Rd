% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-expr.R
\name{expr__cast}
\alias{expr__cast}
\title{Cast between DataType}
\usage{
expr__cast(dtype, ..., strict = TRUE, wrap_numerical = FALSE)
}
\arguments{
\item{dtype}{DataType to cast to.}

\item{...}{These dots are for future extensions and must be empty.}

\item{strict}{If \code{TRUE} (default), an error will be thrown if cast failed at
resolve time.}

\item{wrap_numerical}{If \code{TRUE}, numeric casts wrap overflowing values
instead of marking the cast as invalid.}
}
\value{
A polars \link{expression}
}
\description{
Cast between DataType
}
\examples{
df <- pl$DataFrame(a = 1:3, b = c(1, 2, 3))
df$with_columns(
  pl$col("a")$cast(pl$Float64),
  pl$col("b")$cast(pl$Int32)
)

# strict FALSE, inserts null for any cast failure
pl$select(
  pl$lit(c(100, 200, 300))$cast(pl$UInt8, strict = FALSE)
)$to_series()

# strict TRUE, raise any failure as an error when query is executed.
tryCatch(
  {
    pl$select(
      pl$lit("a")$cast(pl$Float64, strict = TRUE)
    )$to_series()
  },
  error = function(e) e
)
}

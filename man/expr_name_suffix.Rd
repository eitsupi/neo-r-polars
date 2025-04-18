% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-name.R
\name{expr_name_suffix}
\alias{expr_name_suffix}
\title{Add a suffix to a column name}
\usage{
expr_name_suffix(suffix)
}
\arguments{
\item{suffix}{Suffix to be added to column name(s)}
}
\value{
A polars \link{expression}
}
\description{
Add a suffix to a column name
}
\examples{
dat <- as_polars_df(mtcars)

dat$select(
  pl$col("mpg"),
  pl$col("mpg")$name$suffix("_foo"),
  pl$col("cyl", "drat")$name$suffix("_bar")
)
}
\seealso{
\code{\link[=expr_name_prefix]{$prefix()}} to add a prefix
}

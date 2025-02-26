% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/selectors.R
\name{cs__categorical}
\alias{cs__categorical}
\title{Select all categorical columns}
\usage{
cs__categorical()
}
\value{
A Polars selector
}
\description{
Select all categorical columns
}
\examples{
df <- pl$DataFrame(
  foo = c("xx", "yy"),
  bar = c(123, 456),
  baz = c(2.0, 5.5),
  .schema_overrides = list(foo = pl$Categorical()),
)

# Select categorical columns:
df$select(cs$categorical())

# Select all columns except for those that are categorical:
df$select(!cs$categorical())
}
\seealso{
\link{cs} for the documentation on operators supported by Polars
selectors.
}

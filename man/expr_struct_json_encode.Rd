% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-struct.R
\name{expr_struct_json_encode}
\alias{expr_struct_json_encode}
\title{Convert this struct to a string column with json values}
\usage{
expr_struct_json_encode()
}
\value{
A polars \link{expression}
}
\description{
Convert this struct to a string column with json values
}
\examples{
df <- pl$DataFrame(
  a = list(1:2, c(9, 1, 3)),
  b = list(45, NA)
)$select(a = pl$struct("a", "b"))

df

df$with_columns(encoded = pl$col("a")$struct$json_encode())
}

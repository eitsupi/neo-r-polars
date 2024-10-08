% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/functions-as_datatype.R
\name{pl__struct}
\alias{pl__struct}
\title{Collect columns into a struct column}
\usage{
pl__struct(...)
}
\arguments{
\item{...}{<\code{\link[rlang:dyn-dots]{dynamic-dots}}>
Name-value pairs of objects to be converted to polars \link[=Expr]{expressions}
by the \code{\link[=as_polars_expr]{as_polars_expr()}} function.
Characters are parsed as column names, other non-expression inputs are parsed as \link[=pl__lit]{literals}.
Each name will be used as the expression name.}
}
\value{
A polars \link{expression}
}
\description{
Collect columns into a struct column
}
\examples{
# Collect all columns of a dataframe into a struct by passing pl.all().
df <- pl$DataFrame(
  int = 1:2,
  str = c("a", "b"),
  bool = c(TRUE, NA),
  list = list(1:2, 3L),
)
df$select(pl$struct(pl$all())$alias("my_struct"))

# Name each struct field.
df$select(pl$struct(p = "int", q = "bool")$alias("my_struct"))$schema
}

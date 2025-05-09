% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-list.R
\name{expr_list_concat}
\alias{expr_list_concat}
\title{Concat the lists into a new list}
\usage{
expr_list_concat(other)
}
\arguments{
\item{other}{Values to concat with. Can be an Expr or something coercible to
an Expr.}
}
\value{
A polars \link{expression}
}
\description{
Concat the lists into a new list
}
\examples{
df <- pl$DataFrame(
  a = list("a", "x"),
  b = list(c("b", "c"), c("y", "z"))
)
df$with_columns(
  conc_to_b = pl$col("a")$list$concat(pl$col("b")),
  conc_to_lit_str = pl$col("a")$list$concat(pl$lit("some string")),
  conc_to_lit_list = pl$col("a")$list$concat(pl$lit(list("hello", c("hello", "world"))))
)
}

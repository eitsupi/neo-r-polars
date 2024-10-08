% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/lazyframe-frame.R
\name{lazyframe__with_columns}
\alias{lazyframe__with_columns}
\title{Add columns to this LazyFrame}
\usage{
lazyframe__with_columns(...)
}
\arguments{
\item{...}{<\code{\link[rlang:dyn-dots]{dynamic-dots}}>
Name-value pairs of objects to be converted to polars \link[=Expr]{expressions}
by the \code{\link[=as_polars_expr]{as_polars_expr()}} function.
Characters are parsed as column names, other non-expression inputs are parsed as \link[=pl__lit]{literals}.
Each name will be used as the expression name.}
}
\value{
A polars \link{LazyFrame}
}
\description{
Added columns will replace existing columns with the same name.
}
\details{
Creating a new LazyFrame using this method does not create a new copy of
existing data.
}
\examples{
# Pass an expression to add it as a new column.
lf <- pl$LazyFrame(
  a = 1:4,
  b = c(0.5, 4, 10, 13),
  c = c(TRUE, TRUE, FALSE, TRUE),
)
lf$with_columns((pl$col("a")^2)$alias("a^2"))$collect()

# Added columns will replace existing columns with the same name.
lf$with_columns(a = pl$col("a")$cast(pl$Float64))$collect()

# Multiple columns can be added
lf$with_columns(
  (pl$col("a")^2)$alias("a^2"),
  (pl$col("b") / 2)$alias("b/2"),
  (pl$col("c")$not())$alias("not c"),
)$collect()

# Name expression instead of `$alias()`
lf$with_columns(
  `a^2` = pl$col("a")^2,
  `b/2` = pl$col("b") / 2,
  `not c` = pl$col("c")$not(),
)$collect()

# Expressions with multiple outputs can automatically be instantiated
# as Structs by enabling the experimental setting `POLARS_AUTO_STRUCTIFY`:
if (requireNamespace("withr", quietly = TRUE)) {
  withr::with_envvar(c(POLARS_AUTO_STRUCTIFY = "1"), {
    lf$drop("c")$with_columns(
      diffs = pl$col("a", "b")$diff()$name$suffix("_diff"),
    )$collect()
  })
}
}

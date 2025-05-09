% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-string.R
\name{expr_str_count_matches}
\alias{expr_str_count_matches}
\title{Count all successive non-overlapping regex matches}
\usage{
expr_str_count_matches(pattern, ..., literal = FALSE)
}
\arguments{
\item{pattern}{A character or something can be coerced to a string \link{Expr}
of a valid regex pattern, compatible with the \href{https://docs.rs/regex/latest/regex/}{regex crate}.}

\item{...}{These dots are for future extensions and must be empty.}

\item{literal}{Logical. If \code{TRUE}, treat \code{pattern} as a literal string,
not as a regular expression.}
}
\value{
A polars \link{expression}
}
\description{
Count all successive non-overlapping regex matches
}
\examples{
df <- pl$DataFrame(foo = c("12 dbc 3xy", "cat\\\\w", "1zy3\\\\d\\\\d", NA))

df$with_columns(
  count_digits = pl$col("foo")$str$count_matches(r"(\d)"),
  count_slash_d = pl$col("foo")$str$count_matches(r"(\d)", literal = TRUE)
)
}

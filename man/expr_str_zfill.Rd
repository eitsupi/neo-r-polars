% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-string.R
\name{expr_str_zfill}
\alias{expr_str_zfill}
\title{Fills the string with zeroes.}
\usage{
expr_str_zfill(length)
}
\arguments{
\item{length}{Pad the string until it reaches this length. Strings with
length equal to or greater than this value are returned as-is. This can be
an Expr or something coercible to an Expr. Strings are parsed as column
names.}
}
\value{
A polars \link{expression}
}
\description{
Add zeroes to a string until it reaches \code{n} characters. If the
number of characters is already greater than \code{n}, the string is not modified.
}
\details{
Return a copy of the string left filled with ASCII '0' digits to make a string
of length width.

A leading sign prefix ('+'/'-') is handled by inserting the padding after the
sign character rather than before. The original string is returned if width is
less than or equal to \code{len(s)}.
}
\examples{
df <- pl$DataFrame(a = c(-1L, 123L, 999999L, NA))
df$with_columns(zfill = pl$col("a")$cast(pl$String)$str$zfill(4))
}

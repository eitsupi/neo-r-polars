% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-string.R
\name{expr_str_replace}
\alias{expr_str_replace}
\title{Replace first matching regex/literal substring with a new string value}
\usage{
expr_str_replace(pattern, value, ..., literal = FALSE, n = 1L)
}
\arguments{
\item{pattern}{A character or something can be coerced to a string \link{Expr}
of a valid regex pattern, compatible with the \href{https://docs.rs/regex/latest/regex/}{regex crate}.}

\item{value}{A character or an \link{Expr} of string
that will replace the matched substring.}

\item{...}{These dots are for future extensions and must be empty.}

\item{literal}{Logical. If \code{TRUE}, treat \code{pattern} as a literal string,
not as a regular expression.}

\item{n}{A number of matches to replace.
Note that regex replacement with \code{n > 1} not yet supported,
so raise an error if \code{n > 1} and \code{pattern} includes regex pattern
and \code{literal = FALSE}.}
}
\value{
A polars \link{expression}
}
\description{
Replace first matching regex/literal substring with a new string value
}
\details{
To modify regular expression behaviour (such as case-sensitivity)
with flags, use the inline \code{(?iLmsuxU)} syntax. See the regex crate’s section
on \href{https://docs.rs/regex/latest/regex/#grouping-and-flags}{grouping and flags}
for additional information about the use of inline expression modifiers.
}
\section{Capture groups}{

The dollar sign (\code{$}) is a special character related to capture groups.
To refer to a literal dollar sign, use \verb{$$} instead or set \code{literal} to \code{TRUE}.
}

\examples{
df <- pl$DataFrame(id = 1L:2L, text = c("123abc", "abc456"))
df$with_columns(pl$col("text")$str$replace(r"(abc\b)", "ABC"))

# Capture groups are supported.
# Use `${1}` in the value string to refer to the first capture group in the pattern,
# `${2}` to refer to the second capture group, and so on.
# You can also use named capture groups.
df <- pl$DataFrame(word = c("hat", "hut"))
df$with_columns(
  positional = pl$col("word")$str$replace("h(.)t", "b${1}d"),
  named = pl$col("word")$str$replace("h(?<vowel>.)t", "b${vowel}d")
)

# Apply case-insensitive string replacement using the `(?i)` flag.
df <- pl$DataFrame(
  city = rep("Philadelphia", 4),
  season = c("Spring", "Summer", "Autumn", "Winter"),
  weather = c("Rainy", "Sunny", "Cloudy", "Snowy")
)
df$with_columns(
  pl$col("weather")$str$replace("(?i)foggy|rainy|cloudy|snowy", "Sunny")
)
}
\seealso{
\itemize{
\item \code{\link[=expr_str_replace_all]{<Expr>$str$replace_all()}}
}
}

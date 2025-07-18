% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-string.R
\name{expr_str_extract_many}
\alias{expr_str_extract_many}
\title{Use the Aho-Corasick algorithm to extract matches}
\usage{
expr_str_extract_many(
  patterns,
  ...,
  ascii_case_insensitive = FALSE,
  overlapping = FALSE
)
}
\arguments{
\item{patterns}{String patterns to search. This can be an Expr or something
coercible to an Expr. Strings are parsed as column names.}

\item{...}{These dots are for future extensions and must be empty.}

\item{ascii_case_insensitive}{Enable ASCII-aware case insensitive matching.
When this option is enabled, searching will be performed without respect to
case for ASCII letters (a-z and A-Z) only.}

\item{overlapping}{Whether matches can overlap.}
}
\value{
A polars \link{expression}
}
\description{
\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#experimental}{\figure{lifecycle-experimental.svg}{options: alt='[Experimental]'}}}{\strong{[Experimental]}}
This method supports matching on string literals only,
and does not support regular expression matching.
}
\examples{
df <- pl$DataFrame(values = "discontent")
patterns <- pl$lit(c("winter", "disco", "onte", "discontent"))

df$with_columns(
  matches = pl$col("values")$str$extract_many(patterns),
  matches_overlap = pl$col("values")$str$extract_many(patterns, overlapping = TRUE)
)

df <- pl$DataFrame(
  values = c("discontent", "rhapsody"),
  patterns = list(c("winter", "disco", "onte", "discontent"), c("rhap", "ody", "coalesce"))
)

df$select(pl$col("values")$str$extract_many("patterns"))
}

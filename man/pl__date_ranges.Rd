% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/functions-range-date_range.R
\name{pl__date_ranges}
\alias{pl__date_ranges}
\title{Create a column of date ranges}
\usage{
pl__date_ranges(
  start,
  end,
  interval = "1d",
  ...,
  closed = c("both", "left", "none", "right")
)
}
\arguments{
\item{start}{Lower bound of the date range. Something that can be coerced to
a Date or a \link[=pl__Datetime]{Datetime} expression. See examples for details.}

\item{end}{Upper bound of the date range. Something that can be coerced to a
Date or a \link[=pl__Datetime]{Datetime} expression. See examples for details.}

\item{interval}{Interval of the range periods, specified as a \link{difftime}
object or using the Polars duration string language. See the \verb{Polars duration string language} section for details. Must consist of full days.}

\item{...}{These dots are for future extensions and must be empty.}

\item{closed}{Define which sides of the range are closed (inclusive).
One of the following: \code{"both"} (default), \code{"left"}, \code{"right"}, \code{"none"}.}
}
\value{
A polars \link{expression}
}
\description{
If both \code{start} and \code{end} are passed as Date types (not Datetime), and
the \code{interval} granularity is no finer than \code{"1d"}, the returned range is
also of type Date. All other permutations return a Datetime.
}
\section{Polars duration string language}{

Polars duration string language is a simple representation of
durations. It is used in many Polars functions that accept durations.

It has the following format:
\itemize{
\item 1ns (1 nanosecond)
\item 1us (1 microsecond)
\item 1ms (1 millisecond)
\item 1s (1 second)
\item 1m (1 minute)
\item 1h (1 hour)
\item 1d (1 calendar day)
\item 1w (1 calendar week)
\item 1mo (1 calendar month)
\item 1q (1 calendar quarter)
\item 1y (1 calendar year)
}

Or combine them: \code{"3d12h4m25s"} # 3 days, 12 hours, 4 minutes, and 25 seconds

By "calendar day", we mean the corresponding time on the next day
(which may not be 24 hours, due to daylight savings).
Similarly for "calendar week", "calendar month", "calendar quarter", and "calendar year".
}

\examples{
df <- pl$DataFrame(
  start = as.Date(c("2022-01-01", "2022-01-02", NA)),
  end = rep(as.Date("2022-01-03"), 3)
)

df$with_columns(
  date_range = pl$date_ranges("start", "end"),
  date_range_cr = pl$date_ranges("start", "end", closed = "right")
)

# provide a custom "end" value
df$with_columns(
  date_range_lit = pl$date_ranges("start", pl$lit(as.Date("2022-01-02")))
)
}
\seealso{
\code{\link[=pl__date_range]{pl$date_range()}} to create a simple Series of
data type Date.
}

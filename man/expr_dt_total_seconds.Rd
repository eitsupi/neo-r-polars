% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-datetime.R
\name{expr_dt_total_seconds}
\alias{expr_dt_total_seconds}
\title{Extract the seconds from a Duration type}
\usage{
expr_dt_total_seconds()
}
\value{
A polars \link{expression}
}
\description{
Extract the seconds from a Duration type
}
\examples{
df <- pl$select(date = pl$datetime_range(
  start = as.POSIXct("2020-1-1", tz = "GMT"),
  end = as.POSIXct("2020-1-1 00:04:00", tz = "GMT"),
  interval = "1m"
))
df$with_columns(
  diff_sec = pl$col("date")$diff()$dt$total_seconds()
)
}

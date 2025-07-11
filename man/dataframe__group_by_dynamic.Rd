% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dataframe-frame.R
\name{dataframe__group_by_dynamic}
\alias{dataframe__group_by_dynamic}
\title{Group based on a date/time or integer column}
\usage{
dataframe__group_by_dynamic(
  index_column,
  ...,
  every,
  period = NULL,
  offset = NULL,
  include_boundaries = FALSE,
  closed = c("left", "right", "both", "none"),
  label = c("left", "right", "datapoint"),
  group_by = NULL,
  start_by = "window"
)
}
\arguments{
\item{index_column}{Column used to group based on the time window. Often of
type Date/Datetime. This column must be sorted in ascending order (or, if
\code{group_by} is specified, then it must be sorted in ascending order within
each group).
In case of a dynamic group by on indices, the data type needs to be either
Int32 or In64. Note that Int32 gets temporarily cast to Int64, so if
performance matters, use an Int64 column.}

\item{...}{These dots are for future extensions and must be empty.}

\item{every}{Interval of the window.}

\item{period}{Length of the window. If \code{NULL} (default), it will equal
\code{every}.}

\item{offset}{Offset of the window, does not take effect if
\code{start_by = "datapoint"}. Defaults to zero.}

\item{include_boundaries}{Add two columns \code{"_lower_boundary"} and
\code{"_upper_boundary"} columns that show the boundaries of the window. This will
impact performance because it’s harder to parallelize.}

\item{closed}{Define which sides of the interval are closed (inclusive).
Default is \code{"left"}.}

\item{label}{Define which label to use for the window:
\itemize{
\item \code{"left"}: lower boundary of the window
\item \code{"right"}: upper boundary of the window
\item \code{"datapoint"}: the first value of the index column in the given window. If
you don’t need the label to be at one of the boundaries, choose this option
for maximum performance.
}}

\item{group_by}{Also group by this column/these columns. Can be expressions
or objects coercible to expressions.}

\item{start_by}{The strategy to determine the start of the first window by:
\itemize{
\item \code{"window"}: start by taking the earliest timestamp, truncating it with
\code{every}, and then adding \code{offset}. Note that weekly windows start on
Monday.
\item \code{"datapoint"}: start from the first encountered data point.
\item a day of the week (only takes effect if \code{every} contains \code{"w"}): \code{"monday"}
starts the window on the Monday before the first data point, etc.
}}
}
\value{
An object of class \code{polars_group_by_dynamic}
}
\description{
Time windows are calculated and rows are assigned to windows. Different from
a normal group by is that a row can be member of multiple groups. By
default, the windows look like:
\itemize{
\item [start, start + period)
\item [start + every, start + every + period)
\item [start + 2 * every, start + 2 * every + period)
\item …
}

where \code{start} is determined by \code{start_by}, \code{offset}, \code{every}, and the
earliest datapoint. See the \code{start_by} argument description for details.
}
\details{
The \code{every}, \code{period}, and \code{offset} arguments are created with the following
string language:
\itemize{
\item 1ns # 1 nanosecond
\item 1us # 1 microsecond
\item 1ms # 1 millisecond
\item 1s  # 1 second
\item 1m  # 1 minute
\item 1h  # 1 hour
\item 1d  # 1 day
\item 1w  # 1 calendar week
\item 1mo # 1 calendar month
\item 1y  # 1 calendar year
These strings can be combined:
\itemize{
\item 3d12h4m25s # 3 days, 12 hours, 4 minutes, and 25 seconds
}
}

In case of a \code{group_by_dynamic} on an integer column, the windows are
defined by:
\itemize{
\item 1i # length 1
\item 10i # length 10
}
}
\examples{
df <- pl$select(
  time = pl$datetime_range(
    start = strptime("2021-12-16 00:00:00", format = "\%Y-\%m-\%d \%H:\%M:\%S", tz = "UTC"),
    end = strptime("2021-12-16 03:00:00", format = "\%Y-\%m-\%d \%H:\%M:\%S", tz = "UTC"),
    interval = "30m"
  ),
  n = 0:6
)
df

# Group by windows of 1 hour.
df$group_by_dynamic("time", every = "1h", closed = "right")$agg(
  vals = pl$col("n")
)

# The window boundaries can also be added to the aggregation result
df$group_by_dynamic(
  "time",
  every = "1h", include_boundaries = TRUE, closed = "right"
)$agg(
  pl$col("n")$mean()
)

# When closed = "left", the window excludes the right end of interval:
# [lower_bound, upper_bound)
df$group_by_dynamic("time", every = "1h", closed = "left")$agg(
  pl$col("n")
)

# When closed = "both" the time values at the window boundaries belong to 2
# groups.
df$group_by_dynamic("time", every = "1h", closed = "both")$agg(
  pl$col("n")
)

# Dynamic group bys can also be combined with grouping on normal keys
df <- df$with_columns(
  groups = as_polars_series(c("a", "a", "a", "b", "b", "a", "a"))
)
df

df$group_by_dynamic(
  "time",
  every = "1h",
  closed = "both",
  group_by = "groups",
  include_boundaries = TRUE
)$agg(pl$col("n"))

# We can also create a dynamic group by based on an index column
df <- pl$DataFrame(
  idx = 0:5,
  A = c("A", "A", "B", "B", "B", "C")
)$with_columns(pl$col("idx")$set_sorted())
df

df$group_by_dynamic(
  "idx",
  every = "2i",
  period = "3i",
  include_boundaries = TRUE,
  closed = "right"
)$agg(A_agg_list = pl$col("A"))
}
\seealso{
\itemize{
\item \code{\link[=dataframe__rolling]{<DataFrame>$rolling()}}
}
}

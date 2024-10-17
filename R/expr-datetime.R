# The env for storing all expr dt methods
polars_expr_dt_methods <- new.env(parent = emptyenv())

namespace_expr_dt <- function(x) {
  self <- new.env(parent = emptyenv())
  self$`_rexpr` <- x$`_rexpr`

  lapply(names(polars_expr_dt_methods), function(name) {
    fn <- polars_expr_dt_methods[[name]]
    environment(fn) <- environment()
    assign(name, fn, envir = self)
  })

  class(self) <- c("polars_namespace_expr", "polars_object")
  self
}

#' Convert to given time zone for an expression of type Datetime.
#'
#' If converting from a time-zone-naive datetime,
#' then conversion will happen as if converting from UTC,
#' regardless of your system’s time zone.
#' @param time_zone String time zone from [base::OlsonNames()]
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$datetime_range(
#'     as.POSIXct("2020-03-01", tz = "UTC"),
#'     as.POSIXct("2020-05-01", tz = "UTC"),
#'     "1mo1s"
#'   )
#' )
#'
#' df$select(
#'   "date",
#'   London = pl$col("date")$dt$convert_time_zone("Europe/London")
#' )
expr_dt_convert_time_zone <- function(time_zone) {
  self$`_rexpr`$dt_convert_time_zone(time_zone) |>
    wrap()
}

# TODO: link to `convert_time_zone`
# TODO: return, examples
#' Replace time zone for an expression of type Datetime
#'
#' Different from `convert_time_zone`, this will also modify the underlying timestamp
#' and will ignore the original time zone.
#'
#' @inheritParams rlang::args_dots_empty
#' @param time_zone `NULL` or a character time zone from [base::OlsonNames()].
#' Pass `NULL` to unset time zone.
#' @param ambiguous Determine how to deal with ambiguous datetimes.
#' Character vector or [expression] containing the followings:
#' - `"raise"` (default): Throw an error
#' - `"earliest"`: Use the earliest datetime
#' - `"latest"`: Use the latest datetime
#' - `"null"`: Return a null value
#' @param non_existent Determine how to deal with non-existent datetimes.
#' One of the followings:
#' - `"raise"` (default): Throw an error
#' - `"null"`: Return a null value
#' @examples
#' # You can use `ambiguous` to deal with ambiguous datetimes:
#' dates <- c(
#'   "2018-10-28 01:30",
#'   "2018-10-28 02:00",
#'   "2018-10-28 02:30",
#'   "2018-10-28 02:00"
#' ) |>
#'   as.POSIXct("UTC")
#'
#' df2 <- pl$DataFrame(
#'   ts = as_polars_series(dates),
#'   ambiguous = c("earliest", "earliest", "latest", "latest"),
#' )
#'
#' df2$with_columns(
#'   ts_localized = pl$col("ts")$dt$replace_time_zone(
#'     "Europe/Brussels",
#'     ambiguous = pl$col("ambiguous")
#'   )
#' )
expr_dt_replace_time_zone <- function(
    time_zone,
    ...,
    ambiguous = c("raise", "earliest", "latest", "null"),
    non_existent = c("raise", "null")) {
  wrap({
    check_dots_empty0(...)
    non_existent <- arg_match0(non_existent, c("raise", "null"))

    if (!is_polars_expr(ambiguous)) {
      ambiguous <- arg_match0(ambiguous, c("raise", "earliest", "latest", "null")) |>
        as_polars_expr(as_lit = TRUE)
    }

    self$`_rexpr`$dt_replace_time_zone(
      time_zone,
      ambiguous = ambiguous$`_rexpr`,
      non_existent = non_existent
    )
  })
}


#' Truncate datetime
#' @description  Divide the date/datetime range into buckets.
#' Each date/datetime is mapped to the start of its bucket.
#'
#' @param every Either an Expr or a string indicating a column name or a
#' duration (see Details).
#'
#' @details The ``every`` and ``offset`` argument are created with the
#' the following string language:
#' - 1ns # 1 nanosecond
#' - 1us # 1 microsecond
#' - 1ms # 1 millisecond
#' - 1s  # 1 second
#' - 1m  # 1 minute
#' - 1h  # 1 hour
#' - 1d  # 1 day
#' - 1w  # 1 calendar week
#' - 1mo # 1 calendar month
#' - 1y  # 1 calendar year
#' These strings can be combined:
#'   - 3d12h4m25s # 3 days, 12 hours, 4 minutes, and 25 seconds
#' @inherit as_polars_expr return
#' @examples
#' t1 <- as.POSIXct("3040-01-01", tz = "GMT")
#' t2 <- t1 + as.difftime(25, units = "secs")
#' s <- pl$datetime_range(t1, t2, interval = "2s", time_unit = "ms")
#'
#' df <- pl$DataFrame(datetime = s)$with_columns(
#'   pl$col("datetime")$dt$truncate("4s")$alias("truncated_4s")
#' )
#' df
expr_dt_truncate <- function(every) {
  every <- parse_as_polars_duration_string(every, default = "0ns")
  self$`_rexpr`$dt_truncate(as_polars_expr(every, as_lit = TRUE)$`_rexpr`) |>
    wrap()
}

#' Round datetime
#' @description  Divide the date/datetime range into buckets.
#' Each date/datetime in the first half of the interval
#' is mapped to the start of its bucket.
#' Each date/datetime in the second half of the interval
#' is mapped to the end of its bucket.
#'
#' @inherit expr_dt_truncate params details return
#'
#' @examples
#' t1 <- as.POSIXct("3040-01-01", tz = "GMT")
#' t2 <- t1 + as.difftime(25, units = "secs")
#' s <- pl$datetime_range(t1, t2, interval = "2s", time_unit = "ms")
#'
#' df <- pl$DataFrame(datetime = s)$with_columns(
#'   pl$col("datetime")$dt$round("4s")$alias("rounded_4s")
#' )
#' df
expr_dt_round <- function(every) {
  every <- parse_as_polars_duration_string(every, default = "0ns")
  self$`_rexpr`$dt_round(as_polars_expr(every, as_lit = TRUE)$`_rexpr`) |>
    wrap()
}

#' Combine Date and Time
#'
#' If the underlying expression is a Datetime then its time component is
#' replaced, and if it is a Date then a new Datetime is created by combining
#' the two values.
#'
#' @param time The number of epoch since or before (if negative) the Date. Can be
#' an Expr or a PTime.
#' @inheritParams DataType_Datetime
#'
#' @inherit expr_dt_truncate return
#' @examples
#' df <- pl$DataFrame(
#'   dtm = c(
#'     ISOdatetime(2022, 12, 31, 10, 30, 45),
#'     ISOdatetime(2023, 7, 5, 23, 59, 59)
#'   ),
#'   dt = c(ISOdate(2022, 10, 10), ISOdate(2022, 7, 5)),
#'   tm = c(pl$time(1, 2, 3, 456000), pl$time(7, 8, 9, 101000))
#' )$explode("tm")
#'
#' df
#'
#' df$select(
#'   d1 = pl$col("dtm")$dt$combine(pl$col("tm")),
#'   s2 = pl$col("dt")$dt$combine(pl$col("tm")),
#'   d3 = pl$col("dt")$dt$combine(pl$time(4, 5, 6))
#' )
expr_dt_combine <- function(time, time_unit = "us") {
  # PTime implicitly gets converted to "ns"
  if (inherits(time, "PTime")) time_unit <- "ns"
  self$`_rexpr`$dt_combine(time, time_unit) |>
    wrap()
}

#' strftime
#' @description
#' Format Date/Datetime with a formatting rule.
#' See `chrono strftime/strptime
#' <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`_.
#'
#'
#' @param format string format very much like in R passed to chrono
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$lit(as.POSIXct("2021-01-02 12:13:14", tz = "GMT"))$dt$strftime("this is the year: %Y")$to_r()
expr_dt_strftime <- function(format) {
  self$`_rexpr`$dt_strftime(format) |>
    wrap()
}


#' Year
#' @description
#' Extract year from underlying Date representation.
#' Applies to Date and Datetime columns.
#' Returns the year number in the calendar date.
#'
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$date_range(
#'     as.Date("2020-12-25"),
#'     as.Date("2021-1-05"),
#'     interval = "1d",
#'     time_zone = "GMT"
#'   )
#' )
#' df$with_columns(
#'   pl$col("date")$dt$year()$alias("year"),
#'   pl$col("date")$dt$iso_year()$alias("iso_year")
#' )
expr_dt_year <- function() {
  self$`_rexpr`$dt_year() |>
    wrap()
}

#' Iso-Year
#' @description
#' Extract ISO year from underlying Date representation.
#' Applies to Date and Datetime columns.
#' Returns the year number in the ISO standard.
#' This may not correspond with the calendar year.
#'
#'
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$date_range(
#'     as.Date("2020-12-25"),
#'     as.Date("2021-1-05"),
#'     interval = "1d",
#'     time_zone = "GMT"
#'   )
#' )
#' df$with_columns(
#'   pl$col("date")$dt$year()$alias("year"),
#'   pl$col("date")$dt$iso_year()$alias("iso_year")
#' )
expr_dt_iso_year <- function() {
  self$`_rexpr`$dt_iso_year() |>
    wrap()
}


#' Quarter
#' @description
#' Extract quarter from underlying Date representation.
#' Applies to Date and Datetime columns.
#' Returns the quarter ranging from 1 to 4.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$date_range(
#'     as.Date("2020-12-25"),
#'     as.Date("2021-1-05"),
#'     interval = "1d",
#'     time_zone = "GMT"
#'   )
#' )
#' df$with_columns(
#'   pl$col("date")$dt$quarter()$alias("quarter")
#' )
expr_dt_quarter <- function() {
  self$`_rexpr`$dt_quarter() |>
    wrap()
}

#' Month
#' @description
#' Extract month from underlying Date representation.
#' Applies to Date and Datetime columns.
#' Returns the month number starting from 1.
#' The return value ranges from 1 to 12.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$date_range(
#'     as.Date("2020-12-25"),
#'     as.Date("2021-1-05"),
#'     interval = "1d",
#'     time_zone = "GMT"
#'   )
#' )
#' df$with_columns(
#'   pl$col("date")$dt$month()$alias("month")
#' )
expr_dt_month <- function() {
  self$`_rexpr`$dt_month() |>
    wrap()
}


#' Week
#' @description
#' Extract the week from the underlying Date representation.
#' Applies to Date and Datetime columns.
#' Returns the ISO week number starting from 1.
#' The return value ranges from 1 to 53. (The last week of year differs by years.)
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$date_range(
#'     as.Date("2020-12-25"),
#'     as.Date("2021-1-05"),
#'     interval = "1d",
#'     time_zone = "GMT"
#'   )
#' )
#' df$with_columns(
#'   pl$col("date")$dt$week()$alias("week")
#' )
expr_dt_week <- function() {
  self$`_rexpr`$dt_week() |>
    wrap()
}

#' Weekday
#' @description
#' Extract the week day from the underlying Date representation.
#' Applies to Date and Datetime columns.
#' Returns the ISO weekday number where monday = 1 and sunday = 7
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$date_range(
#'     as.Date("2020-12-25"),
#'     as.Date("2021-1-05"),
#'     interval = "1d",
#'     time_zone = "GMT"
#'   )
#' )
#' df$with_columns(
#'   pl$col("date")$dt$weekday()$alias("weekday")
#' )
expr_dt_weekday <- function() {
  self$`_rexpr`$dt_weekday() |>
    wrap()
}


#' Day
#' @description
#' Extract day from underlying Date representation.
#' Applies to Date and Datetime columns.
#' Returns the day of month starting from 1.
#' The return value ranges from 1 to 31. (The last day of month differs by months.)
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$date_range(
#'     as.Date("2020-12-25"),
#'     as.Date("2021-1-05"),
#'     interval = "1d",
#'     time_zone = "GMT"
#'   )
#' )
#' df$with_columns(
#'   pl$col("date")$dt$day()$alias("day")
#' )
expr_dt_day <- function() {
  self$`_rexpr`$dt_day() |>
    wrap()
}

#' Ordinal Day
#' @description
#' Extract ordinal day from underlying Date representation.
#' Applies to Date and Datetime columns.
#' Returns the day of year starting from 1.
#' The return value ranges from 1 to 366. (The last day of year differs by years.)
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$date_range(
#'     as.Date("2020-12-25"),
#'     as.Date("2021-1-05"),
#'     interval = "1d",
#'     time_zone = "GMT"
#'   )
#' )
#' df$with_columns(
#'   pl$col("date")$dt$ordinal_day()$alias("ordinal_day")
#' )
expr_dt_ordinal_day <- function() {
  self$`_rexpr`$dt_ordinal_day() |>
    wrap()
}


#' Hour
#' @description
#' Extract hour from underlying Datetime representation.
#' Applies to Datetime columns.
#' Returns the hour number from 0 to 23.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$datetime_range(
#'     as.Date("2020-12-25"),
#'     as.Date("2021-1-05"),
#'     interval = "1d2h",
#'     time_zone = "GMT"
#'   )
#' )
#' df$with_columns(
#'   pl$col("date")$dt$hour()$alias("hour")
#' )
expr_dt_hour <- function() {
  self$`_rexpr`$dt_hour() |>
    wrap()
}

#' Minute
#' @description
#' Extract minutes from underlying Datetime representation.
#' Applies to Datetime columns.
#' Returns the minute number from 0 to 59.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$datetime_range(
#'     as.Date("2020-12-25"),
#'     as.Date("2021-1-05"),
#'     interval = "1d5s",
#'     time_zone = "GMT"
#'   )
#' )
#' df$with_columns(
#'   pl$col("date")$dt$minute()$alias("minute")
#' )
expr_dt_minute <- function() {
  self$`_rexpr`$dt_minute() |>
    wrap()
}

#' Extract seconds from underlying Datetime representation
#'
#' Applies to Datetime columns.
#' Returns the integer second number from 0 to 59, or a floating
#' point number from 0 < 60 if `fractional=TRUE` that includes
#' any milli/micro/nanosecond component.
#' @param fractional A logical.
#' Whether to include the fractional component of the second.
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   datetime = as.POSIXct(
#'     c(
#'       "1978-01-01 01:01:01",
#'       "2024-10-13 05:30:14.500",
#'       "2065-01-01 10:20:30.06"
#'     ),
#'     "UTC"
#'   )
#' )
#'
#' df$with_columns(
#'   second = pl$col("datetime")$dt$second(),
#'   second_fractional = pl$col("datetime")$dt$second(fractional = TRUE)
#' )
expr_dt_second <- function(fractional = FALSE) {
  wrap({
    sec <- self$`_rexpr`$dt_second()
    if (fractional) {
      sec$add(self$`_rexpr`$dt_nanosecond()$div(pl$lit(1E9)$`_rexpr`))
    } else {
      sec
    }
  })
}


#' Extract milliseconds from underlying Datetime representation
#'
#' Applies to Datetime columns.
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   datetime = as.POSIXct(
#'     c(
#'       "1978-01-01 01:01:01",
#'       "2024-10-13 05:30:14.500",
#'       "2065-01-01 10:20:30.06"
#'     ),
#'     "UTC"
#'   )
#' )
#'
#' df$with_columns(
#'   millisecond = pl$col("datetime")$dt$millisecond()
#' )
expr_dt_millisecond <- function() {
  self$`_rexpr`$dt_millisecond() |>
    wrap()
}


#' Extract microseconds from underlying Datetime representation.
#' @inherit expr_dt_millisecond description return
#' @examples
#' df <- pl$DataFrame(
#'   datetime = as.POSIXct(
#'     c(
#'       "1978-01-01 01:01:01",
#'       "2024-10-13 05:30:14.500",
#'       "2065-01-01 10:20:30.06"
#'     ),
#'     "UTC"
#'   )
#' )
#'
#' df$with_columns(
#'   microsecond = pl$col("datetime")$dt$microsecond()
#' )
expr_dt_microsecond <- function() {
  self$`_rexpr`$dt_microsecond() |>
    wrap()
}


#' Extract nanoseconds from underlying Datetime representation
#' @inherit expr_dt_millisecond description return
#' @examples
#' df <- pl$DataFrame(
#'   datetime = as.POSIXct(
#'     c(
#'       "1978-01-01 01:01:01",
#'       "2024-10-13 05:30:14.500",
#'       "2065-01-01 10:20:30.06"
#'     ),
#'     "UTC"
#'   )
#' )
#'
#' df$with_columns(
#'   nanosecond = pl$col("datetime")$dt$nanosecond()
#' )
expr_dt_nanosecond <- function() {
  self$`_rexpr`$dt_nanosecond() |>
    wrap()
}


#' Epoch
#'
#' Get the time passed since the Unix EPOCH in the give time unit.
#'
#' @param time_unit Time unit, one of `"ns"`, `"us"`, `"ms"`, `"s"` or  `"d"`.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(date = pl$date_range(as.Date("2001-1-1"), as.Date("2001-1-3")))
#'
#' df$with_columns(
#'   epoch_ns = pl$col("date")$dt$epoch(),
#'   epoch_s = pl$col("date")$dt$epoch(time_unit = "s")
#' )
expr_dt_epoch <- function(time_unit = "us") {
  wrap({
    time_unit <- arg_match0(time_unit, values = c("us", "ns", "ms", "s", "d"))
    switch(time_unit,
      "ms" = ,
      "us" = ,
      "ns" = self$`_rexpr`$dt_timestamp(time_unit),
      "s" = self$`_rexpr`$dt_epoch_seconds(),
      "d" = self$`_rexpr`$cast(pl$Date$`_dt`, strict = TRUE, wrap_numerical = FALSE)$cast(pl$Int32$`_dt`, strict = TRUE, wrap_numerical = FALSE)
    )
  })
}


#' timestamp
#' @description Return a timestamp in the given time unit.
#'
#' @param tu string option either 'ns', 'us', or 'ms'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$datetime_range(
#'     start = as.Date("2001-1-1"),
#'     end = as.Date("2001-1-3"),
#'     interval = "1d1s"
#'   )
#' )
#' df$select(
#'   pl$col("date"),
#'   pl$col("date")$dt$timestamp()$alias("timestamp_ns"),
#'   pl$col("date")$dt$timestamp(tu = "ms")$alias("timestamp_ms")
#' )
expr_dt_timestamp <- function(tu = "ns") {
  wrap({
    tu <- arg_match0(tu, values = c("ns", "us", "ms"))
    self$`_rexpr`$dt_timestamp(tu)
  })
}


#' with_time_unit
#' @description  Set time unit of a Series of dtype Datetime or Duration.
#' This does not modify underlying data, and should be used to fix an incorrect time unit.
#' The corresponding global timepoint will change.
#'
#' @param tu string option either 'ns', 'us', or 'ms'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$datetime_range(
#'     start = as.Date("2001-1-1"),
#'     end = as.Date("2001-1-3"),
#'     interval = "1d1s"
#'   )
#' )
#' df$select(
#'   pl$col("date"),
#'   pl$col("date")$dt$with_time_unit()$alias("with_time_unit_ns"),
#'   pl$col("date")$dt$with_time_unit(tu = "ms")$alias("with_time_unit_ms")
#' )
expr_dt_with_time_unit <- function(tu = "ns") {
  wrap({
    tu <- arg_match0(tu, values = c("ns", "us", "ms"))
    self$`_rexpr`$dt_with_time_unit(tu)
  })
}


#' cast_time_unit
#' @description
#' Cast the underlying data to another time unit. This may lose precision.
#' The corresponding global timepoint will stay unchanged +/- precision.
#'
#'
#' @param tu string option either 'ns', 'us', or 'ms'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$datetime_range(
#'     start = as.Date("2001-1-1"),
#'     end = as.Date("2001-1-3"),
#'     interval = "1d1s"
#'   )
#' )
#' df$select(
#'   pl$col("date"),
#'   pl$col("date")$dt$cast_time_unit()$alias("cast_time_unit_ns"),
#'   pl$col("date")$dt$cast_time_unit(tu = "ms")$alias("cast_time_unit_ms")
#' )
expr_dt_cast_time_unit <- function(tu = "ns") {
  wrap({
    tu <- arg_match0(tu, values = c("ns", "us", "ms"))
    self$`_rexpr`$dt_cast_time_unit(tu)
  })
}


#' Days
#' @description Extract the days from a Duration type.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$datetime_range(
#'     start = as.Date("2020-3-1"),
#'     end = as.Date("2020-5-1"),
#'     interval = "1mo1s"
#'   )
#' )
#' df$select(
#'   pl$col("date"),
#'   diff_days = pl$col("date")$diff()$dt$total_days()
#' )
expr_dt_total_days <- function() {
  self$`_rexpr`$dt_total_days() |>
    wrap()
}

#' Hours
#' @description Extract the hours from a Duration type.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$date_range(
#'     start = as.Date("2020-1-1"),
#'     end = as.Date("2020-1-4"),
#'     interval = "1d"
#'   )
#' )
#' df$select(
#'   pl$col("date"),
#'   diff_hours = pl$col("date")$diff()$dt$total_hours()
#' )
expr_dt_total_hours <- function() {
  self$`_rexpr`$dt_total_hours() |>
    wrap()
}

#' Minutes
#' @description Extract the minutes from a Duration type.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   date = pl$date_range(
#'     start = as.Date("2020-1-1"),
#'     end = as.Date("2020-1-4"),
#'     interval = "1d"
#'   )
#' )
#' df$select(
#'   pl$col("date"),
#'   diff_minutes = pl$col("date")$diff()$dt$total_minutes()
#' )
expr_dt_total_minutes <- function() {
  self$`_rexpr`$dt_total_minutes() |>
    wrap()
}

#' Seconds
#' @description Extract the seconds from a Duration type.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(date = pl$datetime_range(
#'   start = as.POSIXct("2020-1-1", tz = "GMT"),
#'   end = as.POSIXct("2020-1-1 00:04:00", tz = "GMT"),
#'   interval = "1m"
#' ))
#' df$select(
#'   pl$col("date"),
#'   diff_sec = pl$col("date")$diff()$dt$total_seconds()
#' )
expr_dt_total_seconds <- function() {
  self$`_rexpr`$dt_total_seconds() |>
    wrap()
}

#' milliseconds
#' @description Extract the milliseconds from a Duration type.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(date = pl$datetime_range(
#'   start = as.POSIXct("2020-1-1", tz = "GMT"),
#'   end = as.POSIXct("2020-1-1 00:00:01", tz = "GMT"),
#'   interval = "1ms"
#' ))
#' df$select(
#'   pl$col("date"),
#'   diff_millisec = pl$col("date")$diff()$dt$total_milliseconds()
#' )
expr_dt_total_milliseconds <- function() {
  self$`_rexpr`$dt_total_milliseconds() |>
    wrap()
}

#' microseconds
#' @description Extract the microseconds from a Duration type.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(date = pl$datetime_range(
#'   start = as.POSIXct("2020-1-1", tz = "GMT"),
#'   end = as.POSIXct("2020-1-1 00:00:01", tz = "GMT"),
#'   interval = "1ms"
#' ))
#' df$select(
#'   pl$col("date"),
#'   diff_microsec = pl$col("date")$diff()$dt$total_microseconds()
#' )
expr_dt_total_microseconds <- function() {
  self$`_rexpr`$dt_total_microseconds() |>
    wrap()
}

#' nanoseconds
#' @description Extract the nanoseconds from a Duration type.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(date = pl$datetime_range(
#'   start = as.POSIXct("2020-1-1", tz = "GMT"),
#'   end = as.POSIXct("2020-1-1 00:00:01", tz = "GMT"),
#'   interval = "1ms"
#' ))
#' df$select(
#'   pl$col("date"),
#'   diff_nanosec = pl$col("date")$diff()$dt$total_nanoseconds()
#' )
expr_dt_total_nanoseconds <- function() {
  self$`_rexpr`$dt_total_nanoseconds() |>
    wrap()
}

#' Offset By
#' @description  Offset this date by a relative time offset.
#' This differs from ``pl$col("foo_datetime_tu") + value_tu`` in that it can
#' take months and leap years into account. Note that only a single minus
#' sign is allowed in the ``by`` string, as the first character.
#'
#'
#' @param by optional string encoding duration see details.
#'
#'
#' @details
#' The ``by`` are created with the the following string language:
#' - 1ns # 1 nanosecond
#' - 1us # 1 microsecond
#' - 1ms # 1 millisecond
#' - 1s  # 1 second
#' - 1m  # 1 minute
#' - 1h  # 1 hour
#' - 1d  # 1 day
#' - 1w  # 1 calendar week
#' - 1mo # 1 calendar month
#' - 1y  # 1 calendar year
#' - 1i  # 1 index count
#'
#' These strings can be combined:
#'   - 3d12h4m25s # 3 days, 12 hours, 4 minutes, and 25 seconds
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   dates = pl$date_range(
#'     as.Date("2000-1-1"),
#'     as.Date("2005-1-1"),
#'     "1y"
#'   )
#' )
#' df$select(
#'   pl$col("dates")$dt$offset_by("1y")$alias("date_plus_1y"),
#'   pl$col("dates")$dt$offset_by("-1y2mo")$alias("date_min")
#' )
#'
#' # the "by" argument also accepts expressions
#' df <- pl$DataFrame(
#'   dates = pl$datetime_range(
#'     as.POSIXct("2022-01-01", tz = "GMT"),
#'     as.POSIXct("2022-01-02", tz = "GMT"),
#'     interval = "6h", time_unit = "ms", time_zone = "GMT"
#'   )$to_r(),
#'   offset = c("1d", "-2d", "1mo", NA, "1y")
#' )
#'
#' df
#'
#' df$with_columns(new_dates = pl$col("dates")$dt$offset_by(pl$col("offset")))
expr_dt_offset_by <- function(by) {
  self$`_rexpr`$dt_offset_by(as_polars_expr(by, as_lit = TRUE)$`_rexpr`) |>
    wrap()
}


#' Extract time from a Datetime Series
#'
#' This only works on Datetime Series, it will error on Date Series.
#'
#' @inherit as_polars_expr return
#'
#'
#' @examples
#' df <- pl$DataFrame(dates = pl$datetime_range(
#'   as.Date("2000-1-1"),
#'   as.Date("2000-1-2"),
#'   "1h"
#' ))
#'
#' df$with_columns(times = pl$col("dates")$dt$time())
expr_dt_time <- function() {
  self$`_rexpr`$dt_time() |>
    wrap()
}

#' Determine whether the year of the underlying date is a leap year
#'
#' @inherit as_polars_expr return
#'
#' @examples
#' df <- pl$DataFrame(date = as.Date(c("2000-01-01", "2001-01-01", "2002-01-01")))
#'
#' df$with_columns(
#'   leap_year = pl$col("date")$dt$is_leap_year()
#' )
expr_dt_is_leap_year <- function() {
  self$`_rexpr`$dt_is_leap_year() |>
    wrap()
}

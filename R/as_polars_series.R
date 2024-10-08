# TODO: link to data type docs
# TODO: link to the type mapping vignette
#' Create a Polars Series from an R object
#'
#' The [as_polars_series()] function creates a [polars Series][Series] from various R objects.
#' The Data Type of the Series is determined by the class of the input object.
#'
#' The default method of [as_polars_series()] throws an error,
#' so we need to define S3 methods for the classes we want to support.
#'
#' ## S3 method for [list] and [list] based classes
#'
#' In R, a [list] can contain elements of different types, but in Polars (Apache Arrow),
#' all elements must have the same type.
#' So the [as_polars_series()] function automatically casts all elements to the same type
#' or throws an error, depending on the `strict` argument.
#' If you want to create a list with all elements of the same type in R,
#' consider using the [vctrs::list_of()] function.
#'
#' Since a [list] can contain another [list], the `strict` argument is also used
#' when creating [Series] from the inner [list] in the case of classes constructed on top of a [list],
#' such as [data.frame] or [vctrs_rcrd][vctrs::new_rcrd].
#'
#' ## S3 method for [Date]
#'
#' Sinse polars Data type's unit is day and physical type is integer,
#' sub-day precision of the R's [Date] object will be ignored.
#'
#' ## S3 method for [hms][hms::hms]
#'
#' If the [hms][hms::hms] vector contains values greater-equal to 24-oclock or less than 0-oclock,
#' an error will be thrown.
#'
#' ## S3 method for [polars_data_frame][DataFrame]
#'
#' This method is a shortcut for [`<DataFrame>$to_struct()`][dataframe__to_struct].
#' @param x An R object.
#' @param name A single string or `NULL`. Name of the Series.
#' Will be used as a column name when used in a [polars DataFrame][DataFrame].
#' When not specified, name is set to an empty string.
#' @param ... Additional arguments passed to the methods.
#' @param strict A logical value to indicate whether throwing an error when
#' the input [list]'s elements have different data types.
#' If `FALSE` (default), all elements are automatically cast to the super type, or,
#' casting to the super type is failed, the value will be `null`.
#' If `TRUE`, the first non-`NULL` element's data type is used as the data type of the inner Series.
#' @return A [polars Series][Series]
#' @seealso
#' - [`<Series>$to_r_vector()`][series__to_r_vector]: Export the Series as an R vector.
#' - [as_polars_df()]: Create a Polars DataFrame from an R object.
#' @examples
#' # double
#' as_polars_series(c(NA, 1, 2))
#'
#' # integer
#' as_polars_series(c(NA, 1:2))
#'
#' # character
#' as_polars_series(c(NA, "foo", "bar"))
#'
#' # logical
#' as_polars_series(c(NA, TRUE, FALSE))
#'
#' # raw
#' as_polars_series(charToRaw("foo"))
#'
#' # factor
#' as_polars_series(factor(c(NA, "a", "b")))
#'
#' # Date
#' as_polars_series(as.Date(c(NA, "2021-01-01")))
#'
#' # POSIXct with timezone
#' as_polars_series(as.POSIXct(c(NA, "2021-01-01 00:00:00"), "UTC"))
#'
#' # POSIXct without timezone
#' as_polars_series(as.POSIXct(c(NA, "2021-01-01 00:00:00")))
#'
#' # difftime
#' as_polars_series(as.difftime(c(NA, 1), units = "days"))
#'
#' # NULL
#' as_polars_series(NULL)
#'
#' # list
#' as_polars_series(list(NA, NULL, list(), 1, "foo", TRUE))
#'
#' ## 1st element will be `null` due to the casting failure
#' as_polars_series(list(list("bar"), "foo"))
#'
#' # data.frame
#' as_polars_series(
#'   data.frame(x = 1:2, y = c("foo", "bar"), z = I(list(1, 2)))
#' )
#'
#' # vctrs_unspecified
#' if (requireNamespace("vctrs", quietly = TRUE)) {
#'   as_polars_series(vctrs::unspecified(3L))
#' }
#'
#' # hms
#' if (requireNamespace("hms", quietly = TRUE)) {
#'   as_polars_series(hms::as_hms(c(NA, "01:00:00")))
#' }
#'
#' # blob
#' if (requireNamespace("blob", quietly = TRUE)) {
#'   as_polars_series(blob::as_blob(c(NA, "foo", "bar")))
#' }
#'
#' # integer64
#' if (requireNamespace("bit64", quietly = TRUE)) {
#'   as_polars_series(bit64::as.integer64(c(NA, "9223372036854775807")))
#' }
#'
#' # clock_naive_time
#' if (requireNamespace("clock", quietly = TRUE)) {
#'   as_polars_series(clock::naive_time_parse(c(
#'     NA,
#'     "1900-01-01T12:34:56.123456789",
#'     "2020-01-01T12:34:56.123456789"
#'   ), precision = "nanosecond"))
#' }
#'
#' # clock_duration
#' if (requireNamespace("clock", quietly = TRUE)) {
#'   as_polars_series(clock::duration_nanoseconds(c(NA, 1)))
#' }
#' @export
as_polars_series <- function(x, name = NULL, ...) {
  UseMethod("as_polars_series")
}

#' @rdname as_polars_series
#' @export
as_polars_series.default <- function(x, name = NULL, ...) {
  abort(
    paste0("Unsupported class for `as_polars_series()`: ", toString(class(x))),
    call = parent.frame()
  )
}

#' @rdname as_polars_series
#' @export
as_polars_series.polars_series <- function(x, name = NULL, ...) {
  if (is.null(name)) {
    x
  } else {
    x$rename(name)
  }
}

#' @rdname as_polars_series
#' @export
as_polars_series.polars_data_frame <- function(x, name = NULL, ...) {
  x$to_struct(name = name %||% "")
}

#' @rdname as_polars_series
#' @export
as_polars_series.double <- function(x, name = NULL, ...) {
  PlRSeries$new_f64(name %||% "", x) |>
    wrap()
}

#' @rdname as_polars_series
#' @export
as_polars_series.integer <- function(x, name = NULL, ...) {
  PlRSeries$new_i32(name %||% "", x) |>
    wrap()
}

#' @rdname as_polars_series
#' @export
as_polars_series.character <- function(x, name = NULL, ...) {
  PlRSeries$new_str(name %||% "", x) |>
    wrap()
}

#' @rdname as_polars_series
#' @export
as_polars_series.logical <- function(x, name = NULL, ...) {
  PlRSeries$new_bool(name %||% "", x) |>
    wrap()
}

#' @rdname as_polars_series
#' @export
as_polars_series.raw <- function(x, name = NULL, ...) {
  PlRSeries$new_single_binary(name %||% "", x) |>
    wrap()
}

#' @rdname as_polars_series
#' @export
as_polars_series.factor <- function(x, name = NULL, ...) {
  PlRSeries$new_str(name %||% "", as.character(x))$cast(
    pl$Categorical()$`_dt`,
    strict = TRUE
  ) |>
    wrap()
}

#' @rdname as_polars_series
#' @export
as_polars_series.Date <- function(x, name = NULL, ...) {
  wrap({
    # Date is based on integer or double
    new_series_fn <- if (is_integer(x)) {
      PlRSeries$new_i32
    } else {
      PlRSeries$new_f64
    }

    new_series_fn(name %||% "", x)$cast(
      pl$Date$`_dt`,
      strict = TRUE
    )
  })
}

#' @rdname as_polars_series
#' @export
as_polars_series.POSIXct <- function(x, name = NULL, ...) {
  wrap({
    tzone <- attr(x, "tzone") %||% ""
    name <- name %||% ""

    # POSIXct is based on integer or double
    new_series_fn <- if (is_integer(x)) {
      PlRSeries$new_i32
    } else {
      PlRSeries$new_f64
    }

    int_series <- new_series_fn(name, x)$mul(
      PlRSeries$new_f64("", 1000)
    )$cast(pl$Int64$`_dt`, strict = TRUE)

    if (tzone == "") {
      # TODO: simplify to remove the need for the `wrap()` function
      wrap(int_series)$to_frame()$select(
        pl$col(name)$cast(
          pl$Datetime("ms", "UTC")
        )$dt$convert_time_zone(
          Sys.timezone()
        )$dt$replace_time_zone(
          NULL,
          ambiguous = "raise", non_existent = "raise"
        )
      )$to_series()
    } else {
      int_series$cast(
        pl$Datetime("ms", tzone)$`_dt`,
        strict = TRUE
      )
    }
  })
}

#' @rdname as_polars_series
#' @export
as_polars_series.difftime <- function(x, name = NULL, ...) {
  mul_value <- switch(attr(x, "units"),
    "secs" = 1000L,
    "mins" = 60L * 1000L,
    "hours" = 60L * 60L * 1000L,
    "days" = 24L * 60L * 60L * 1000L,
    "weeks" = 7L * 24L * 60L * 60L * 1000L,
    abort("Unsupported `units` attribute of the difftime object.")
  )

  PlRSeries$new_f64(name %||% "", x)$mul(
    PlRSeries$new_i32("", mul_value)
  )$cast(
    pl$Duration("ms")$`_dt`,
    strict = TRUE
  ) |>
    wrap()
}

#' @rdname as_polars_series
#' @export
as_polars_series.hms <- function(x, name = NULL, ...) {
  wrap({
    # TODO: should not panic in upstream
    if (any((x >= 86400L | 0 > x), na.rm = TRUE)) {
      abort("`hms` class object contains values greater-equal to 24-oclock or less than 0-oclock is not supported")
    }

    PlRSeries$new_f64(name %||% "", x)$mul(
      PlRSeries$new_i32("", 1000000000L)
    )$cast(
      pl$Time$`_dt`,
      strict = TRUE
    )
  })
}

#' @rdname as_polars_series
#' @export
as_polars_series.blob <- function(x, name = NULL, ...) {
  PlRSeries$new_binary(name %||% "", x) |>
    wrap()
}

#' @rdname as_polars_series
#' @export
as_polars_series.array <- function(x, name = NULL, ...) {
  dims <- dim(x) |>
    rev()
  NextMethod()$reshape(dims)
}

#' @rdname as_polars_series
#' @export
as_polars_series.NULL <- function(x, name = NULL, ...) {
  wrap({
    if (missing(x)) {
      abort("The `x` argument of `as_polars_series()` can't be missing")
    }
    PlRSeries$new_null(name %||% "", 0L)
  })
}

#' @rdname as_polars_series
#' @export
as_polars_series.list <- function(x, name = NULL, ..., strict = FALSE) {
  series_list <- lapply(x, \(child) {
    if (is.null(child)) {
      NULL
    } else {
      as_polars_series(child, ..., strict = strict)$`_s`
    }
  })

  PlRSeries$new_series_list(name %||% "", series_list, strict = strict) |>
    wrap()
}

#' @rdname as_polars_series
#' @export
as_polars_series.AsIs <- function(x, name = NULL, ...) {
  class(x) <- setdiff(class(x), "AsIs")
  as_polars_series(x, name = name, ...)
}

#' @rdname as_polars_series
#' @export
as_polars_series.data.frame <- function(x, name = NULL, ...) {
  as_polars_df(x, ...)$to_struct(name = name %||% "")
}

#' @rdname as_polars_series
#' @export
as_polars_series.integer64 <- function(x, name = NULL, ...) {
  PlRSeries$new_i64(name %||% "", x) |>
    wrap()
}

#' @rdname as_polars_series
#' @export
as_polars_series.ITime <- function(x, name = NULL, ...) {
  PlRSeries$new_i32(name %||% "", x)$mul(
    PlRSeries$new_f64("", 1000000000)
  )$cast(
    pl$Time$`_dt`,
    strict = TRUE
  ) |>
    wrap()
}

#' @rdname as_polars_series
#' @export
as_polars_series.vctrs_unspecified <- function(x, name = NULL, ...) {
  PlRSeries$new_null(name %||% "", length(x)) |>
    wrap()
}

#' @rdname as_polars_series
#' @export
as_polars_series.vctrs_rcrd <- function(x, name = NULL, ...) {
  internal_data <- vctrs::fields(x) |>
    lapply(\(field_name) {
      vctrs::field(x, field_name) |>
        as_polars_series(name = field_name, ...)
    })

  as_polars_df(internal_data)$to_struct(name = name %||% "")
}

#' @rdname as_polars_series
#' @export
as_polars_series.clock_time_point <- function(x, name = NULL, ...) {
  precision <- clock::time_point_precision(x)

  time_unit <- switch(precision,
    nanosecond = "ns",
    microsecond = "us",
    "ms"
  )

  left <- vctrs::field(x, "lower")
  right <- vctrs::field(x, "upper")

  PlRSeries$new_i64_from_clock_pair(
    name %||% "",
    left,
    right,
    precision
  )$cast(
    PlRDataType$new_datetime(time_unit, NULL),
    strict = TRUE
  ) |>
    wrap()
}

#' @rdname as_polars_series
#' @export
as_polars_series.clock_sys_time <- function(x, name = NULL, ...) {
  as_polars_series.clock_time_point(x, name = name, ...)$dt$replace_time_zone("UTC")
}

#' @rdname as_polars_series
#' @export
as_polars_series.clock_zoned_time <- function(x, name = NULL, ...) {
  time_zone <- clock::zoned_time_zone(x)

  if (isTRUE(time_zone == "")) {
    # https://github.com/r-lib/clock/issues/366
    time_zone <- Sys.timezone()
  }

  as_polars_series.clock_time_point(
    clock::as_naive_time(x),
    name = name,
    ...
  )$dt$replace_time_zone(time_zone)
}

#' @rdname as_polars_series
#' @export
as_polars_series.clock_duration <- function(x, name = NULL, ...) {
  precision <- clock::duration_precision(x)

  time_unit <- switch(precision,
    nanosecond = "ns",
    microsecond = "us",
    "ms"
  )

  left <- vctrs::field(x, "lower")
  right <- vctrs::field(x, "upper")

  PlRSeries$new_i64_from_clock_pair(
    name %||% "",
    left,
    right,
    precision
  )$cast(
    PlRDataType$new_duration(time_unit),
    strict = TRUE
  ) |>
    wrap()
}

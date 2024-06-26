#' @export
as_polars_series <- function(x, name = NULL, ...) {
  UseMethod("as_polars_series")
}

#' @export
as_polars_series.default <- function(x, name = NULL, ...) {
  classes <- class(x)
  abort(
    paste0("Unsupported class for `as_polars_series()`: ", paste(classes, collapse = ", ")),
    call = parent.frame()
  )
}

#' @export
as_polars_series.polars_series <- function(x, name = NULL, ...) {
  if (is.null(name)) {
    x
  } else {
    x$rename(name)
  }
}

#' @export
as_polars_series.double <- function(x, name = NULL, ...) {
  PlRSeries$new_f64(name %||% "", x) |>
    wrap()
}

#' @export
as_polars_series.integer <- function(x, name = NULL, ...) {
  PlRSeries$new_i32(name %||% "", x) |>
    wrap()
}

#' @export
as_polars_series.character <- function(x, name = NULL, ...) {
  PlRSeries$new_str(name %||% "", x) |>
    wrap()
}

#' @export
as_polars_series.logical <- function(x, name = NULL, ...) {
  PlRSeries$new_bool(name %||% "", x) |>
    wrap()
}

#' @export
as_polars_series.factor <- function(x, name = NULL, ...) {
  PlRSeries$new_str(name %||% "", as.character(x))$cast(
    pl$Categorical()$`_dt`,
    strict = TRUE
  ) |>
    wrap()
}

#' @export
as_polars_series.Date <- function(x, name = NULL, ...) {
  PlRSeries$new_f64(name %||% "", x)$cast(
    pl$Date$`_dt`,
    strict = TRUE
  ) |>
    wrap()
}

#' @export
as_polars_series.POSIXct <- function(x, name = NULL, ...) {
  tzone <- attr(x, "tzone")
  name <- name %||% ""

  int_series <- PlRSeries$new_f64(name, x)$mul(
    PlRSeries$new_f64("", 1000)
  )$cast(pl$Int64$`_dt`, strict = TRUE)

  if (tzone == "") {
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
    ) |>
      wrap()
  }
}

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

#' @export
as_polars_series.hms <- function(x, name = NULL, ...) {
  PlRSeries$new_f64(name %||% "", x)$mul(
    PlRSeries$new_i32("", 1000000000L)
  )$cast(
    pl$Time$`_dt`,
    strict = TRUE
  ) |>
    wrap()
}

#' @export
as_polars_series.array <- function(x, name = NULL, ...) {
  dims <- dim(x) |>
    rev()
  NextMethod()$reshape(dims)
}

#' @export
as_polars_series.NULL <- function(x, name = NULL, ...) {
  PlRSeries$new_empty(name %||% "") |>
    wrap()
}

#' @export
as_polars_series.list <- function(x, name = NULL, ...) {
  series_list <- lapply(x, \(child) {
    if (is.null(child)) {
      NULL
    } else {
      as_polars_series(child)$`_s`
    }
  })

  PlRSeries$new_series_list(name %||% "", series_list) |>
    wrap()
}

#' @export
as_polars_series.AsIs <- function(x, name = NULL, ...) {
  class(x) <- setdiff(class(x), "AsIs")
  as_polars_series(x, name = name)
}

#' @export
as_polars_series.data.frame <- function(x, name = NULL, ...) {
  as_polars_df(x)$to_struct(name = name %||% "")
}

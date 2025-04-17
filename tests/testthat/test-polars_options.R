test_that("default options", {
  polars_options_reset()
  skip_if_not_installed("hms")
  library(hms)
  expect_snapshot(polars_options())
  try(detach("package:hms"), silent = TRUE)
})

test_that("options are validated", {
  polars_options_reset()
  withr::with_options(
    list(polars.conversion_int64 = "foobar"),
    expect_snapshot(print(polars_options()), error = TRUE)
  )
  withr::with_options(
    list(polars.df_knitr_print = "foobar"),
    expect_snapshot(print(polars_options()), error = TRUE)
  )
  withr::with_options(
    list(polars.conversion_uint8 = "foobar"),
    expect_snapshot(print(polars_options()), error = TRUE)
  )
  withr::with_options(
    list(polars.conversion_date = "foobar"),
    expect_snapshot(print(polars_options()), error = TRUE)
  )
  withr::with_options(
    list(polars.conversion_time = "foobar"),
    expect_snapshot(print(polars_options()), error = TRUE)
  )
  withr::with_options(
    list(polars.conversion_ambiguous = "foobar"),
    expect_snapshot(print(polars_options()), error = TRUE)
  )
  withr::with_options(
    list(polars.conversion_non_existent = "foobar"),
    expect_snapshot(print(polars_options()), error = TRUE)
  )
})

test_that("option 'conversion_int64' works", {
  polars_options_reset()
  df <- pl$DataFrame(a = c(1:3, NA))$cast(a = pl$Int64)

  # Default is to convert Int64 to float.
  expect_identical(
    as.list(df, as_series = FALSE),
    list(a = c(1, 2, 3, NA))
  )

  # can convert to character
  withr::with_options(
    list(polars.conversion_int64 = "character"),
    {
      expect_message(
        expect_identical(
          as.list(df, as_series = FALSE),
          list(a = c("1", "2", "3", NA))
        ),
        r"(Using `int64 = "character"`)"
      )
    }
  )

  # can convert to bit64, but *only* if bit64 is attached
  try(detach("package:bit64"), silent = TRUE)
  withr::with_options(
    list(polars.conversion_int64 = "integer64"),
    expect_snapshot(polars_options(), error = TRUE)
  )

  skip_if_not_installed("bit64")
  suppressPackageStartupMessages(library(bit64))

  withr::with_options(
    list(polars.conversion_int64 = "integer64"),
    expect_message(
      expect_identical(
        as.list(df, as_series = FALSE),
        list(a = as.integer64(c(1, 2, 3, NA)))
      ),
      r"(Using `int64 = "integer64"`)"
    )
  )

  # can override the global option by passing a custom arg
  # option currently is "integer64"
  expect_identical(
    as.list(df, int64 = "character", as_series = FALSE),
    list(a = c("1", "2", "3", NA))
  )

  # check other S3 methods
  withr::with_options(
    list(polars.conversion_int64 = "character"),
    {
      expect_message(
        expect_identical(
          as.data.frame(df),
          data.frame(a = c("1", "2", "3", NA))
        ),
        r"(Using `int64 = "character"`)"
      )
      expect_message(
        expect_identical(
          as.vector(pl$Series("a", c(1:3, NA))$cast(pl$Int64)),
          c("1", "2", "3", NA)
        ),
        r"(Using `int64 = "character"`)"
      )
      skip_if_not_installed("tibble")
      expect_message(
        expect_identical(
          tibble::as_tibble(df),
          tibble::tibble(a = c("1", "2", "3", NA))
        ),
        r"(Using `int64 = "character"`)"
      )
    }
  )
})

test_that("option 'conversion_date' works", {
  polars_options_reset()
  df <- pl$DataFrame(a = as.Date("2020-01-01"))

  # TODO: expect_identical() fails because x is integer and y is double?
  expect_equal(
    as.list(df, as_series = FALSE),
    list(a = as.Date("2020-01-01"))
  )

  try(detach("package:data.table"), silent = TRUE)
  withr::with_options(
    list(polars.conversion_date = "IDate"),
    expect_snapshot(polars_options(), error = TRUE)
  )

  skip_if_not_installed("data.table")
  suppressPackageStartupMessages(library(data.table))

  withr::with_options(
    list(polars.conversion_date = "IDate"),
    expect_message(
      expect_identical(
        as.list(df, as_series = FALSE),
        list(a = data.table::as.IDate("2020-01-01"))
      ),
      r"(Using `date = "IDate"`)"
    )
  )
})

test_that("option 'conversion_time' works", {
  skip_if_not_installed("hms")
  polars_options_reset()
  df <- pl$DataFrame(a = hms::hms(1, 1, 1))

  try(detach("package:hms"), silent = TRUE)
  expect_snapshot(polars_options(), error = TRUE)

  suppressPackageStartupMessages(library(hms))

  withr::with_options(
    list(polars.conversion_time = "hms"),
    expect_identical(
      as.list(df, as_series = FALSE),
      list(a = hms::hms(1, 1, 1))
    )
  )

  try(detach("package:data.table"), silent = TRUE)
  withr::with_options(
    list(polars.conversion_time = "ITime"),
    expect_snapshot(polars_options(), error = TRUE)
  )

  skip_if_not_installed("data.table")
  suppressPackageStartupMessages(library(data.table))

  withr::with_options(
    list(polars.conversion_time = "ITime"),
    expect_message(
      expect_identical(
        as.list(df, as_series = FALSE),
        list(a = data.table::as.ITime("01:01:01"))
      ),
      r"(Using `time = "ITime"`)"
    )
  )
})

test_that("option 'conversion_uint8' works", {
  polars_options_reset()
  df <- pl$DataFrame(a = 1)$cast(pl$UInt8)

  expect_identical(
    as.list(df, as_series = FALSE),
    list(a = 1L)
  )

  withr::with_options(
    list(polars.conversion_uint8 = "raw"),
    expect_message(
      expect_identical(
        as.list(df, as_series = FALSE),
        list(a = as.raw(1L))
      ),
      r"(Using `uint8 = "raw"`)"
    )
  )
})

# TODO: how to test conversion of ambiguous or non-existent dates from polars
# to R?

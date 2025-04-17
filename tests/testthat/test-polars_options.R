test_that("default options", {
  polars_options_reset()
  expect_snapshot(polars_options())
})

test_that("options are validated", {
  polars_options_reset()
  withr::with_options(
    list(polars.int64_conversion = "foobar"),
    expect_snapshot(print(polars_options()), error = TRUE)
  )
  withr::with_options(
    list(polars.df_knitr_print = "foobar"),
    expect_snapshot(print(polars_options()), error = TRUE)
  )
})

test_that("option 'int64_conversion ' works", {
  polars_options_reset()
  df <- pl$DataFrame(a = c(1:3, NA))$cast(a = pl$Int64)

  # Default is to convert Int64 to float.
  expect_identical(
    as.list(df, as_series = FALSE),
    list(a = c(1, 2, 3, NA))
  )

  # can convert to character
  withr::with_options(
    list(polars.int64_conversion = "character"),
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
    list(polars.int64_conversion = "integer64"),
    expect_snapshot(polars_options(), error = TRUE)
  )

  skip_if_not_installed("bit64")
  suppressPackageStartupMessages(library(bit64))

  withr::with_options(
    list(polars.int64_conversion = "integer64"),
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

  # arg correctly passed from as.data.frame() to as.list()
  expect_identical(
    as.data.frame(df, int64 = "character"),
    data.frame(a = c("1", "2", "3", NA))
  )
})

test_that("options are validated", {
  polars_options_reset()
  withr::with_options(
    list(polars.to_r_vector_int64 = "foobar"),
    expect_snapshot(print(polars_options()), error = TRUE)
  )
  withr::with_options(
    list(polars.df_knitr_print = "foobar"),
    expect_snapshot(print(polars_options()), error = TRUE)
  )
  withr::with_options(
    list(polars.to_r_vector_uint8 = "foobar"),
    expect_snapshot(print(polars_options()), error = TRUE)
  )
  withr::with_options(
    list(polars.to_r_vector_date = "foobar"),
    expect_snapshot(print(polars_options()), error = TRUE)
  )
  withr::with_options(
    list(polars.to_r_vector_time = "foobar"),
    expect_snapshot(print(polars_options()), error = TRUE)
  )
  withr::with_options(
    list(polars.to_r_vector_ambiguous = "foobar"),
    expect_snapshot(print(polars_options()), error = TRUE)
  )
  withr::with_options(
    list(polars.to_r_vector_non_existent = "foobar"),
    expect_snapshot(print(polars_options()), error = TRUE)
  )
})

test_that("option 'to_r_vector_int64' works", {
  polars_options_reset()
  df <- pl$DataFrame(a = c(1:3, NA))$cast(a = pl$Int64)

  # Default is to convert Int64 to float.
  expect_identical(
    as.list(df, as_series = FALSE),
    list(a = c(1, 2, 3, NA))
  )

  # can convert to character
  withr::with_options(
    list(polars.to_r_vector_int64 = "character"),
    {
      expect_snapshot(
        expect_identical(
          as.list(df, as_series = FALSE),
          list(a = c("1", "2", "3", NA))
        )
      )
    }
  )

  # bit64 is automatically loaded if required
  withr::with_options(
    list(polars.to_r_vector_int64 = "integer64"),
    expect_snapshot(
      expect_identical(
        as.list(df, as_series = FALSE),
        list(a = bit64::as.integer64(c(1, 2, 3, NA)))
      )
    )
  )

  # can override the global option by passing a custom arg (no message in this
  # case)
  withr::with_options(
    list(polars.to_r_vector_int64 = "integer64"),
    expect_silent(
      expect_identical(
        as.list(df, int64 = "character", as_series = FALSE),
        list(a = c("1", "2", "3", NA))
      )
    )
  )

  # check other S3 methods
  withr::with_options(
    list(polars.to_r_vector_int64 = "character"),
    {
      expect_snapshot(
        expect_identical(
          as.data.frame(df),
          data.frame(a = c("1", "2", "3", NA))
        )
      )
      expect_snapshot(
        expect_identical(
          as.vector(pl$Series("a", c(1:3, NA))$cast(pl$Int64)),
          c("1", "2", "3", NA)
        )
      )
      skip_if_not_installed("tibble")
      expect_snapshot(
        expect_identical(
          tibble::as_tibble(df),
          tibble::tibble(a = c("1", "2", "3", NA))
        )
      )
    }
  )

  # error if wrong option
  withr::with_options(
    list(polars.to_r_vector_int64 = complex(1)),
    expect_snapshot(as.list(df, as_series = FALSE), error = TRUE)
  )
})

test_that("option 'to_r_vector_date' works", {
  polars_options_reset()
  df <- pl$DataFrame(a = as.Date("2020-01-01"))

  # TODO: expect_identical() fails because x is integer and y is double?
  expect_equal(
    as.list(df, as_series = FALSE),
    list(a = as.Date("2020-01-01"))
  )

  # data.table is automatically loaded if required
  withr::with_options(
    list(polars.to_r_vector_date = "IDate"),
    expect_snapshot(
      expect_identical(
        as.list(df, as_series = FALSE),
        list(a = data.table::as.IDate("2020-01-01"))
      )
    )
  )
})

test_that("option 'to_r_vector_time' works", {
  skip_if_not_installed("hms")
  skip_if_not_installed("data.table")
  polars_options_reset()
  df <- pl$DataFrame(a = hms::hms(1, 1, 1))

  # hms is automatically loaded if required
  withr::with_options(
    list(polars.to_r_vector_time = "hms"),
    expect_identical(
      as.list(df, as_series = FALSE),
      list(a = hms::hms(1, 1, 1))
    )
  )

  # data.table is automatically loaded if required
  withr::with_options(
    list(polars.to_r_vector_time = "ITime"),
    expect_identical(
      as.list(df, as_series = FALSE),
      list(a = data.table::as.ITime("01:01:01"))
    )
  )
})

test_that("option 'to_r_vector_uint8' works", {
  polars_options_reset()
  df <- pl$DataFrame(a = 1)$cast(pl$UInt8)

  expect_identical(
    as.list(df, as_series = FALSE),
    list(a = 1L)
  )

  withr::with_options(
    list(polars.to_r_vector_uint8 = "raw"),
    expect_snapshot(
      expect_identical(
        as.list(df, as_series = FALSE),
        list(a = as.raw(1L))
      )
    )
  )
})

# TODO: how to test conversion of ambiguous or non-existent dates from polars
# to R?

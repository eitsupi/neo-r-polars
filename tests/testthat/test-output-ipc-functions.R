patrick::with_parameters_test_that(
  "Test sinking data to IPC file",
  {
    lf <- as_polars_lf(iris)
    df <- as_polars_df(iris)
    tmpf <- withr::local_tempfile()
    expect_silent(lf$sink_ipc(tmpf, compression = compression))
    expect_equal(pl$read_ipc(tmpf), df)

    # update with new data
    lf$slice(5, 5)$sink_ipc(tmpf)
    expect_equal(
      pl$read_ipc(tmpf),
      df$slice(5, 5)
    )

    # return the input data
    x <- lf$sink_ipc(tmpf)
    expect_identical(x, lf)
  },
  compression = list("uncompressed", "zstd", "lz4", NULL)
)

test_that("sink_ipc: wrong compression", {
  lf <- as_polars_lf(iris)
  tmpf <- withr::local_tempfile()
  expect_error(
    lf$sink_ipc(tmpf, compression = "rar"),
    "must be one of"
  )
})

patrick::with_parameters_test_that(
  "Test writing data to IPC file",
  {
    df <- as_polars_df(iris)
    tmpf <- withr::local_tempfile()
    expect_silent(df$write_ipc(tmpf, compression = compression))
    expect_equal(pl$read_ipc(tmpf), df)

    # update with new data
    df$slice(5, 5)$write_ipc(tmpf)
    expect_equal(
      pl$read_ipc(tmpf),
      df$slice(5, 5)
    )

    # return the input data
    x <- df$write_ipc(tmpf)
    expect_identical(x, df)
  },
  compression = list("uncompressed", "zstd", "lz4", NULL)
)

test_that("write_ipc: wrong compression", {
  df <- as_polars_df(iris)
  tmpf <- withr::local_tempfile()
  expect_error(
    df$write_ipc(tmpf, compression = "rar"),
    "must be one of"
  )
})

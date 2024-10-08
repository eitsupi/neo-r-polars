test_that("select works lazy/eager", {
  .data <- pl$DataFrame(
    int32 = 1:5,
    int64 = as_polars_series(1:5)$cast(pl$Int64),
    string = letters[1:5],
  )

  expect_query_equal(
    .input$select("int32"),
    .data,
    pl$DataFrame(int32 = 1:5)
  )
  expect_query_equal(
    .input$select(pl$lit("int32")),
    .data,
    pl$DataFrame(literal = "int32")
  )
  expect_query_equal(
    .input$select(foo = "int32"),
    .data,
    pl$DataFrame(foo = 1:5)
  )
})

test_that("POLARS_AUTO_STRUCTIFY works for select", {
  .data <- pl$DataFrame(
    foo = 1:3,
    bar = 6:8,
    ham = letters[1:3],
  )

  withr::with_envvar(
    c(POLARS_AUTO_STRUCTIFY = "foo"),
    {
      expect_query_error(
        .input$select(1),
        .data,
        r"(Environment variable `POLARS_AUTO_STRUCTIFY` must be one of \('0', '1'\), got 'foo')"
      )
    }
  )

  withr::with_envvar(
    c(POLARS_AUTO_STRUCTIFY = "0"),
    {
      expect_query_error(
        .input$select(is_odd = ((pl$col(pl$Int32) %% 2) == 1)$name$suffix("_is_odd")),
        .data,
        "`keep`, `suffix`, `prefix` should be last expression"
      )

      expect_query_equal(
        withr::with_envvar(c(POLARS_AUTO_STRUCTIFY = "1"), {
          .input$select(is_odd = ((pl$col(pl$Int32) %% 2) == 1)$name$suffix("_is_odd"))
        }),
        .data,
        as_polars_lf(.data)$select(
          is_odd = pl$struct(((pl$col(pl$Int32) %% 2) == 1)$name$suffix("_is_odd")),
        )$collect()
      )
    }
  )
})

test_that("slice/head/tail works lazy/eager", {
  .data <- pl$DataFrame(
    foo = 1:5,
    bar = 6:10,
  )

  # slice
  expect_query_equal(
    .input$slice(1),
    .data,
    pl$DataFrame(foo = 2:5, bar = 7:10)
  )
  expect_query_equal(
    .input$slice(1, 2),
    .data,
    pl$DataFrame(foo = 2:3, bar = 7:8)
  )
  expect_query_equal(
    .input$slice(1, 2),
    .data,
    pl$DataFrame(foo = 2:3, bar = 7:8)
  )
  expect_query_equal(
    .input$slice(4, 100),
    .data,
    pl$DataFrame(foo = 5L, bar = 10L)
  )
  expect_eager_equal_lazy_error(
    .input$slice(0, -2),
    .data,
    pl$DataFrame(foo = 1:3, bar = 6:8),
    r"(negative slice length \(-2\) are invalid for LazyFrame)"
  )

  # head
  expect_query_equal(
    .input$head(1),
    .data,
    pl$DataFrame(foo = 1L, bar = 6L)
  )
  expect_query_equal(
    .input$head(100),
    .data,
    .data
  )
  expect_eager_equal_lazy_error(
    .input$head(-4),
    .data,
    pl$DataFrame(foo = 1L, bar = 6L),
    r"(negative slice length \(-4\) are invalid for LazyFrame)"
  )

  # tail
  expect_query_equal(
    .input$tail(1),
    .data,
    pl$DataFrame(foo = 5L, bar = 10L)
  )
  expect_query_equal(
    .input$tail(100),
    .data,
    .data
  )
  expect_eager_equal_lazy_error(
    .input$tail(-4),
    .data,
    pl$DataFrame(foo = 5L, bar = 10L),
    r"(Value `-4\.0` is too small to be converted to u32)"
  )
})

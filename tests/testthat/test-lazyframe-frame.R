#############################################
### [GENERATED AUTOMATICALLY] Update test-dataframe-frame.R instead.
#############################################

withr::with_envvar(
    list(POLARS_IN_LAZY_TEST = TRUE),
    {
patrick::with_parameters_test_that(
  "use pl$LazyFrame() to construct a DataFrame",
  .cases = {
    tibble::tribble(
      ~.test_name, ~object, ~expected,
      "simple", pl$LazyFrame(a = 1, b = list("b"), ), as_polars_lf(list(a = 1, b = list("b"))),
      "!!! for list", pl$LazyFrame(!!!list(a = 1, b = list("b")), c = 1), as_polars_lf(list(a = 1, b = list("b"), c = 1)),
      "!!! for data.frame", pl$LazyFrame(!!!data.frame(a = 1, b = "b"), c = 1), as_polars_lf(list(a = 1, b = "b", c = 1)),
      "empty", pl$LazyFrame(), as_polars_lf(list()),
    )
  },
  code = {
    expect_equal_lazy(object, expected)
  }
)

test_that("pl$LazyFrame() requires series the same length", {
  expect_error_lazy(pl$LazyFrame(a = 1:2, b = "foo"), "has length 2")
})

test_that("pl$LazyFrame() rejects expressions", {
  expect_error_lazy(
    pl$LazyFrame(a = 1:2, b = pl$lit("foo")),
    r"(Try evaluating the expression first using `pl\$select\(\)`)"
  )
})

test_that("to_struct()", {
  skip_if(is_in_lazy_test())
  expect_equal_lazy(
    as_polars_lf(mtcars)$to_struct("foo"),
    as_polars_series(mtcars, "foo")
  )
})

test_that("get_columns()", {
  skip_if(is_in_lazy_test())
  expect_equal_lazy(
    pl$LazyFrame(a = 1:2, b = c("foo", "bar"))$get_columns(),
    list(
      a = as_polars_series(1:2, "a"),
      b = as_polars_series(c("foo", "bar"), "b")
    )
  )
})

test_that("to_series()", {
  skip_if(is_in_lazy_test())
  data <- data.frame(
    a = 1:2,
    b = c("foo", "bar")
  )

  expect_equal_lazy(
    as_polars_lf(data)$to_series(),
    as_polars_series(data$a, "a")
  )
  expect_equal_lazy(
    as_polars_lf(data)$to_series(1),
    as_polars_series(data$b, "b")
  )
})

test_that("select works lazy/eager", {
  .data <- pl$LazyFrame(
    int32 = 1:5,
    int64 = as_polars_series(1:5)$cast(pl$Int64),
    string = letters[1:5],
  )

  expect_equal_lazy(
    .data$select("int32"),
    pl$LazyFrame(int32 = 1:5)
  )
  expect_equal_lazy(
    .data$select(pl$lit("int32")),
    pl$LazyFrame(literal = "int32")
  )
  expect_equal_lazy(
    .data$select(foo = "int32"),
    pl$LazyFrame(foo = 1:5)
  )
})

test_that("POLARS_AUTO_STRUCTIFY works for select", {
  .data <- pl$LazyFrame(
    foo = 1:3,
    bar = 6:8,
    ham = letters[1:3],
  )

  withr::with_envvar(
    c(POLARS_AUTO_STRUCTIFY = "foo"),
    {
      expect_error_lazy(
        .data$select(1),
        r"(Environment variable `POLARS_AUTO_STRUCTIFY` must be one of \('0', '1'\), got 'foo')"
      )
    }
  )

  withr::with_envvar(
    c(POLARS_AUTO_STRUCTIFY = "0"),
    {
      expect_error_lazy(
        .data$select(is_odd = ((pl$col(pl$Int32) %% 2) == 1)$name$suffix("_is_odd")),
        "`keep`, `suffix`, `prefix` should be last expression"
      )

      expect_equal_lazy(
        withr::with_envvar(c(POLARS_AUTO_STRUCTIFY = "1"), {
          .data$select(is_odd = ((pl$col(pl$Int32) %% 2) == 1)$name$suffix("_is_odd"))
        }),
        as_polars_lf(.data)$select(
          is_odd = pl$struct(((pl$col(pl$Int32) %% 2) == 1)$name$suffix("_is_odd")),
        )$collect()
      )
    }
  )
})

test_that("slice/head/tail work", {
  .data <- pl$LazyFrame(
    foo = 1:5,
    bar = 6:10,
  )

  # slice
  expect_equal_lazy(
    .data$slice(1),
    pl$LazyFrame(foo = 2:5, bar = 7:10)
  )
  expect_equal_lazy(
    .data$slice(1, 2),
    pl$LazyFrame(foo = 2:3, bar = 7:8)
  )
  expect_equal_lazy(
    .data$slice(1, 2),
    pl$LazyFrame(foo = 2:3, bar = 7:8)
  )
  expect_equal_lazy(
    .data$slice(4, 100),
    pl$LazyFrame(foo = 5L, bar = 10L)
  )
  if (is_in_lazy_test()) {
    expect_error_lazy(
      .data$slice(0, -2),
      r"(-2.0 is out of range that can be safely converted to u32)"
    )
  } else {
    expect_equal_lazy(
      .data$slice(0, -2),
      pl$LazyFrame(foo = 1:3, bar = 6:8)
    )
  }

  # head
  expect_equal_lazy(
    .data$head(1),
    pl$LazyFrame(foo = 1L, bar = 6L)
  )
  expect_equal_lazy(
    .data$head(100),
    .data
  )
  if (is_in_lazy_test()) {
    expect_error_lazy(
      .data$head(-4),
      r"(-4.0 is out of range that can be safely converted to u32)"
    )
  } else {
    expect_equal_lazy(
      .data$head(-4),
      pl$LazyFrame(foo = 1L, bar = 6L)
    )
  }

  # tail
  expect_equal_lazy(
    .data$tail(1),
    pl$LazyFrame(foo = 5L, bar = 10L)
  )
  expect_equal_lazy(
    .data$tail(100),
    .data
  )
  if (is_in_lazy_test()) {
    expect_error_lazy(
      .data$tail(-4),
      r"(-4\.0 is out of range that can be safely converted to u32)"
    )
  } else {
    expect_equal_lazy(
      .data$tail(-4),
      pl$LazyFrame(foo = 5L, bar = 10L)
    )
  }
})


test_that("unnest works correctly", {
  df <- pl$LazyFrame(
    a = 1:5,
    b = c("one", "two", "three", "four", "five"),
    c = 6:10
  )$
    select(
    foo = pl$lit(1),
    pl$struct("b"),
    pl$struct(c("a", "c"))$alias("a_and_c")
  )

  expect_identical(
    df$unnest("b", "a_and_c"),
    df$unnest(c("b", "a_and_c"))
  )

  # wrong input
  expect_snapshot_lazy(
    df$unnest("b", pl$col("a_and_c")),
    error = TRUE
  )
  expect_snapshot_lazy(df$unnest(1), error = TRUE)

  # wrong datatype
  expect_snapshot_lazy(df$unnest("foo"), error = TRUE)
})
}
  )
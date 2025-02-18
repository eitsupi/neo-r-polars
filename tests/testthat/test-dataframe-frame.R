patrick::with_parameters_test_that(
  "use pl$DataFrame() to construct a DataFrame",
  .cases = {
    tibble::tribble(
      ~.test_name, ~object, ~expected,
      "simple", pl$DataFrame(a = 1, b = list("b"), ), as_polars_df(list(a = 1, b = list("b"))),
      "!!! for list", pl$DataFrame(!!!list(a = 1, b = list("b")), c = 1), as_polars_df(list(a = 1, b = list("b"), c = 1)),
      "!!! for data.frame", pl$DataFrame(!!!data.frame(a = 1, b = "b"), c = 1), as_polars_df(list(a = 1, b = "b", c = 1)),
      "empty", pl$DataFrame(), as_polars_df(list()),
    )
  },
  code = {
    expect_equal(object, expected)
  }
)

test_that("pl$DataFrame() requires series the same length", {
  expect_error(pl$DataFrame(a = 1:2, b = "foo"), "has length 2")
})

test_that("pl$DataFrame() rejects expressions", {
  expect_error(
    pl$DataFrame(a = 1:2, b = pl$lit("foo")),
    r"(Try evaluating the expression first using `pl\$select\(\)`)"
  )
})

test_that("to_struct()", {
  expect_equal(
    as_polars_df(mtcars)$to_struct("foo"),
    as_polars_series(mtcars, "foo")
  )
})

test_that("get_columns()", {
  expect_equal(
    pl$DataFrame(a = 1:2, b = c("foo", "bar"))$get_columns(),
    list(
      a = as_polars_series(1:2, "a"),
      b = as_polars_series(c("foo", "bar"), "b")
    )
  )
})

test_that("to_series()", {
  data <- data.frame(
    a = 1:2,
    b = c("foo", "bar")
  )

  expect_equal(
    as_polars_df(data)$to_series(),
    as_polars_series(data$a, "a")
  )
  expect_equal(
    as_polars_df(data)$to_series(1),
    as_polars_series(data$b, "b")
  )
})

test_that("flags work", {
  df <- pl$DataFrame(a = c(2, 1), b = c(3, 4), c = list(c(1, 2), 4))
  expect_identical(
    df$sort("a")$flags,
    list(
      a = c(SORTED_ASC = TRUE, SORTED_DESC = FALSE),
      b = c(SORTED_ASC = FALSE, SORTED_DESC = FALSE),
      c = c(SORTED_ASC = FALSE, SORTED_DESC = FALSE, FAST_EXPLODE = FALSE)
    )
  )
  expect_identical(
    df$with_columns(pl$col("b")$implode())$flags,
    list(
      a = c(SORTED_ASC = FALSE, SORTED_DESC = FALSE),
      b = c(SORTED_ASC = FALSE, SORTED_DESC = FALSE, FAST_EXPLODE = TRUE),
      c = c(SORTED_ASC = FALSE, SORTED_DESC = FALSE, FAST_EXPLODE = TRUE)
    )
  )

  # no FAST_EXPLODE for array
  df <- pl$DataFrame(
    a = list(c(1, 2), c(4, 5)),
    .schema_overrides = list(a = pl$Array(pl$Float64, 2))
  )
  expect_identical(
    df$flags,
    list(a = c(SORTED_ASC = FALSE, SORTED_DESC = FALSE))
  )
})

test_that("partition_by() works", {
  df <- pl$DataFrame(
    a = c("a", "b", "a", "b", "c"),
    b = c(1, 2, 1, 3, 3),
    c = c(5, 4, 3, 2, 1)
  )
  expect_equal(
    df$partition_by("a"),
    list(
      pl$DataFrame(a = c("a", "a"), b = c(1, 1), c = c(5, 3)),
      pl$DataFrame(a = c("b", "b"), b = c(2, 3), c = c(4, 2)),
      pl$DataFrame(a = "c", b = 3, c = 1)
    )
  )
  expect_equal(
    df$partition_by("a", "b"),
    list(
      pl$DataFrame(a = c("a", "a"), b = c(1, 1), c = c(5, 3)),
      pl$DataFrame(a = "b", b = 2, c = 4),
      pl$DataFrame(a = "b", b = 3, c = 2),
      pl$DataFrame(a = "c", b = 3, c = 1)
    )
  )
  # arg "include_key"
  expect_equal(
    df$partition_by("a", include_key = FALSE),
    list(
      pl$DataFrame(b = c(1, 1), c = c(5, 3)),
      pl$DataFrame(b = c(2, 3), c = c(4, 2)),
      pl$DataFrame(b = 3, c = 1)
    )
  )
  # errors
  expect_error(
    df$partition_by(),
    "must contain at least one column name"
  )
  expect_error(
    df$partition_by("a", NA),
    "only accepts column names"
  )
  expect_error(
    df$partition_by(pl$col("a") + 1),
    "only accepts column names"
  )
  expect_error(
    df$partition_by(foo = "a"),
    "must be passed by position"
  )
  expect_error(
    df$partition_by("a", include_key = 42),
    "must be logical, not double"
  )
})

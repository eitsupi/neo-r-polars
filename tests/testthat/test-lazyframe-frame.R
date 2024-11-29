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
        )
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
    r"(-2.0 is out of range that can be safely converted to u32)"
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
    r"(-4.0 is out of range that can be safely converted to u32)"
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
    r"(-4.0 is out of range that can be safely converted to u32)"
  )
})

test_that("shift works lazy/eager", {
  .data <- as_polars_df(mtcars[1:3, 1:2])
  expect_query_equal(
    .input$shift(2),
    .data,
    pl$DataFrame(mpg = c(NA, NA, 21), cyl = c(NA, NA, 6))
  )
  expect_query_equal(
    .input$shift(2, fill_value = 999),
    .data,
    pl$DataFrame(mpg = c(999, 999, 21), cyl = c(999, 999, 6))
  )
})

# TODO-REWRITE: add $join() for DataFrame
test_that("joins work lazy/eager", {
  df <- pl$DataFrame(
    foo = 1:3,
    bar = c(6, 7, 8),
    ham = c("a", "b", "c")
  )

  other_df <- pl$DataFrame(
    apple = c("x", "y", "z"),
    ham = c("a", "b", "d")
  )

  expect_query_equal(
    .input$join(.input2, on = "ham"),
    .input = df, .input2 = other_df,
    pl$DataFrame(
      foo = 1:2,
      bar = c(6, 7),
      ham = c("a", "b"),
      apple = c("x", "y")
    )
  )
})

test_that("sort()", {
  expect_query_error(
    .input$sort(complex(1)),
    pl$DataFrame(x = 1),
    "Unsupported class"
  )
  expect_query_error(
    .input$sort(by = complex(1)),
    pl$DataFrame(x = 1),
    "must be passed by position"
  )
  expect_query_error(
    .input$sort(),
    pl$DataFrame(x = 1),
    "at least one element"
  )
  # `descending` and `nulls_last` need either 1 or as many booleans as items
  expect_query_error(
    .input$sort("cyl", "mpg", "drat", descending = c(TRUE, FALSE)),
    pl$DataFrame(x = 1),
    "does not match"
  )
  expect_query_error(
    .input$sort("cyl", "mpg", "drat", nulls_last = c(TRUE, FALSE)),
    pl$DataFrame(x = 1),
    "does not match"
  )

  # `descending` and `nulls_last` can only take booleans
  expect_query_error(
    .input$sort("cyl", "mpg", "drat", descending = 42),
    pl$DataFrame(x = 1),
    "must be a logical vector"
  )
  expect_query_error(
    .input$sort("cyl", "mpg", "drat", descending = NULL),
    pl$DataFrame(x = 1),
    "must be a logical vector"
  )
  expect_query_error(
    .input$sort("cyl", "mpg", "drat", nulls_last = 42),
    pl$DataFrame(x = 1),
    "must be a logical vector"
  )
  expect_query_error(
    .input$sort("cyl", "mpg", "drat", nulls_last = NULL),
    pl$DataFrame(x = 1),
    "must be a logical vector"
  )

  df <- pl$DataFrame(
    x = c(3, 3, 4, 1, 2),
    y = c(2, 1, 5, 4, 3)
  )
  expect_query_equal(
    .input$sort("x", maintain_order = TRUE),
    df,
    pl$DataFrame(x = c(1, 2, 3, 3, 4), y = c(4, 3, 2, 1, 5))
  )
  expect_query_equal(
    .input$sort(pl$col("x"), maintain_order = TRUE),
    df,
    pl$DataFrame(x = c(1, 2, 3, 3, 4), y = c(4, 3, 2, 1, 5))
  )
  # several columns
  expect_query_equal(
    .input$sort("x", "y", maintain_order = TRUE),
    df,
    pl$DataFrame(x = c(1, 2, 3, 3, 4), y = c(4, 3, 1, 2, 5))
  )
  expect_query_equal(
    .input$sort(pl$col("x"), pl$col("y"), maintain_order = TRUE),
    df,
    pl$DataFrame(x = c(1, 2, 3, 3, 4), y = c(4, 3, 1, 2, 5))
  )
  expect_query_equal(
    .input$sort(c("x", "y"), maintain_order = TRUE),
    df,
    pl$DataFrame(x = c(1, 2, 3, 3, 4), y = c(4, 3, 1, 2, 5))
  )

  # descending arg
  expect_query_equal(
    .input$sort("x", "y", maintain_order = TRUE, descending = TRUE),
    df,
    pl$DataFrame(x = c(4, 3, 3, 2, 1), y = c(5, 2, 1, 3, 4))
  )

  # descending arg: vector of boolean
  expect_query_equal(
    .input$sort("x", "y", maintain_order = TRUE, descending = c(TRUE, FALSE)),
    df,
    pl$DataFrame(x = c(4, 3, 3, 2, 1), y = c(5, 1, 2, 3, 4))
  )

  # expr: one increasing and one decreasing
  expect_query_equal(
    .input$sort(-pl$col("x"), pl$col("y"), maintain_order = TRUE),
    df,
    pl$DataFrame(x = c(4, 3, 3, 2, 1), y = c(5, 1, 2, 3, 4))
  )

  # nulls_last
  df <- pl$DataFrame(
    x = c(NA, 3, 4, 1, 2),
    y = c(2, 1, 5, 4, 3)
  )
  expect_query_equal(
    .input$sort("x", "y", maintain_order = TRUE, nulls_last = TRUE),
    df,
    pl$DataFrame(x = c(1, 2, 3, 4, NA), y = c(4, 3, 1, 5, 2))
  )
  expect_query_equal(
    .input$sort("x", "y", maintain_order = TRUE, nulls_last = FALSE),
    df,
    pl$DataFrame(x = c(NA, 1, 2, 3, 4), y = c(2, 4, 3, 1, 5))
  )
})

# TODO-REWRITE: add $rename() for DataFrame
patrick::with_parameters_test_that(
  "rename()",
  {
    dat <- do.call(fun, list(mtcars))
    dat2 <- dat$rename(mpg = "miles_per_gallon", hp = "horsepower")
    if (is_polars_lf(dat2)) {
      dat2 <- dat2$collect()
    }
    nms <- names(dat2)
    expect_false("hp" %in% nms)
    expect_false("mpg" %in% nms)
    expect_true("miles_per_gallon" %in% nms)
    expect_true("horsepower" %in% nms)

    expect_error(
      dat$rename(),
      "must be character, not NULL"
    )
  },
  fun = c("as_polars_df", "as_polars_lf")
)

# TODO-REWRITE: requires $name$map()
# patrick::with_parameters_test_that(
#   "rename()",
#   {
#     dat <- do.call(fun, list(data.frame(foo = 1:3, bar = 6:8, ham = letters[1:3])))
#     dat2 <- dat$rename(
#       \(column_name) paste0("c", substr(column_name, 2, 100))
#     )
#     if (is_polars_lf(dat2)) {
#       dat2 <- dat2$collect()
#     }
#     expect_named(dat2, c("coo", "car", "cam"))
#   },
#   fun = c("as_polars_df", "as_polars_lf")
# )

test_that("explode", {
  df <- pl$DataFrame(
    letters = c("a", "a", "b", "c"),
    numbers = list(1, c(2, 3), c(4, 5), c(6, 7, 8)),
    jumpers = list(1, c(2, 3), c(4, 5), c(6, 7, 8))
  )

  expected_df <- pl$DataFrame(
    letters = c(rep("a", 3), "b", "b", rep("c", 3)),
    numbers = 1:8,
    jumpers = 1:8
  )

  expect_query_equal(
    .input$explode(c("numbers", "jumpers")),
    df,
    expected_df
  )
  expect_query_equal(
    df$explode("numbers", pl$col("jumpers")),
    df,
    expected_df
  )

  # empty values -> NA
  df <- pl$DataFrame(
    letters = c("a", "a", "b", "c"),
    numbers = list(1, NULL, c(4, 5), c(6, 7, 8))
  )
  expect_query_equal(
    .input$explode("numbers"),
    df,
    pl$DataFrame(
      letters = c(rep("a", 2), "b", "b", rep("c", 3)),
      numbers = c(1, NA, 4:8)
    )
  )

  # several cols to explode test2
  df <- pl$DataFrame(
    letters = c("a", "a", "b", "c"),
    numbers = list(1, NULL, c(4, 5), c(6, 7, 8)),
    numbers2 = list(1, NULL, c(4, 5), c(6, 7, 8))
  )
  expect_query_equal(
    .input$explode("numbers", pl$col("numbers2")),
    df,
    pl$DataFrame(
      letters = c(rep("a", 2), "b", "b", rep("c", 3)),
      numbers = c(1, NA, 4:8),
      numbers2 = c(1, NA, 4:8)
    )
  )
})

test_that("with_row_index", {
  expect_query_equal(
    .input$with_row_index("idx", 42),
    pl$DataFrame(x = 1:3),
    pl$DataFrame(idx = 42:44, x = 1:3)$cast(idx = pl$UInt32)
  )
})

test_that("rolling: date variable", {
  df <- pl$DataFrame(
    dt = c(
      "2020-01-01", "2020-01-01", "2020-01-01",
      "2020-01-02", "2020-01-03", "2020-01-08"
    ),
    a = c(3, 7, 5, 9, 2, 1)
  )$with_columns(
    pl$col("dt")$str$strptime(pl$Date, format = NULL)
  )

  expect_query_equal(
    .input$rolling(index_column = "dt", period = "2d")$agg(
      pl$sum("a")$alias("sum_a"),
      pl$min("a")$alias("min_a"),
      pl$max("a")$alias("max_a")
    )$select("sum_a", "min_a", "max_a"),
    df,
    pl$DataFrame(
      sum_a = c(15, 15, 15, 24, 11, 1),
      min_a = c(3, 3, 3, 3, 2, 1),
      max_a = c(7, 7, 7, 9, 9, 1)
    )
  )
})

test_that("rolling: datetime variable", {
  df <- pl$DataFrame(
    dt = c(
      "2020-01-01 13:45:48", "2020-01-01 16:42:13", "2020-01-01 16:45:09",
      "2020-01-02 18:12:48", "2020-01-03 19:45:32", "2020-01-08 23:16:43"
    ),
    a = c(3, 7, 5, 9, 2, 1)
  )$with_columns(
    pl$col("dt")$str$strptime(pl$Datetime("ms"), format = NULL)
  )

  expect_query_equal(
    .input$rolling(index_column = "dt", period = "2d")$agg(
      pl$sum("a")$alias("sum_a"),
      pl$min("a")$alias("min_a"),
      pl$max("a")$alias("max_a")
    )$select("sum_a", "min_a", "max_a"),
    df,
    pl$DataFrame(
      sum_a = c(3, 10, 15, 24, 11, 1),
      min_a = c(3, 3, 3, 3, 2, 1),
      max_a = c(3, 7, 7, 9, 9, 1)
    )
  )
})

test_that("rolling: integer variable", {
  df <- pl$DataFrame(
    index = c(1L, 2L, 3L, 4L, 8L, 9L),
    a = c(3, 7, 5, 9, 2, 1)
  )

  expect_query_equal(
    .input$rolling(index_column = "index", period = "2i")$agg(
      pl$sum("a")$alias("sum_a"),
      pl$min("a")$alias("min_a"),
      pl$max("a")$alias("max_a")
    )$select("sum_a", "min_a", "max_a"),
    df,
    pl$DataFrame(
      sum_a = c(3, 10, 12, 14, 2, 3),
      min_a = c(3, 3, 5, 5, 2, 1),
      max_a = c(3, 7, 7, 9, 2, 2)
    )
  )
})

test_that("rolling: using difftime as period", {
  df <- pl$DataFrame(
    dt = as.Date(c(
      "2020-01-01", "2020-01-01", "2020-01-01",
      "2020-01-02", "2020-01-03", "2020-01-08"
    )),
    a = c(3, 7, 5, 9, 2, 1)
  )
  expected <- pl$DataFrame(
    dt = as.Date(c(
      "2020-01-01", "2020-01-01", "2020-01-01",
      "2020-01-02", "2020-01-03", "2020-01-08"
    )),
    sum_a = c(15, 15, 15, 24, 11, 1)
  )

  expect_query_equal(
    .input$rolling(index_column = "dt", period = "2d")$agg(
      pl$sum("a")$alias("sum_a")
    ),
    df,
    expected
  )
  expect_query_equal(
    .input$rolling(index_column = "dt", period = as.difftime(2, units = "days"))$agg(
      pl$sum("a")$alias("sum_a")
    ),
    df,
    expected
  )
})

test_that("rolling: error if period is negative", {
  df <- pl$DataFrame(
    index = c(1L, 2L, 3L, 4L, 8L, 9L),
    a = c(3, 7, 5, 9, 2, 1)
  )
  expect_query_error(
    .input$rolling(index_column = "index", period = "-2i")$agg(pl$col("a")),
    df,
    "rolling window period should be strictly positive"
  )
})

test_that("rolling: argument 'group_by' works", {
  df <- pl$DataFrame(
    index = c(1L, 2L, 3L, 4L, 8L, 9L),
    grp = c("a", "a", rep("b", 4)),
    a = c(3, 7, 5, 9, 2, 1)
  )
  expect_query_equal(
    .input$rolling(index_column = "index", period = "2i", group_by = pl$col("grp"))$agg(
      pl$sum("a")$alias("sum_a"),
      pl$min("a")$alias("min_a"),
      pl$max("a")$alias("max_a")
    )$select("sum_a", "min_a", "max_a"),
    df,
    pl$DataFrame(
      sum_a = c(3, 10, 5, 14, 2, 3),
      min_a = c(3, 3, 5, 5, 2, 1),
      max_a = c(3, 7, 5, 9, 2, 2)
    )
  )

  # string is parsed as column name in "group_by"
  expect_query_equal(
    .input$rolling(index_column = "index", period = "2i", group_by = "grp")$agg(
      pl$sum("a")$alias("sum_a"),
      pl$min("a")$alias("min_a"),
      pl$max("a")$alias("max_a")
    )$select("sum_a", "min_a", "max_a"),
    df,
    pl$DataFrame(
      sum_a = c(3, 10, 5, 14, 2, 3),
      min_a = c(3, 3, 5, 5, 2, 1),
      max_a = c(3, 7, 5, 9, 2, 2)
    )
  )
})
test_that("rolling for LazyFrame: error if index not int or date/time", {
  df <- pl$LazyFrame(
    index = c(1:5, 6.0),
    a = c(3, 7, 5, 9, 2, 1)
  )

  expect_grepl_error(
    df$rolling(index_column = "index", period = "2i")$agg(
      pl$sum("a")$alias("sum_a")
    )$collect()
  )
})

test_that("rolling: arg 'offset' works", {
  df <- pl$DataFrame(
    dt = as.Date(c(
      "2020-01-01", "2020-01-01", "2020-01-01",
      "2020-01-02", "2020-01-03", "2020-01-08"
    )),
    a = c(3, 7, 5, 9, 2, 1)
  )

  expect_query_equal(
    .input$rolling(index_column = "dt", period = "2d", offset = "1d")$agg(
      pl$sum("a")$alias("sum_a"),
      pl$min("a")$alias("min_a"),
      pl$max("a")$alias("max_a")
    )$select("sum_a", "min_a", "max_a"),
    df,
    pl$DataFrame(
      sum_a = c(2, 2, 2, NA, NA, NA),
      min_a = c(2, 2, 2, NA, NA, NA),
      max_a = c(2, 2, 2, NA, NA, NA)
    )
  )
})

test_that("rolling: can be ungrouped", {
  df <- pl$DataFrame(
    index = c(1:5, 6.0),
    a = c(3, 7, 5, 9, 2, 1)
  )
  expect_query_equal(
    .input$rolling(index_column = "dt", period = "2i")$ungroup(),
    df,
    df
  )
})

test_that("with_columns_seq", {
  df <- pl$DataFrame(x = 1:2)

  expect_query_equal(
    .input$with_columns_seq(y = list(1:2, 3:4)),
    df,
    pl$DataFrame(x = 1:2, y = list(1:2, 3:4))
  )

  expect_query_equal(
    .input$with_columns_seq(y = list(1:2, 3:4), z = list(c("a", "b"), c("c", "d"))),
    df,
    pl$DataFrame(x = 1:2, y = list(1:2, 3:4), z = list(c("a", "b"), c("c", "d")))
  )
})

test_that("$clear() works", {
  df <- pl$DataFrame(
    a = c(NA, 2),
    b = c("a", NA),
    c = c(TRUE, TRUE)
  )

  expect_query_equal(
    .input$clear(),
    df,
    pl$DataFrame(a = numeric(0), b = character(0), c = logical(0))
  )

  # n > number of rows
  expect_query_equal(
    .input$clear(3),
    df,
    pl$DataFrame(a = rep(NA_real_, 3), b = rep(NA_character_, 3), c = rep(NA, 3))
  )

  # error
  expect_query_error(
    .input$clear(-1),
    df,
    "greater or equal to 0"
  )
})

test_that("$explain() works", {
  lazy_query <- as_polars_lf(iris)$sort("Species")$filter(pl$col("Species") != "setosa")

  expect_error(
    lazy_query$explain(format = "foobar"),
    "`format` must be one of"
  )
  expect_error(
    lazy_query$explain(format = 1),
    "`format` must be a string or character vector"
  )

  expect_snapshot(cat(lazy_query$explain(optimized = FALSE)))
  expect_snapshot(cat(lazy_query$explain()))

  expect_snapshot(cat(lazy_query$explain(format = "tree", optimized = FALSE)))
  expect_snapshot(cat(lazy_query$explain(format = "tree", )))
})

test_that("$gather_every() works", {
  df <- pl$DataFrame(a = 1:4, b = 5:8)

  expect_query_equal(
    .input$gather_every(2),
    df,
    pl$DataFrame(a = c(1L, 3L), b = c(5L, 7L))
  )
  expect_query_equal(
    .input$gather_every(2, offset = 1),
    df,
    pl$DataFrame(a = c(2L, 4L), b = c(6L, 8L))
  )

  # must specify n
  expect_query_error(
    .input$gather_every(),
    df,
    r"(argument "n" is missing)"
  )

  # offset must be positive
  expect_query_error(
    .input$gather_every(2, offset = -1),
    df,
    "cannot be less than zero"
  )
  expect_query_error(
    .input$gather_every(2, offset = "a"),
    df,
    "Expected a value of type"
  )
})

test_that("$cast() works", {
  df <- pl$DataFrame(
    foo = 1:3,
    bar = c(6, 7, 8),
    ham = as.Date(c("2020-01-02", "2020-03-04", "2020-05-06"))
  )

  expect_query_equal(
    .input$cast(foo = pl$Float32, bar = pl$UInt8),
    df,
    pl$DataFrame(
      foo = 1:3,
      bar = c(6, 7, 8),
      ham = as.Date(c("2020-01-02", "2020-03-04", "2020-05-06")),
      .schema_overrides = list(foo = pl$Float32, bar = pl$UInt8, ham = pl$Date)
    )
  )

  expect_query_equal(
    .input$cast(pl$String),
    df,
    pl$DataFrame(
      foo = 1:3,
      bar = c(6, 7, 8),
      ham = as.Date(c("2020-01-02", "2020-03-04", "2020-05-06")),
      .schema_overrides = list(foo = pl$String, bar = pl$String, ham = pl$String)
    )
  )

  expect_query_equal(
    .input$cast(),
    df,
    df
  )

  expect_query_error(.input$cast(1), df)
  expect_query_error(.input$cast("a"), df)
  expect_query_error(.input$cast(list(foo = "a")), df)
  expect_query_error(.input$cast(list(), strict = 1), df)

  # Test overflow error
  df <- pl$DataFrame(x = 1024)

  expect_query_error(
    .input$cast(pl$Int8),
    df,
    "conversion from `f64` to `i8` failed"
  )
  expect_query_equal(
    .input$cast(pl$Int8, .strict = FALSE),
    df,
    pl$DataFrame(x = NA_integer_, .schema_overrides = list(x = pl$Int8))
  )
})

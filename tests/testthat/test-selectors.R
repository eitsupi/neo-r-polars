test_that("all", {
  expect_named(
    pl$DataFrame(dt = as.Date(c("2000-1-1")), value = 10)$select(cs$all()),
    c("dt", "value")
  )
})

test_that("alpha", {
  df <- pl$DataFrame(
    no1 = c(100, 200, 300),
    café = c("espresso", "latte", "mocha"),
    `t or f` = c(TRUE, FALSE, NA),
    hmm = c("aaa", "bbb", "ccc"),
    都市 = c("東京", "大阪", "京都")
  )

  expect_named(
    df$select(cs$alpha()),
    c("café", "hmm", "都市")
  )
  expect_named(
    df$select(cs$alpha(ascii_only = TRUE)),
    "hmm"
  )
  expect_named(
    df$select(cs$alpha(ascii_only = TRUE, ignore_spaces = TRUE)),
    c("t or f", "hmm")
  )
})

test_that("alphanumeric", {
  df <- pl$DataFrame(
    `1st_col` = c(100, 200, 300),
    flagged = c(TRUE, FALSE, TRUE),
    `00prefix` = c("01:aa", "02:bb", "03:cc"),
    `last col` = c("x", "y", "z")
  )

  expect_named(
    df$select(cs$alphanumeric()),
    c("flagged", "00prefix")
  )
  expect_named(
    df$select(cs$alphanumeric(ignore_spaces = TRUE)),
    c("flagged", "00prefix", "last col")
  )
})

test_that("binary", {
  df <- pl$DataFrame(
    a = charToRaw("hello"),
    b = "world",
    c = charToRaw("!"),
    d = ":"
  )

  expect_named(df$select(cs$binary()), c("a", "c"))
})

test_that("boolean", {
  df <- pl$DataFrame(
    a = 1:4,
    b = c(FALSE, TRUE, FALSE, TRUE)
  )

  expect_named(df$select(cs$boolean()), "b")
})

test_that("by_dtype", {
  df <- pl$DataFrame(
    dt = as.Date(c("1999-12-31", "2024-1-1", "2010-7-5")),
    value = c(1234500, 5000555, -4500000),
    other = c("foo", "bar", "foo")
  )

  expect_named(
    df$select(cs$by_dtype(pl$Date, pl$String)),
    c("dt", "other")
  )
  expect_named(
    df$select(!cs$by_dtype(pl$Date, pl$String)),
    "value"
  )
})

test_that("by_index", {
  df <- as_polars_df(mtcars)
  expect_named(
    df$select(cs$by_index(c(0, 1, -2, -1))),
    c("mpg", "cyl", "gear", "carb")
  )
})

test_that("by_name", {
  df <- pl$DataFrame(
    foo = c("x", "y"),
    bar = c(123, 456),
    baz = c(2.0, 5.5),
    zap = c(FALSE, TRUE)
  )

  expect_named(
    df$select(cs$by_name("foo", "bar")),
    c("foo", "bar")
  )
  expect_named(
    df$select(cs$by_name("baz", "moose", "foo", "bear", .require_all = FALSE)),
    c("foo", "baz")
  )
})

test_that("categorical", {
  df <- pl$DataFrame(
    foo = c("xx", "yy"),
    bar = c(123, 456),
    baz = c(2.0, 5.5),
    .schema_overrides = list(foo = pl$Categorical()),
  )
  expect_named(df$select(cs$categorical()), "foo")
})

test_that("contains", {
  df <- pl$DataFrame(
    foo = c("x", "y"),
    bar = c(123, 456),
    baz = c(2.0, 5.5),
    zap = c(FALSE, TRUE)
  )

  expect_named(
    df$select(cs$contains("ba")),
    c("bar", "baz")
  )
  expect_named(
    df$select(cs$contains(c("ba", "z"))),
    c("bar", "baz", "zap")
  )
})

test_that("date", {
  df <- pl$DataFrame(
    dtm = as.POSIXct(c("2001-5-7 10:25", "2031-12-31 00:30")),
    dt = as.Date(c("1999-12-31", "2024-8-9"))
  )
  expect_named(df$select(cs$date()), "dt")
})

test_that("datetime", {
})

test_that("decimal", {
  df <- pl$DataFrame(
    foo = c("x", "y"),
    bar = c(123, 456),
    baz = c("2.0005", "-50.5555"),
    .schema_overrides = list(
      bar = pl$Decimal(),
      baz = pl$Decimal(scale = 5, precision = 10)
    )
  )
  expect_named(
    df$select(cs$decimal()),
    c("bar", "baz")
  )
})

test_that("digit", {
  df <- pl$DataFrame(
    key = c("aaa", "bbb"),
    `2001` = 1:2,
    `2025` = 3:4
  )

  expect_named(
    df$select(cs$digit()),
    c("2001", "2025")
  )

  df <- pl$DataFrame(`१९९९` = 1999, `२०७७` = 2077, `3000` = 3000)
  expect_named(
    df$select(cs$digit()),
    c("१९९९", "२०७७", "3000")
  )
  expect_named(
    df$select(cs$digit(ascii_only = TRUE)),
    "3000"
  )
})

test_that("duration", {
  skip_if_not_installed("clock")
  df <- pl$DataFrame(
    dtm = as.POSIXct(c("2001-5-7 10:25", "2031-12-31 00:30")),
    dur_ms = clock::duration_milliseconds(1:2),
    dur_us = clock::duration_microseconds(1:2),
    dur_ns = clock::duration_nanoseconds(1:2),
  )

  expect_named(
    df$select(cs$duration()),
    c("dur_ms", "dur_us", "dur_ns")
  )
  expect_named(
    df$select(cs$duration("ms")),
    "dur_ms"
  )
  expect_named(
    df$select(cs$duration(c("ms", "ns"))),
    c("dur_ms", "dur_ns")
  )
})

test_that("ends_with", {
  df <- pl$DataFrame(
    foo = c("x", "y"),
    bar = c(123, 456),
    baz = c(2.0, 5.5),
    zap = c(FALSE, TRUE)
  )

  expect_named(
    df$select(cs$ends_with("z")),
    "baz"
  )
  expect_named(
    df$select(cs$ends_with(c("z", "r"))),
    c("baz", "bar")
  )
})

test_that("exclude", {
})

test_that("first", {
  df <- pl$DataFrame(
    foo = c("x", "y"),
    bar = c(123L, 456L),
    baz = c(2.0, 5.5),
    zap = c(FALSE, TRUE)
  )
  expect_named(df$select(cs$first()), "foo")
})

test_that("float", {
  df <- pl$DataFrame(
    foo = c("x", "y"),
    bar = c(123L, 456L),
    baz = c(2.0, 5.5),
    zap = c(FALSE, TRUE),
    .schema_overrides = list(baz = pl$Float32, zap = pl$Float64),
  )

  expect_named(
    df$select(cs$float()),
    c("baz", "zap")
  )
})

test_that("integer", {
  df <- pl$DataFrame(
    foo = c("x", "y"),
    bar = c(123L, 456L),
    baz = c(2.0, 5.5),
    zap = 0:1
  )
  expect_named(
    df$select(cs$integer()),
    c("bar", "zap")
  )
})

test_that("last", {
  df <- pl$DataFrame(
    foo = c("x", "y"),
    bar = c(123L, 456L),
    baz = c(2.0, 5.5),
    zap = c(FALSE, TRUE)
  )
  expect_named(df$select(cs$last()), "zap")
})

test_that("matches", {
  df <- pl$DataFrame(
    foo = c("x", "y"),
    bar = c(123, 456),
    baz = c(2.0, 5.5),
    zap = c(0, 1)
  )

  expect_named(
    df$select(cs$matches("[^z]a")),
    c("bar", "baz")
  )
  expect_named(
    df$select(!cs$matches(r"((?i)R|z$)")),
    c("foo", "zap")
  )
})

test_that("numeric", {
  df <- pl$DataFrame(
    foo = c("x", "y"),
    bar = c(123L, 456L),
    baz = c(2.0, 5.5),
    zap = 0:1,
    .schema_overrides = list(bar = pl$Int16, baz = pl$Float32, zap = pl$UInt8),
  )

  expect_named(
    df$select(cs$numeric()),
    c("bar", "baz", "zap")
  )
})

test_that("signed_integer", {
  df <- pl$DataFrame(
    foo = c(-123L, -456L),
    bar = c(3456L, 6789L),
    baz = c(7654L, 4321L),
    zap = c("ab", "cd"),
    .schema_overrides = list(bar = pl$UInt32, baz = pl$UInt64),
  )

  expect_named(
    df$select(cs$signed_integer()),
    "foo"
  )
})

test_that("starts_with", {
  df <- pl$DataFrame(
    foo = c("x", "y"),
    bar = c(123, 456),
    baz = c(2.0, 5.5),
    zap = c(FALSE, TRUE)
  )
  expect_named(
    df$select(cs$starts_with("b")),
    c("bar", "baz")
  )
  expect_named(
    df$select(cs$starts_with(c("b", "z"))),
    c("bar", "baz", "zap")
  )
})

test_that("string", {
  df <- pl$DataFrame(
    w = c("xx", "yy", "xx", "yy", "xx"),
    x = c(1, 2, 1, 4, -2),
    y = c(3.0, 4.5, 1.0, 2.5, -2.0),
    z = c("a", "b", "a", "b", "b")
  )$with_columns(
    z = pl$col("z")$cast(pl$Categorical("lexical"))
  )

  expect_named(
    df$select(cs$string()),
    "w"
  )
  expect_named(
    df$select(cs$string(include_categorical = TRUE)),
    c("w", "z")
  )
})

test_that("temporal", {
})

test_that("time", {
  skip_if_not_installed("hms")
  df <- pl$DataFrame(
    dtm = as.POSIXct(c("2001-5-7 10:25", "2031-12-31 00:30")),
    dt = as.Date(c("1999-12-31", "2024-8-9")),
    tm = hms::parse_hms(c("0:0:0", "23:59:59"))
  )
  expect_named(df$select(cs$time()), "tm")
})

test_that("unsigned_integer", {
  df <- pl$DataFrame(
    foo = c(-123L, -456L),
    bar = c(3456L, 6789L),
    baz = c(7654L, 4321L),
    zap = c("ab", "cd"),
    .schema_overrides = list(bar = pl$UInt32, baz = pl$UInt64),
  )

  expect_named(
    df$select(cs$unsigned_integer()),
    c("bar", "baz")
  )
})

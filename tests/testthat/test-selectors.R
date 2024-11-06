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
})

test_that("boolean", {
})

test_that("by_dtype", {
})

test_that("by_index", {
})

test_that("by_name", {
})

test_that("categorical", {
})

test_that("contains", {
})

test_that("date", {
})

test_that("datetime", {
})

test_that("decimal", {
})

test_that("digit", {
})

test_that("duration", {
})

test_that("ends_with", {
})

test_that("exclude", {
})

test_that("first", {
})

test_that("float", {
})

test_that("integer", {
})

test_that("last", {
})

test_that("matches", {
})

test_that("numeric", {
})

test_that("signed_integer", {
})

test_that("starts_with", {
})

test_that("string", {
})

test_that("temporal", {
})

test_that("time", {
})

test_that("unsigned_integer", {
})

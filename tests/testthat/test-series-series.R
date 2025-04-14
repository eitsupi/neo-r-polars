test_that("flags work", {
  s <- as_polars_series(c(2, 1, 3))
  expect_identical(
    s$flags,
    c(SORTED_ASC = FALSE, SORTED_DESC = FALSE)
  )
  expect_identical(
    s$flags,
    c(SORTED_ASC = FALSE, SORTED_DESC = FALSE)
  )

  s <- as_polars_series(list(1, 2, 3))
  expect_identical(
    s$flags,
    c(SORTED_ASC = FALSE, SORTED_DESC = FALSE, FAST_EXPLODE = TRUE)
  )
})

test_that("alias/rename works", {
  series <- pl$Series("a", 1:3)
  expect_equal(
    series$alias("b"),
    pl$Series("b", 1:3)
  )
  expect_equal(
    series$rename("b"),
    pl$Series("b", 1:3)
  )
})

test_that("rechunk() and n_chunks() work", {
  s <- as_polars_series(1:3)
  expect_identical(s$n_chunks(), 1L)

  s2 <- as_polars_series(4:6)
  s3 <- pl$concat(s, s2, rechunk = FALSE)
  expect_identical(s3$n_chunks(), 2L)

  expect_identical(s3$rechunk()$n_chunks(), 1L)
  # The original chunk size is not changed yet
  expect_identical(s3$n_chunks(), 2L)
  expect_identical(s3$rechunk(in_place = TRUE)$n_chunks(), 1L)
  # The operation above changes the original chunk size
  expect_identical(s3$n_chunks(), 1L)
})

test_that("rechunk() and chunk_lengths() work", {
  s <- as_polars_series(1:3)
  expect_identical(s$chunk_lengths(), 3L)

  s2 <- as_polars_series(4:6)
  s3 <- pl$concat(s, s2, rechunk = FALSE)
  expect_identical(s3$chunk_lengths(), c(3L, 3L))

  expect_identical(s3$rechunk()$chunk_lengths(), 6L)
})

test_that("some functions must return an R scalar", {
  s <- pl$Series("", 1:2)
  expect_identical(s$max(), 2L)
  expect_identical(s$min(), 1L)
  expect_identical(s$sum(), 3L)
  expect_identical(s$mean(), 1.5)
  expect_identical(s$product(), 2)
  expect_identical(s$quantile(0.3), 1)
  expect_identical(s$var(), 0.5)
  expect_identical(s$std(), 0.7071, tolerance = 0.0001)
  expect_identical(s$median(), 1.5)
  expect_identical(s$first(), 1L)
  expect_identical(s$last(), 2L)
  expect_identical(s$bitwise_and(), 0L)
  expect_identical(s$bitwise_or(), 3L)
  expect_identical(s$bitwise_xor(), 3L)

  # Ensure AnyValue -> R conversion in Rust doesn't panic
  s <- pl$Series("", c(TRUE, FALSE))
  expect_identical(s$bitwise_and(), FALSE)
  expect_identical(s$bitwise_or(), TRUE)
  expect_identical(s$bitwise_xor(), TRUE)
})

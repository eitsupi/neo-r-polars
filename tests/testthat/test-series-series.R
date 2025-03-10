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
  s <- pl$Series("a", c(1, 2, 3))
  expect_equal(s$n_chunks(), 1)

  s2 <- pl$Series("a", c(4, 5, 6))
  s <- pl$concat(s, s2, rechunk = FALSE)
  expect_equal(s$n_chunks(), 2)

  expect_equal(s$rechunk()$n_chunks(), 1)
})

test_that("rechunk() and chunk_lengths() work", {
  s <- pl$Series("a", c(1, 2, 3))
  expect_equal(s$chunk_lengths(), 3)

  s2 <- pl$Series("a", c(4, 5, 6))
  s <- pl$concat(s, s2, rechunk = FALSE)
  expect_equal(s$chunk_lengths(), c(3, 3))

  expect_equal(s$rechunk()$chunk_lengths(), 6)
})

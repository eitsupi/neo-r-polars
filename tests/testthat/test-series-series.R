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

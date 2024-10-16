test_that("set_ordering", {
  dat <- pl$DataFrame(x = c("z", "z", "k", "a", "b"))$cast(pl$Categorical())
  expect_equal(
    dat$with_columns(pl$col("x")$cat$set_ordering("physical")$sort()),
    pl$DataFrame(x = factor(c("z", "z", "k", "a", "b")))
  )

  expect_equal(
    dat$with_columns(pl$col("x")$cat$set_ordering("lexical")$sort()),
    pl$DataFrame(x = factor(c("a", "b", "k", "z", "z")))$cast(pl$Categorical("lexical"))
  )

  expect_snapshot(
    dat$with_columns(pl$col("x")$cat$set_ordering("foo")$sort()),
    error = TRUE
  )
})

test_that("get_categories", {
  dat <- pl$DataFrame(x = factor(c("z", "z", "k", "a", "b")))
  expect_equal(
    dat$select(pl$col("x")$cat$get_categories()),
    pl$DataFrame(x = c("z", "k", "a", "b"))
  )
})

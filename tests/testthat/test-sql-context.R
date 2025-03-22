test_that("pl$SQLContext() works", {
  expect_equal(
    pl$SQLContext(mtcars = mtcars, foo = data.frame(x = 1))$tables(),
    c("foo", "mtcars")
  )
  expect_error(
    pl$SQLContext(mtcars, foo = data.frame(x = 1)),
    "All frames in `...` must be named"
  )
  # TODO: add message pattern when this is fixed
  # https://github.com/eitsupi/neo-r-polars/issues/206
  expect_error(pl$SQLContext(a = complex(1)))
})

test_that("arg '.register_globals' works", {
  foo <- pl$DataFrame(x = 1)
  assign("foo", foo, envir = global_env())
  expect_equal(
    pl$SQLContext(mtcars = mtcars)$tables(),
    "mtcars"
  )
  expect_equal(
    pl$SQLContext(mtcars = mtcars, .register_globals = TRUE)$tables(),
    c("foo", "mtcars")
  )

  # Conflict between table passed in `$SQLContext()` and table existing in
  # environment -> the former takes precedence
  expect_equal(
    pl$SQLContext(mtcars = mtcars, foo = data.frame(x = 2), .register_globals = TRUE)$execute(
      "SELECT * FROM foo"
    )$collect(),
    pl$DataFrame(x = 2)
  )

  rm("foo", envir = global_env())
})

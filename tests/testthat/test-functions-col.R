patrick::with_parameters_test_that(
  "pl$col() works",
  .cases = {
    tibble::tribble(
      ~.test_name, ~object, ~expected_columns,
      "int8, int16", pl$col("i8", "i16"), c("i8", "i16"),
      "c(int8, int16), string", pl$col(c("i8", "i16"), "str"), c("i8", "i16", "str"),
      "wildcard", pl$col("*"), c("i8", "i16", "i32", "str", "struct"),
      "str", pl$col("str"), c("str"),
      "^str.*$", pl$col("^str.*$"), c("str", "struct"),
      "c(^str.*$, i8)", pl$col(c("^str.*$", "i8")), c("str", "struct", "i8"),
      "pl$Int8", pl$col(pl$Int8), c("i8"),
      "pl$Int8, pl$Int16", pl$col(pl$Int8, pl$Int16), c("i8", "i16"),
      "list(pl$Int8, pl$Int16)", pl$col(list(pl$Int8, pl$Int16)), c("i8", "i16"),
    )
  },
  code = {
    df <- pl$select(
      i8 = pl$lit(NULL, pl$Int8),
      i16 = pl$lit(NULL, pl$Int16),
      i32 = pl$lit(NULL, pl$Int32),
      str = pl$lit(NULL, pl$String),
      struct = pl$lit(NULL, pl$Struct()),
    )

    expect_identical(df$select(object)$columns, expected_columns)
    expect_snapshot(object)
  }
)

test_that("pl$col() input error", {
  expect_error(pl$col(NA_character_), "`NA` is not a valid column name")
  expect_error(pl$col("foo", NA_character_), "`NA` is not a valid column name")
  expect_error(pl$col(1), r"(invalid input for `pl\$col\(\)`)")
  expect_error(pl$col(list("foo")), r"(invalid input for `pl\$col\(\)`)")
  expect_error(pl$col("foo", pl$Int8), r"(invalid input for `pl\$col\(\)`)")
  expect_error(pl$col(pl$Int8, "foo"), "Expect polars data type, got character")
  expect_error(pl$col(foo = "bar"), "Arguments in `...` must be passed by position, not name")
})

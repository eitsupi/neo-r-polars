patrick::with_parameters_test_that(
  "infer_polars_dtype() works for various objects",
  x = {
    skip_if_not_installed("hms")
    skip_if_not_installed("blob")
    skip_if_not_installed("bit64")
    skip_if_not_installed("vctrs")
    skip_if_not_installed("clock")

    withr::with_timezone(
      "UTC",
      list2(
        as_polars_series(1),
        pl$DataFrame(a = 1L, b = "foo"),
        pl$LazyFrame(a = 1L, b = "foo"),
        1:10,
        integer(),
        NA,
        NA_character_,
        raw(),
        factor(),
        as.Date(NA),
        as.POSIXct(NA),
        as.POSIXlt(NA, "UTC"),
        as.difftime(integer(), units = "days"),
        numeric_version(character()),
        numeric_version(NA_character_, strict = FALSE),
        NULL,
        list(1, "foo"),
        list(NULL, 1L),
        list(),
        data.frame(a = 1L, b = "foo"),
        data.frame(a = 1L, b = I(list("foo"))),
        vctrs::unspecified(10),
        hms::hms(),
        blob::blob(),
        bit64::integer64(),
        vctrs::list_of(NULL, 1L),
        vctrs::list_of(NULL, raw(0)),
        vctrs::list_of(NULL, list(1L)),
        vctrs::list_of(NULL, list(), list("foo")),
        vctrs::list_of(NULL, data.frame(a = 1L), data.frame(b = "foo")),
        vctrs::list_of(NULL, data.frame(a = 1L), data.frame(b = I(list("foo")))),
        vctrs::new_rcrd(list(a = 1L, b = "foo")),
        vctrs::new_rcrd(list(a = 1L, b = list("foo"), c = list(list("bar")))),
        clock::naive_time_parse(NA_character_),
        clock::duration_years(NA),
      )
    )
  },
  code = {
    withr::with_timezone("UTC", {
      expect_equal(
        infer_polars_dtype(x),
        as_polars_series(x)$dtype
      )
      expect_true(is_convertible_to_polars_series(x))
      expect_true(is_convertible_to_polars_expr(x))
    })
  }
)

patrick::with_parameters_test_that(
  "infer_polars_dtype() raises an error for unsupported objects",
  # fmt: skip
  .cases = tibble::tribble(
    ~.test_name, ~x,
    "polars_expr", pl$lit(1L),
    "complex", 1i,
    "polars_dtype", pl$Null,
  ),
  code = {
    expect_snapshot(infer_polars_dtype(x), error = TRUE)
    expect_snapshot(is_convertible_to_polars_expr(x))
    expect_false(is_convertible_to_polars_series(x))
  }
)

patrick::with_parameters_test_that(
  "infer_polars_dtype(<list>)'s infer_dtype_length option works",
  # fmt: skip
  .cases = tibble::tribble(
    ~.test_name, ~infer_dtype_length, ~expected_dtype,
    "too short", 1, pl$List(pl$Int32),
    "enough", 2, pl$List(pl$String),
    "Inf", Inf, pl$List(pl$String),
  ),
  code = {
    list_to_check <- list(NULL, 1L, NULL, "foo")

    expect_equal(
      infer_polars_dtype(
        list_to_check,
        infer_dtype_length = infer_dtype_length
      ),
      expected_dtype
    )
    expect_equal(
      infer_polars_dtype(
        data.frame(a = seq_along(list_to_check), b = I(list_to_check)),
        infer_dtype_length = infer_dtype_length
      ),
      pl$Struct(a = pl$Int32, b = expected_dtype)
    )
    expect_true(is_convertible_to_polars_series(list_to_check))
    expect_true(is_convertible_to_polars_expr(list_to_check))
  }
)

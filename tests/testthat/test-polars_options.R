patrick::with_parameters_test_that(
  "options are validated by polars_options()",
  .cases = tibble::tribble(
    ~.test_name,
    "polars.df_knitr_print",
    "polars.to_r_vector.uint8",
    "polars.to_r_vector.int64",
    "polars.to_r_vector.date",
    "polars.to_r_vector.time",
    "polars.to_r_vector.struct",
    "polars.to_r_vector.decimal",
    "polars.to_r_vector.as_clock_class",
    "polars.to_r_vector.ambiguous",
    "polars.to_r_vector.non_existent",
  ),
  {
    withr::with_options(
      list("foo") |>
        rlang::set_names(.test_name),
      expect_snapshot(print(polars_options()), error = TRUE)
    )
  }
)

patrick::with_parameters_test_that(
  "options for to_r_vector() works: {opt_name} = {opt_value}",
  .cases = {
    skip_if_not_installed("vctrs")
    skip_if_not_installed("bit64")
    skip_if_not_installed("data.table")
    skip_if_not_installed("hms")
    skip_if_not_installed("clock")

    # fmt: skip
    tibble::tribble(
      ~opt_name, ~opt_value,
      "duppy", NULL, # work around for creating a list column
      "polars.to_r_vector.uint8", "integer",
      "polars.to_r_vector.uint8", "raw",
      "polars.to_r_vector.int64", "double",
      "polars.to_r_vector.int64", "character",
      "polars.to_r_vector.int64", "integer",
      "polars.to_r_vector.int64", "integer64",
      "polars.to_r_vector.date", "Date",
      "polars.to_r_vector.date", "IDate",
      "polars.to_r_vector.time", "hms",
      "polars.to_r_vector.time", "ITime",
      "polars.to_r_vector.struct", "dataframe",
      "polars.to_r_vector.struct", "tibble",
      "polars.to_r_vector.decimal", "double",
      "polars.to_r_vector.decimal", "character",
      "polars.to_r_vector.as_clock_class", FALSE,
      "polars.to_r_vector.as_clock_class", TRUE,
    )[-1, ] # remove the first dummy row
  },
  {
    df <- pl$DataFrame(
      uint8 = as_polars_series(1:3)$cast(pl$UInt8),
      int64 = as_polars_series(1:3)$cast(pl$Int64),
      date = as_polars_series(1:3)$cast(pl$Date),
      time = as_polars_series(1:3)$cast(pl$Time),
      struct = as_polars_series(data.frame(a = 1:3)),
      decimal = as_polars_series(1:3)$cast(pl$Decimal(10, 2)),
      duration = as_polars_series(1:3)$cast(pl$Duration("ms")),
      datetime_without_tz = as_polars_series(1:3)$cast(pl$Datetime("ms")),
      datetime_with_tz = as_polars_series(1:3)$cast(pl$Datetime("ms", "Europe/London")),
    )
    series <- df$to_struct()
    withr::with_options(
      c(
        opt_value |>
          rlang::set_names(opt_name),
        list(width = 200) # To print all columns of tibble
      ),
      withr::with_timezone("UTC", {
        expect_snapshot(series$to_r_vector())
        expect_snapshot(as.vector(series))
        expect_snapshot(as.data.frame(df))
        expect_snapshot(as.list(df, as_series = FALSE))
        expect_snapshot(tibble::as_tibble(df))
      })
    )
  }
)

# dt$timestamp

    Code
      as_polars_series(as.Date("2022-1-1"))$dt$timestamp("bob")
    Condition
      Error in `as_polars_series(as.Date("2022-1-1"))$dt$timestamp()`:
      ! Evaluation failed in `$timestamp()`.
      Caused by error:
      ! Evaluation failed.
      Caused by error:
      ! `tu` must be one of "ns", "us", or "ms", not "bob".

---

    Code
      as_polars_series(as.Date("2022-1-1"))$dt$timestamp(42)
    Condition
      Error in `as_polars_series(as.Date("2022-1-1"))$dt$timestamp()`:
      ! Evaluation failed in `$timestamp()`.
      Caused by error:
      ! Evaluation failed.
      Caused by error:
      ! `tu` must be a string or character vector.

# dt$with_time_unit cast_time_unit

    Code
      as_polars_series(as.Date("2022-1-1"))$dt$cast_time_unit("bob")
    Condition
      Error in `as_polars_series(as.Date("2022-1-1"))$dt$cast_time_unit()`:
      ! Evaluation failed in `$cast_time_unit()`.
      Caused by error:
      ! Evaluation failed.
      Caused by error:
      ! `tu` must be one of "ns", "us", or "ms", not "bob".

---

    Code
      as_polars_series(as.Date("2022-1-1"))$dt$cast_time_unit(42)
    Condition
      Error in `as_polars_series(as.Date("2022-1-1"))$dt$cast_time_unit()`:
      ! Evaluation failed in `$cast_time_unit()`.
      Caused by error:
      ! Evaluation failed.
      Caused by error:
      ! `tu` must be a string or character vector.

---

    Code
      as_polars_series(as.Date("2022-1-1"))$dt$with_time_unit("bob")
    Condition
      Error in `as_polars_series(as.Date("2022-1-1"))$dt$with_time_unit()`:
      ! Evaluation failed in `$with_time_unit()`.
      Caused by error:
      ! Evaluation failed.
      Caused by error:
      ! `tu` must be one of "ns", "us", or "ms", not "bob".

---

    Code
      as_polars_series(as.Date("2022-1-1"))$dt$with_time_unit(42)
    Condition
      Error in `as_polars_series(as.Date("2022-1-1"))$dt$with_time_unit()`:
      ! Evaluation failed in `$with_time_unit()`.
      Caused by error:
      ! Evaluation failed.
      Caused by error:
      ! `tu` must be a string or character vector.


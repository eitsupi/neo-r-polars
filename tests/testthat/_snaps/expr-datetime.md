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


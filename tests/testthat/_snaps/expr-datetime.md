# dt$round

    Code
      pl$col("datetime")$dt$round(42)
    Condition
      Error in `pl$col("datetime")$dt$round()`:
      ! `every` must be a single non-NA character or difftime.

---

    Code
      pl$col("datetime")$dt$round(c("2s", "1h"))
    Condition
      Error in `pl$col("datetime")$dt$round()`:
      ! `every` must be a single non-NA character or difftime.

# dt$epoch

    Code
      as_polars_series(as.Date("2022-1-1"))$dt$epoch("bob")
    Condition
      Error in `as_polars_series(as.Date("2022-1-1"))$dt$epoch()`:
      ! Evaluation failed in `$epoch()`.
      Caused by error:
      ! Evaluation failed.
      Caused by error:
      ! `time_unit` must be one of "us", "ns", "ms", "s", or "d", not "bob".

---

    Code
      as_polars_series(as.Date("2022-1-1"))$dt$epoch(42)
    Condition
      Error in `as_polars_series(as.Date("2022-1-1"))$dt$epoch()`:
      ! Evaluation failed in `$epoch()`.
      Caused by error:
      ! Evaluation failed.
      Caused by error:
      ! `time_unit` must be a string or character vector.

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


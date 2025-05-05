# options are validated

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector.int64` must be one of "double", "character", "integer", or "integer64", not "foobar".

---

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `df_knitr_print` must be one of "auto", not "foobar".

---

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector.uint8` must be one of "integer" or "raw", not "foobar".

---

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector.date` must be one of "Date" or "IDate", not "foobar".

---

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector.time` must be one of "hms" or "ITime", not "foobar".

---

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector.ambiguous` must be one of "raise", "earliest", "latest", or "null", not "foobar".

---

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector.non_existent` must be one of "raise" or "null", not "foobar".

# option 'to_r_vector.int64' works

    Code
      expect_identical(as.list(df, as_series = FALSE), list(a = c("1", "2", "3", NA)))
    Message
      `int64` is overridden by the option "polars.to_r_vector.int64" with the string "character"

---

    Code
      expect_identical(as.list(df, as_series = FALSE), list(a = bit64::as.integer64(c(
        1, 2, 3, NA))))
    Message
      `int64` is overridden by the option "polars.to_r_vector.int64" with the string "integer64"

---

    Code
      expect_identical(as.data.frame(df), data.frame(a = c("1", "2", "3", NA)))
    Message
      `int64` is overridden by the option "polars.to_r_vector.int64" with the string "character"

---

    Code
      expect_identical(as.vector(pl$Series("a", c(1:3, NA))$cast(pl$Int64)), c("1",
        "2", "3", NA))
    Message
      `int64` is overridden by the option "polars.to_r_vector.int64" with the string "character"

---

    Code
      expect_identical(tibble::as_tibble(df), tibble::tibble(a = c("1", "2", "3", NA)))
    Message
      `int64` is overridden by the option "polars.to_r_vector.int64" with the string "character"

---

    Code
      as.list(df, as_series = FALSE)
    Message
      `int64` is overridden by the option "polars.to_r_vector.int64" with the complex number 0+0i
    Condition
      Error:
      ! Evaluation failed in `$to_r_vector()`.
      Caused by error:
      ! `int64` must be a string or character vector.

# option 'to_r_vector.date' works

    Code
      expect_identical(as.list(df, as_series = FALSE), list(a = data.table::as.IDate(
        "2020-01-01")))
    Message
      `date` is overridden by the option "polars.to_r_vector.date" with the string "IDate"

# option 'to_r_vector.uint8' works

    Code
      expect_identical(as.list(df, as_series = FALSE), list(a = as.raw(1L)))
    Message
      `uint8` is overridden by the option "polars.to_r_vector.uint8" with the string "raw"


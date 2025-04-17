# default options

    Code
      polars_options()
    Output
      Options:
      ========                               
      df_knitr_print             auto
      conversion_int64         double
      conversion_uint8        integer
      conversion_date            Date
      conversion_time             hms
      conversion_decimal       double
      conversion_ambiguous      raise
      conversion_non_existent   raise
      
      See `?polars_options` for the definition of all options.

# options are validated

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `conversion_int64` must be one of "double", "character", "integer", or "integer64", not "foobar".

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
      ! `conversion_uint8` must be one of "integer" or "raw", not "foobar".

---

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `conversion_date` must be one of "Date" or "IDate", not "foobar".

---

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `conversion_time` must be one of "hms" or "ITime", not "foobar".

---

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `conversion_ambiguous` must be one of "raise", "earliest", "latest", or "null", not "foobar".

---

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `conversion_non_existent` must be one of "raise" or "null", not "foobar".

# option 'conversion_int64' works

    Code
      polars_options()
    Condition
      Error in `polars_options()`:
      ! package `bit64` must be attached to use `conversion_int64 = "integer64"`.

# option 'conversion_date' works

    Code
      polars_options()
    Condition
      Error in `polars_options()`:
      ! package `data.table` must be attached to use `conversion_date = "IDate"`.

# option 'conversion_time' works

    Code
      polars_options()
    Condition
      Error in `polars_options()`:
      ! package `hms` must be attached to use `conversion_time = "hms"`.

---

    Code
      polars_options()
    Condition
      Error in `polars_options()`:
      ! package `data.table` must be attached to use `conversion_time = "ITime"`.


# default options

    Code
      polars_options()
    Output
      Options:
      ========                                
      df_knitr_print              auto
      to_r_vector_int64         double
      to_r_vector_uint8        integer
      to_r_vector_date            Date
      to_r_vector_time             hms
      to_r_vector_decimal       double
      to_r_vector_ambiguous      raise
      to_r_vector_non_existent   raise
      
      See `?polars_options` for the definition of all options.

# options are validated

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector_int64` must be one of "double", "character", "integer", or "integer64", not "foobar".

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
      ! `to_r_vector_uint8` must be one of "integer" or "raw", not "foobar".

---

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector_date` must be one of "Date" or "IDate", not "foobar".

---

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector_time` must be one of "hms" or "ITime", not "foobar".

---

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector_ambiguous` must be one of "raise", "earliest", "latest", or "null", not "foobar".

---

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `to_r_vector_non_existent` must be one of "raise" or "null", not "foobar".

# option 'to_r_vector_int64' works

    Code
      polars_options()
    Condition
      Error in `polars_options()`:
      ! package `bit64` must be attached to use `to_r_vector_int64 = "integer64"`.

# option 'to_r_vector_date' works

    Code
      polars_options()
    Condition
      Error in `polars_options()`:
      ! package `data.table` must be attached to use `to_r_vector_date = "IDate"`.

# option 'to_r_vector_time' works

    Code
      polars_options()
    Condition
      Error in `polars_options()`:
      ! package `hms` must be attached to use `to_r_vector_time = "hms"`.

---

    Code
      polars_options()
    Condition
      Error in `polars_options()`:
      ! package `data.table` must be attached to use `to_r_vector_time = "ITime"`.


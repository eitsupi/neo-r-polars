# default options

    Code
      polars_options()
    Output
      Options:
      ========                       
      df_knitr_print     auto
      int64_conversion double
      
      See `?polars_options` for the definition of all options.

# options are validated

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `int64_conversion` must be one of "double", "character", "integer", or "integer64", not "foobar".

---

    Code
      print(polars_options())
    Condition
      Error in `polars_options()`:
      ! `df_knitr_print` must be one of "auto", not "foobar".

# option 'int64_conversion ' works

    Code
      polars_options()
    Condition
      Error in `polars_options()`:
      ! package `bit64` must be attached to use `int64_conversion = "integer64"`.


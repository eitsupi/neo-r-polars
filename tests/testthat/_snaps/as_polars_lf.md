# as_polars_lf.default throws an error

    Code
      as_polars_lf(1)
    Condition
      Error in `as_polars_df()`:
      ! This object is not supported for the default method of `as_polars_df()` because it is not a Struct dtype like object.
      i Use `infer_polars_dtype()` to check the dtype for corresponding to the object.

---

    Code
      as_polars_lf(0+1i)
    Condition
      Error in `as_polars_df()`:
      ! This object is not supported for the default method of `as_polars_df()` because it can't be converted to a polars Series.
      Caused by error:
      ! Unsupported class for `infer_polars_dtype()`: complex
      Caused by error in `infer_polars_dtype_default_impl()`:
      ! Unsupported class for `as_polars_series()`: complex


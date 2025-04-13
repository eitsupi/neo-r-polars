# as_polars_lf.default throws an error

    Code
      as_polars_lf(1)
    Condition
      Error in `as_polars_lf()`:
      ! Failed to create a polars LazyFrame.
      Caused by error in `as_polars_df()`:
      ! `x` would have dtype 'Float64' once converted to polars.
      i `as_polars_df()` requires `x` to be an object with dtype 'Struct'.

---

    Code
      as_polars_lf(0+1i)
    Condition
      Error in `as_polars_lf()`:
      ! Failed to create a polars LazyFrame.
      Caused by error in `as_polars_df()`:
      ! the complex number 0+1i may not be converted to a polars Series, and hence to a polars DataFrame.
      Caused by error in `infer_polars_dtype()`:
      ! Can't infer polars dtype of the complex number 0+1i
      Caused by error in `as_polars_series()`:
      ! an empty complex vector can't be converted to a polars Series.


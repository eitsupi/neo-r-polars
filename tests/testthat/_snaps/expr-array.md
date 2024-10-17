# arr$var

    Code
      df$select(pl$col("strings")$arr$var(ddof = 1000))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error in `pl$col("strings")$arr$var()`:
      ! Evaluation failed in `$var()`.
      Caused by error:
      ! Value `1000.0` is too large to be converted to u8

# arr$std

    Code
      df$select(pl$col("strings")$arr$std(ddof = 1000))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error in `pl$col("strings")$arr$std()`:
      ! Evaluation failed in `$std()`.
      Caused by error:
      ! Value `1000.0` is too large to be converted to u8


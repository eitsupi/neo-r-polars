# map_batches works

    Code
      .data$select(pl$col("a")$map_batches(function(...) integer))
    Condition
      Error:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! Unsupported class for `as_polars_series()`: function
      Error:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! User function raised an error

# entropy

    Code
      pl$select(pl$lit(c("a", "b", "b", "c", "c", "c"))$entropy(base = 2))
    Condition
      Error:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! Invalid operation: expected numerical input for 'entropy'

# implode

    Code
      pl$lit(42)$implode(42)
    Condition
      Error:
      ! unused argument (42)

# rolling: error if period is negative

    Code
      df$select(pl$col("a")$rolling(index_column = "dt", period = "-2d"))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! rolling window period should be strictly positive

# replace_strict works

    Code
      df$select(replaced = pl$col("a")$replace_strict(2, 100, return_dtype = pl$
        Float32))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! Invalid operation: incomplete mapping specified for `replace_strict`
      
      Hint: Pass a `default` value to set unmapped values.

---

    Code
      df$select(pl$col("a")$replace_strict(mapping, return_dtype = pl$foo))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$replace_strict()`.
      Caused by error in `pl$foo`:
      ! $ - syntax error: `foo` is not a member of this polars object

# qcut works

    Code
      df$select(qcut = pl$col("foo")$qcut("a"))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$qcut()`.
      Caused by error:
      ! Argument `probs` must be numeric, not character

---

    Code
      df$select(qcut = pl$col("foo")$qcut(c("a", "b")))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$qcut()`.
      Caused by error:
      ! Argument `probs` must be numeric, not character


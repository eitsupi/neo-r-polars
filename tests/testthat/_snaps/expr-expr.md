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

# $over() with mapping_strategy

    Code
      df$select(pl$col("val")$top_k(2)$over("a"))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! the length of the window expression did not match that of the group
      
      Error originated in expression: 'col("val").top_k([dyn float: 2.0]).over([col("a")])'

# to_physical + cast

    Code
      as_polars_df(iris)$with_columns(pl$col("Species")$cast(pl$String)$cast(pl$
        Boolean))
    Condition
      Error in `as_polars_df(iris)$with_columns()`:
      ! Evaluation failed in `$with_columns()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! Invalid operation: casting from Utf8View to Boolean not supported

---

    Code
      df_big_n$with_columns(pl$col("big")$cast(pl$Int32))
    Condition
      Error in `df_big_n$with_columns()`:
      ! Evaluation failed in `$with_columns()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! Invalid operation: conversion from `i64` to `i32` failed in column 'big' for 1 out of 1 values: [1125899906842624]

# exclude

    Code
      df$select(pl$all()$exclude("Species", pl$Boolean))$columns
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$exclude()`.
      Caused by error:
      ! cannot exclude by both column name and dtype; use a selector instead

---

    Code
      df$select(pl$all()$exclude(foo = "Species"))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$exclude()`.
      Caused by error:
      ! Arguments in `...` must be passed by position, not name.
      x Problematic argument:
      * foo = "Species"

# Expr_append

    Code
      pl$select(pl$lit("Bob")$append(FALSE, upcast = FALSE))
    Condition
      Error:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! type Boolean is incompatible with expected type String

# gather that

    Code
      pl$select(pl$lit(0:10)$gather(11))
    Condition
      Error:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! gather indices are out of bounds

# fill_nan() works

    Code
      pl$DataFrame(!!!l)$select(pl$col("a")$fill_nan(10:11))
    Condition
      Error:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! lengths don't match: shapes of `self`, `mask` and `other` are not suitable for `zip_with` operation

# std var

    Code
      pl$lit(1:321)$std(256)
    Condition
      Error:
      ! Evaluation failed in `$std()`.
      Caused by error:
      ! Value `256.0` is too large to be converted to u8

---

    Code
      pl$lit(1:321)$var(-1)
    Condition
      Error:
      ! Evaluation failed in `$var()`.
      Caused by error:
      ! Value `-1.0` is too small to be converted to u8

# is_between errors if wrong 'closed' arg

    Code
      df$select(pl$col("var")$is_between(1, 2, "foo"))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$is_between()`.
      Caused by error:
      ! `closed` must be one of "both", "left", "right", or "none", not "foo".

# rolling_*_by only works with date/datetime

    Code
      df$select(pl$col("a")$rolling_min_by(1, window_size = "2d"))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! Invalid operation: `by` column in `rolling_*_by` must be the same length as values column

# rolling_*_by: arg 'min_periods'

    Code
      df$select(pl$col("a")$rolling_min_by("date", window_size = "2d", min_periods = -
        1))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$rolling_min_by()`.
      Caused by error:
      ! Negative value `-1.0` cannot be converted to usize

# rolling_*_by: arg 'closed'

    Code
      df$select(pl$col("a")$rolling_min_by("date", window_size = "2d", closed = "foo"))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$rolling_min_by()`.
      Caused by error:
      ! `closed` must be one of "both", "left", "right", or "none", not "foo".

# diff

    Code
      pl$lit(1:5)$diff(99^99)
    Condition
      Error:
      ! Evaluation failed in `$diff()`.
      Caused by error:
      ! Value `3.697296376497268e197` is too large to be converted to i64

---

    Code
      pl$lit(1:5)$diff(5, "not a null behavior")
    Condition
      Error:
      ! Evaluation failed in `$diff()`.
      Caused by error:
      ! `null_behavior` must be one of "ignore" or "drop", not "not a null behavior".

# reshape

    Code
      pl$lit(1:12)$reshape("hej")
    Condition
      Error:
      ! Evaluation failed in `$reshape()`.
      Caused by error:
      ! Argument `dimensions` must be numeric, not character

---

    Code
      pl$lit(1:12)$reshape(NA)
    Condition
      Error:
      ! Evaluation failed in `$reshape()`.
      Caused by error:
      ! Should not reach here!

# shuffle

    Code
      pl$lit(1:12)$shuffle("hej")
    Condition
      Error:
      ! Evaluation failed in `$shuffle()`.
      Caused by error:
      ! Argument `seed` must be numeric, not character

---

    Code
      pl$lit(1:12)$shuffle(-2)
    Condition
      Error:
      ! Evaluation failed in `$shuffle()`.
      Caused by error:
      ! Value `-2.0` is too small to be converted to u64

---

    Code
      pl$lit(1:12)$shuffle(NaN)
    Condition
      Error:
      ! Evaluation failed in `$shuffle()`.
      Caused by error:
      ! `NaN` cannot be converted to u64

---

    Code
      pl$lit(1:12)$shuffle(10^73)
    Condition
      Error:
      ! Evaluation failed in `$shuffle()`.
      Caused by error:
      ! Value `1e73` is too large to be converted to u64

# sample

    Code
      df$select(pl$col("a")$sample(fraction = 2))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! lengths don't match: cannot take a larger sample than the total population when `with_replacement=false`

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


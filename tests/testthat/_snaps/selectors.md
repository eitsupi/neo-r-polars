# 'minus' operator works

    Code
      cs$alphanumeric() - 1
    Condition
      Error:
      ! `e2` must be a polars selector, not the number 1.

# by_dtype

    Code
      df$select(cs$by_dtype(a = pl$String))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error in `cs$by_dtype()`:
      ! Arguments in `...` must be passed by position, not name.
      x Problematic argument:
      * a = pl$String

# by_name

    Code
      df$select(cs$by_name(a = "foo"))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error in `cs$by_name()`:
      ! Arguments in `...` must be passed by position, not name.
      x Problematic argument:
      * a = "foo"


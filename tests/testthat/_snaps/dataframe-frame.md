# unnest works correctly

    Code
      df$unnest("b", pl$col("a_and_c"))
    Condition
      Error in `df$unnest()`:
      ! Evaluation failed in `$unnest()`.
      Caused by error in `df$unnest()`:
      ! `columns` must be a character vector, not a list.

---

    Code
      df$unnest(1)
    Condition
      Error in `df$unnest()`:
      ! Evaluation failed in `$unnest()`.
      Caused by error in `df$unnest()`:
      ! `columns` must be a character vector, not the number 1.

---

    Code
      df$unnest("foo")
    Condition
      Error in `df$unnest()`:
      ! Evaluation failed in `$unnest()`.
      Caused by error:
      ! invalid series dtype: expected `Struct`, got `f64`


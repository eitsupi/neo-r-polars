# list$unique list$sort

    Code
      df$select(pl$all()$list$unique(TRUE))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error in `pl$all()$list$unique()`:
      ! Evaluation failed in `$unique()`.
      Caused by error in `pl$all()$list$unique()`:
      ! `...` must be empty.
      x Problematic argument:
      * ..1 = TRUE
      i Did you forget to name an argument?

---

    Code
      df$select(pl$all()$list$sort(TRUE))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error in `pl$all()$list$sort()`:
      ! Evaluation failed in `$sort()`.
      Caused by error in `pl$all()$list$sort()`:
      ! `...` must be empty.
      x Problematic argument:
      * ..1 = TRUE
      i Did you forget to name an argument?

# gather

    Code
      pl$DataFrame(x = l)$with_columns(pl$col("x")$list$gather(list(c(0:3), 0L, 0L)))
    Condition
      Error:
      ! Evaluation failed in `$with_columns()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! OutOfBounds(ErrString("gather indices are out of bounds"))

---

    Code
      pl$DataFrame(x = l)$with_columns(pl$col("x")$list$gather(1, TRUE))
    Condition
      Error:
      ! Evaluation failed in `$with_columns()`.
      Caused by error:
      ! Evaluation failed in `$with_columns()`.
      Caused by error in `pl$col("x")$list$gather()`:
      ! Evaluation failed in `$gather()`.
      Caused by error in `pl$col("x")$list$gather()`:
      ! `...` must be empty.
      x Problematic argument:
      * ..1 = TRUE
      i Did you forget to name an argument?

# gather_every

    Code
      df$select(out = pl$col("a")$list$gather_every(-1))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! InvalidOperation(ErrString("conversion from `f64` to `u32` failed in column 'literal' for 1 out of 1 values: [-1.0]"))

---

    Code
      df$select(out = pl$col("a")$list$gather_every())
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error in `pl$col("a")$list$gather_every()`:
      ! Evaluation failed in `$gather_every()`.
      Caused by error in `pl$col("a")$list$gather_every()`:
      ! argument "n" is missing, with no default

---

    Code
      df$select(out = pl$col("a")$list$gather_every(n = 2, offset = -1))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! InvalidOperation(ErrString("conversion from `f64` to `u32` failed in column 'literal' for 1 out of 1 values: [-1.0]"))

# join

    Code
      df$select(pl$col("s")$list$join(pl$col("separator"), TRUE))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error in `pl$col("s")$list$join()`:
      ! Evaluation failed in `$join()`.
      Caused by error in `pl$col("s")$list$join()`:
      ! `...` must be empty.
      x Problematic argument:
      * ..1 = TRUE
      i Did you forget to name an argument?

# $list$explode() works

    Code
      df$with_columns(pl$col("a")$list$explode())
    Condition
      Error in `df$with_columns()`:
      ! Evaluation failed in `$with_columns()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! ShapeMismatch(ErrString("unable to add a column of length 6 to a DataFrame of height 2"))

# $list$sample() works

    Code
      df$select(pl$col("values")$list$sample(fraction = 2))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! ShapeMismatch(ErrString("cannot take a larger sample than the total population when `with_replacement=false`"))

# str$replace_many

    Code
      dat$with_columns(pl$col("x")$str$replace_many(c("hi", "hello"), c("foo", "bar",
        "foo2")))
    Condition
      Error in `dat$with_columns()`:
      ! Evaluation failed in `$with_columns()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! InvalidOperation(ErrString("expected the same amount of patterns as replacement strings"))

---

    Code
      dat$with_columns(pl$col("x")$str$replace_many(c("hi", "hello", "good morning"),
      c("foo", "bar")))
    Condition
      Error in `dat$with_columns()`:
      ! Evaluation failed in `$with_columns()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! InvalidOperation(ErrString("expected the same amount of patterns as replacement strings"))

# str$find works

    Code
      test$select(default = pl$col("s")$str$find("Aa", "b"))
    Condition
      Error in `test$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error in `pl$col("s")$str$find()`:
      ! Evaluation failed in `$find()`.
      Caused by error in `pl$col("s")$str$find()`:
      ! `...` must be empty.
      x Problematic argument:
      * ..1 = "b"
      i Did you forget to name an argument?

---

    Code
      test$select(lit = pl$col("s")$str$find("(?iAa"))
    Condition
      Error in `test$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! ComputeError(ErrString("Invalid regular expression: regex parse error:\n    (?iAa\n       ^\nerror: unrecognized flag"))

# $str$extract_many works

    Code
      df$select(matches = pl$col("values")$str$extract_many(patterns, "a"))
    Condition
      Error in `df$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error in `pl$col("values")$str$extract_many()`:
      ! Evaluation failed in `$extract_many()`.
      Caused by error in `pl$col("values")$str$extract_many()`:
      ! `...` must be empty.
      x Problematic argument:
      * ..1 = "a"
      i Did you forget to name an argument?


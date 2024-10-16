# set_ordering

    Code
      dat$with_columns(pl$col("x")$cat$set_ordering("foo")$sort())
    Condition
      Error in `dat$with_columns()`:
      ! Evaluation failed in `$with_columns()`.
      Caused by error:
      ! Evaluation failed in `$with_columns()`.
      Caused by error in `pl$col("x")$cat$set_ordering()`:
      ! Evaluation failed in `$set_ordering()`.
      Caused by error in `pl$col("x")$cat$set_ordering()`:
      ! `ordering` must be one of "lexical" or "physical", not "foo".


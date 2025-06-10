# clear error message when passing lists with some Polars expr to dynamic dots

    Code
      dat$select(exprs)
    Condition
      Error in `dat$select()`:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! Evaluation failed in `$select()`.
      Caused by error:
      ! `...` doesn't accept inputs of type list.
      i Use `!!!` on the input(s), e.g. `!!!my_list`.


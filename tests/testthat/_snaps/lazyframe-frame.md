# unnest works correctly

    Code
      current$collect()
    Condition
      Error in `current$collect()`:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! Invalid operation: invalid selector expression: dyn float: 1.0
      
      Resolved plan until failure:
      
      	---> FAILED HERE RESOLVING THIS_NODE <---
       SELECT [dyn float: 1.0.alias("foo"), col("b").as_struct(), col("a").as_struct([col("c")]).alias("a_and_c")] FROM
        DF ["a", "b", "c"]; PROJECT */3 COLUMNS; SELECTION: None

---

    Code
      current$collect()
    Output
      polars: closing concurrent R handler
    Condition
      Error in `current$collect()`:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! A polars sub-thread panicked. See panic msg, which is likely more informative than this error: Any { .. }


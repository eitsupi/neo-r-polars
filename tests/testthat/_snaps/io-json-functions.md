# bad paths

    Code
      pl$read_ndjson(character())
    Condition
      Error in `pl$read_ndjson()`:
      ! Evaluation failed in `$read_ndjson()`.
      Caused by error:
      ! `source` must have length > 0.

---

    Code
      pl$read_ndjson("foobar")
    Condition
      Error in `pl$read_ndjson()`:
      ! Evaluation failed in `$read_ndjson()`.
      Caused by error in `do.call(pl__scan_ndjson, .args)$collect()`:
      ! Evaluation failed in `$collect()`.
      Caused by error:
      ! No such file or directory (os error 2): foobar: 'ndjson scan'


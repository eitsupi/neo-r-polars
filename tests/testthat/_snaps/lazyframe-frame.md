# Can't serialize lazyframe includes map function

    Code
      pl$LazyFrame()$select(pl$lit(1)$map_batches(function(x) x + 1))$serialize()
    Condition
      Error:
      ! Evaluation failed in `$serialize()`.
      Caused by error:
      ! serialization not supported for this 'opaque' function

# deserialize lazyframe' error

    Code
      pl$deserialize_lf(0L)
    Condition
      Error in `pl$deserialize_lf()`:
      ! Evaluation failed in `$deserialize_lf()`.
      Caused by error:
      ! Argument `data` must be raw, not integer

---

    Code
      pl$deserialize_lf(raw(0))
    Condition
      Error in `pl$deserialize_lf()`:
      ! Evaluation failed in `$deserialize_lf()`.
      Caused by error:
      ! The input value is not a valid serialized LazyFrame.

---

    Code
      pl$deserialize_lf(as.raw(1:100))
    Condition
      Error in `pl$deserialize_lf()`:
      ! Evaluation failed in `$deserialize_lf()`.
      Caused by error:
      ! The input value is not a valid serialized LazyFrame.

# $to_dot() works

    Code
      cat(lf$to_dot())
    Output
      graph  polars_query {
        p1[label="TABLE\nπ */11"]
      }

---

    Code
      cat(lf$select("am")$to_dot())
    Output
      graph  polars_query {
        p1[label="TABLE\nπ 1/11"]
      }

# explain() works

    Code
      cat(lazy_query$explain(optimized = FALSE))
    Output
      FILTER [(col("Species")) != ("setosa")]
      FROM
        SORT BY [col("Species")]
          DF ["Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width", ...]; PROJECT */5 COLUMNS

---

    Code
      cat(lazy_query$explain())
    Output
      SORT BY [col("Species")]
        FILTER [(col("Species")) != ("setosa")]
        FROM
          DF ["Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width", ...]; PROJECT */5 COLUMNS

---

    Code
      cat(lazy_query$explain(format = "tree", optimized = FALSE))
    Output
                             0                            1                                               2
         ┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
         │
         │               ╭────────╮
       0 │               │ FILTER │
         │               ╰───┬┬───╯
         │                   ││
         │                   │╰───────────────────────────╮
         │                   │                            │
         │  ╭────────────────┴─────────────────╮     ╭────┴────╮
         │  │ predicate:                       │     │ FROM:   │
       1 │  │ [(col("Species")) != ("setosa")] │     │ SORT BY │
         │  ╰──────────────────────────────────╯     ╰────┬┬───╯
         │                                                ││
         │                                                │╰──────────────────────────────────────────────╮
         │                                                │                                               │
         │                                        ╭───────┴────────╮  ╭───────────────────────────────────┴────────────────────────────────────╮
         │                                        │ expression:    │  │ DF ["Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width", ...] │
       2 │                                        │ col("Species") │  │ PROJECT */5 COLUMNS                                                    │
         │                                        ╰────────────────╯  ╰────────────────────────────────────────────────────────────────────────╯

---

    Code
      cat(lazy_query$explain(format = "tree", ))
    Output
                    0                            1                                                        2
         ┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
         │
         │     ╭─────────╮
       0 │     │ SORT BY │
         │     ╰────┬┬───╯
         │          ││
         │          │╰───────────────────────────╮
         │          │                            │
         │  ╭───────┴────────╮                   │
         │  │ expression:    │               ╭───┴────╮
       1 │  │ col("Species") │               │ FILTER │
         │  ╰────────────────╯               ╰───┬┬───╯
         │                                       ││
         │                                       │╰───────────────────────────────────────────────────────╮
         │                                       │                                                        │
         │                      ╭────────────────┴─────────────────╮  ╭───────────────────────────────────┴────────────────────────────────────╮
         │                      │ predicate:                       │  │ FROM:                                                                  │
       2 │                      │ [(col("Species")) != ("setosa")] │  │ DF ["Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width", ...] │
         │                      ╰──────────────────────────────────╯  │ PROJECT */5 COLUMNS                                                    │
         │                                                            ╰────────────────────────────────────────────────────────────────────────╯

# describe() works

    Code
      df$describe()
    Output
      shape: (9, 6)
      ┌────────────┬──────────┬────────┬─────────────────────────┬──────┬──────┐
      │ statistic  ┆ float    ┆ string ┆ date                    ┆ cat  ┆ bool │
      │ ---        ┆ ---      ┆ ---    ┆ ---                     ┆ ---  ┆ ---  │
      │ str        ┆ f64      ┆ str    ┆ str                     ┆ str  ┆ f64  │
      ╞════════════╪══════════╪════════╪═════════════════════════╪══════╪══════╡
      │ count      ┆ 2.0      ┆ 2      ┆ 2                       ┆ 2    ┆ 2.0  │
      │ null_count ┆ 1.0      ┆ 1      ┆ 1                       ┆ 1    ┆ 1.0  │
      │ mean       ┆ 1.75     ┆ null   ┆ 2024-01-20 12:00:00.000 ┆ null ┆ 0.5  │
      │ std        ┆ 0.353553 ┆ null   ┆ null                    ┆ null ┆ null │
      │ min        ┆ 1.5      ┆ a      ┆ 2024-01-20              ┆ zz   ┆ 0.0  │
      │ 25%        ┆ 1.5      ┆ null   ┆ 2024-01-20              ┆ null ┆ null │
      │ 50%        ┆ 2.0      ┆ null   ┆ 2024-01-21              ┆ null ┆ null │
      │ 75%        ┆ 2.0      ┆ null   ┆ 2024-01-21              ┆ null ┆ null │
      │ max        ┆ 2.0      ┆ b      ┆ 2024-01-21              ┆ a    ┆ 1.0  │
      └────────────┴──────────┴────────┴─────────────────────────┴──────┴──────┘

---

    Code
      df$lazy()$describe()
    Output
      shape: (9, 6)
      ┌────────────┬──────────┬────────┬─────────────────────────┬──────┬──────┐
      │ statistic  ┆ float    ┆ string ┆ date                    ┆ cat  ┆ bool │
      │ ---        ┆ ---      ┆ ---    ┆ ---                     ┆ ---  ┆ ---  │
      │ str        ┆ f64      ┆ str    ┆ str                     ┆ str  ┆ f64  │
      ╞════════════╪══════════╪════════╪═════════════════════════╪══════╪══════╡
      │ count      ┆ 2.0      ┆ 2      ┆ 2                       ┆ 2    ┆ 2.0  │
      │ null_count ┆ 1.0      ┆ 1      ┆ 1                       ┆ 1    ┆ 1.0  │
      │ mean       ┆ 1.75     ┆ null   ┆ 2024-01-20 12:00:00.000 ┆ null ┆ 0.5  │
      │ std        ┆ 0.353553 ┆ null   ┆ null                    ┆ null ┆ null │
      │ min        ┆ 1.5      ┆ a      ┆ 2024-01-20              ┆ zz   ┆ 0.0  │
      │ 25%        ┆ 1.5      ┆ null   ┆ 2024-01-20              ┆ null ┆ null │
      │ 50%        ┆ 2.0      ┆ null   ┆ 2024-01-21              ┆ null ┆ null │
      │ 75%        ┆ 2.0      ┆ null   ┆ 2024-01-21              ┆ null ┆ null │
      │ max        ┆ 2.0      ┆ b      ┆ 2024-01-21              ┆ a    ┆ 1.0  │
      └────────────┴──────────┴────────┴─────────────────────────┴──────┴──────┘

---

    Code
      pl$DataFrame()$describe()
    Condition
      Error:
      ! Evaluation failed in `$describe()`.
      Caused by error:
      ! Can't describe a DataFrame without any columns

---

    Code
      pl$LazyFrame()$describe()
    Condition
      Error:
      ! Evaluation failed in `$describe()`.
      Caused by error:
      ! Can't describe a LazyFrame without any columns

---

    Code
      df$describe(percentiles = 0.1)
    Output
      shape: (7, 6)
      ┌────────────┬──────────┬────────┬─────────────────────────┬──────┬──────┐
      │ statistic  ┆ float    ┆ string ┆ date                    ┆ cat  ┆ bool │
      │ ---        ┆ ---      ┆ ---    ┆ ---                     ┆ ---  ┆ ---  │
      │ str        ┆ f64      ┆ str    ┆ str                     ┆ str  ┆ f64  │
      ╞════════════╪══════════╪════════╪═════════════════════════╪══════╪══════╡
      │ count      ┆ 2.0      ┆ 2      ┆ 2                       ┆ 2    ┆ 2.0  │
      │ null_count ┆ 1.0      ┆ 1      ┆ 1                       ┆ 1    ┆ 1.0  │
      │ mean       ┆ 1.75     ┆ null   ┆ 2024-01-20 12:00:00.000 ┆ null ┆ 0.5  │
      │ std        ┆ 0.353553 ┆ null   ┆ null                    ┆ null ┆ null │
      │ min        ┆ 1.5      ┆ a      ┆ 2024-01-20              ┆ zz   ┆ 0.0  │
      │ 10%        ┆ 1.5      ┆ null   ┆ 2024-01-20              ┆ null ┆ null │
      │ max        ┆ 2.0      ┆ b      ┆ 2024-01-21              ┆ a    ┆ 1.0  │
      └────────────┴──────────┴────────┴─────────────────────────┴──────┴──────┘

---

    Code
      df$select(pl$col("cat")$cast(pl$Categorical("lexical")))$describe()
    Output
      shape: (9, 2)
      ┌────────────┬──────┐
      │ statistic  ┆ cat  │
      │ ---        ┆ ---  │
      │ str        ┆ str  │
      ╞════════════╪══════╡
      │ count      ┆ 2    │
      │ null_count ┆ 1    │
      │ mean       ┆ null │
      │ std        ┆ null │
      │ min        ┆ a    │
      │ 25%        ┆ null │
      │ 50%        ┆ null │
      │ 75%        ┆ null │
      │ max        ┆ zz   │
      └────────────┴──────┘

# error and warning from collect engines

    Code
      as_polars_lf(mtcars)$collect(engine = "gpu")
    Condition
      Error in `as_polars_lf(mtcars)$collect()`:
      ! Evaluation failed in `$collect()`.
      Caused by error in `as_polars_lf(mtcars)$collect()`:
      ! `engine` must be one of "auto", "in-memory", or "streaming", not "gpu".

---

    Code
      as_polars_lf(mtcars)$collect(engine = "old-streaming")
    Condition
      Error in `as_polars_lf(mtcars)$collect()`:
      ! Evaluation failed in `$collect()`.
      Caused by error in `as_polars_lf(mtcars)$collect()`:
      ! `engine` must be one of "auto", "in-memory", or "streaming", not "old-streaming".
      i Did you mean "streaming"?

# group_by() warns with arg maintain_order

    Code
      dat$group_by("a", maintain_order = TRUE)$agg()
    Condition
      Warning:
      ! In `$group_by()`, `...` contain an argument named `maintain_order`.
      i You may want to specify the argument `.maintain_order` instead.
    Output
      shape: (1, 2)
      ┌─────┬────────────────┐
      │ a   ┆ maintain_order │
      │ --- ┆ ---            │
      │ i32 ┆ bool           │
      ╞═════╪════════════════╡
      │ 1   ┆ true           │
      └─────┴────────────────┘

# active bindings

    Code
      as_polars_lf(mtcars)$width
    Condition
      Warning:
      ! Potentially expensive operation.
      * Determining the width of a LazyFrame requires resolving its schema,
      * so this calls `$collect_schema()` internally.
      i Use `length(<lazyframe>)` to get the width without this warning.
    Output
      [1] 11

---

    Code
      as_polars_lf(mtcars)$columns
    Condition
      Warning:
      ! Potentially expensive operation.
      * Determining the column names of a LazyFrame requires resolving its schema,
      * so this calls `$collect_schema()` internally.
      i Use `names(<lazyframe>)` to get the column names without this warning.
    Output
       [1] "mpg"  "cyl"  "disp" "hp"   "drat" "wt"   "qsec" "vs"   "am"   "gear"
      [11] "carb"


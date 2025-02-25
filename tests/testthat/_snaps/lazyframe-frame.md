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
      FILTER [(col("Species")) != (String(setosa))] FROM
        SORT BY [col("Species")]
          DF ["Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width"]; PROJECT */5 COLUMNS

---

    Code
      cat(lazy_query$explain())
    Output
      SORT BY [col("Species")]
        FILTER [(col("Species")) != (String(setosa))] FROM
          DF ["Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width"]; PROJECT */5 COLUMNS

---

    Code
      cat(lazy_query$explain(format = "tree", optimized = FALSE))
    Output
                                0                               1                                             2
         ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
         │
         │                  ╭────────╮
       0 │                  │ FILTER │
         │                  ╰───┬┬───╯
         │                      ││
         │                      │╰──────────────────────────────╮
         │                      │                               │
         │  ╭───────────────────┴────────────────────╮     ╭────┴────╮
         │  │ predicate:                             │     │ FROM:   │
       1 │  │ [(col("Species")) != (String(setosa))] │     │ SORT BY │
         │  ╰────────────────────────────────────────╯     ╰────┬┬───╯
         │                                                      ││
         │                                                      │╰────────────────────────────────────────────╮
         │                                                      │                                             │
         │                                              ╭───────┴────────╮  ╭─────────────────────────────────┴─────────────────────────────────╮
         │                                              │ expression:    │  │ DF ["Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width"] │
       2 │                                              │ col("Species") │  │ PROJECT */5 COLUMNS                                               │
         │                                              ╰────────────────╯  ╰───────────────────────────────────────────────────────────────────╯

---

    Code
      cat(lazy_query$explain(format = "tree", ))
    Output
                    0                               1                                                         2
         ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
         │
         │     ╭─────────╮
       0 │     │ SORT BY │
         │     ╰────┬┬───╯
         │          ││
         │          │╰──────────────────────────────╮
         │          │                               │
         │  ╭───────┴────────╮                      │
         │  │ expression:    │                  ╭───┴────╮
       1 │  │ col("Species") │                  │ FILTER │
         │  ╰────────────────╯                  ╰───┬┬───╯
         │                                          ││
         │                                          │╰────────────────────────────────────────────────────────╮
         │                                          │                                                         │
         │                      ╭───────────────────┴────────────────────╮  ╭─────────────────────────────────┴─────────────────────────────────╮
         │                      │ predicate:                             │  │ FROM:                                                             │
       2 │                      │ [(col("Species")) != (String(setosa))] │  │ DF ["Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width"] │
         │                      ╰────────────────────────────────────────╯  │ PROJECT */5 COLUMNS                                               │
         │                                                                  ╰───────────────────────────────────────────────────────────────────╯

# sink_csv: quote_style quote_style=necessary

    Code
      readLines(temp_out)
    Output
      [1] "a,b,c"                 "\"\"\"foo\"\"\",1.0,a"

# sink_csv: quote_style quote_style=always

    Code
      readLines(temp_out)
    Output
      [1] "\"a\",\"b\",\"c\""             "\"\"\"foo\"\"\",\"1.0\",\"a\""

# sink_csv: quote_style quote_style=non_numeric

    Code
      readLines(temp_out)
    Output
      [1] "\"a\",\"b\",\"c\""         "\"\"\"foo\"\"\",1.0,\"a\""

# sink_csv: quote_style quote_style=never

    Code
      readLines(temp_out)
    Output
      [1] "a,b,c"         "\"foo\",1.0,a"

# describe() works

    Code
      df$describe()
    Output
      shape: (8, 6)
      ┌────────────┬──────────┬────────┬─────────────────────────┬──────┬──────┐
      │ statistic  ┆ float    ┆ string ┆ date                    ┆ cat  ┆ bool │
      │ ---        ┆ ---      ┆ ---    ┆ ---                     ┆ ---  ┆ ---  │
      │ str        ┆ f64      ┆ str    ┆ str                     ┆ str  ┆ f64  │
      ╞════════════╪══════════╪════════╪═════════════════════════╪══════╪══════╡
      │ count      ┆ 2.0      ┆ 2      ┆ 2                       ┆ 2    ┆ 2.0  │
      │ null_count ┆ 1.0      ┆ 1      ┆ 1                       ┆ 1    ┆ 1.0  │
      │ mean       ┆ 1.75     ┆ null   ┆ 2024-01-20 12:00:00.000 ┆ null ┆ 0.5  │
      │ std        ┆ 0.353553 ┆ null   ┆ null                    ┆ null ┆ null │
      │ min        ┆ 1.5      ┆ a      ┆ 2024-01-20              ┆ null ┆ 0.0  │
      │ 25%        ┆ 1.5      ┆ null   ┆ 2024-01-20              ┆ null ┆ null │
      │ 75%        ┆ 2.0      ┆ null   ┆ 2024-01-21              ┆ null ┆ null │
      │ max        ┆ 2.0      ┆ b      ┆ 2024-01-21              ┆ null ┆ 1.0  │
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
      │ min        ┆ 1.5      ┆ a      ┆ 2024-01-20              ┆ null ┆ 0.0  │
      │ 25%        ┆ 1.5      ┆ null   ┆ 2024-01-20              ┆ null ┆ null │
      │ 50%        ┆ 2.0      ┆ null   ┆ 2024-01-21              ┆ null ┆ null │
      │ 75%        ┆ 2.0      ┆ null   ┆ 2024-01-21              ┆ null ┆ null │
      │ max        ┆ 2.0      ┆ b      ┆ 2024-01-21              ┆ null ┆ 1.0  │
      └────────────┴──────────┴────────┴─────────────────────────┴──────┴──────┘

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
      │ min        ┆ 1.5      ┆ a      ┆ 2024-01-20              ┆ null ┆ 0.0  │
      │ 10%        ┆ 1.5      ┆ null   ┆ 2024-01-20              ┆ null ┆ null │
      │ max        ┆ 2.0      ┆ b      ┆ 2024-01-21              ┆ null ┆ 1.0  │
      └────────────┴──────────┴────────┴─────────────────────────┴──────┴──────┘

---

    Code
      df$select(pl$col("cat")$cast(pl$Categorical("lexical")))$describe()
    Output
      shape: (8, 2)
      ┌────────────┬──────┐
      │ statistic  ┆ cat  │
      │ ---        ┆ ---  │
      │ str        ┆ str  │
      ╞════════════╪══════╡
      │ count      ┆ 2    │
      │ null_count ┆ 1    │
      │ mean       ┆ null │
      │ std        ┆ null │
      │ min        ┆ null │
      │ 25%        ┆ null │
      │ 75%        ┆ null │
      │ max        ┆ null │
      └────────────┴──────┘

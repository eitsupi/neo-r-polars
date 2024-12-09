# $explain() works

    Code
      cat(lazy_query$explain(optimized = FALSE))
    Output
      FILTER [(col("Species")) != (String(setosa))] FROM
        SORT BY [col("Species")]
          DF ["Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width"]; PROJECT */5 COLUMNS; SELECTION: None

---

    Code
      cat(lazy_query$explain())
    Output
      SORT BY [col("Species")]
        DF ["Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width"]; PROJECT */5 COLUMNS; SELECTION: [(col("Species")) != (String(setosa))]

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
                    0                                             1
         ┌───────────────────────────────────────────────────────────────────────────────────────────
         │
         │     ╭─────────╮
       0 │     │ SORT BY │
         │     ╰────┬┬───╯
         │          ││
         │          │╰────────────────────────────────────────────╮
         │          │                                             │
         │  ╭───────┴────────╮  ╭─────────────────────────────────┴─────────────────────────────────╮
         │  │ expression:    │  │ DF ["Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width"] │
       1 │  │ col("Species") │  │ PROJECT */5 COLUMNS                                               │
         │  ╰────────────────╯  ╰─────────────────────────────────┬─────────────────────────────────╯
         │                                                        │
         │                                                        │
         │                                                        │
         │                                    ╭───────────────────┴────────────────────╮
         │                                    │ SELECTION:                             │
       2 │                                    │ [(col("Species")) != (String(setosa))] │
         │                                    ╰────────────────────────────────────────╯

# join_asof

    Code
      l_gdp$lazy()$join_asof(l_pop$lazy(), on = "date", strategy = "fruitcake")
    Condition
      Error:
      ! Evaluation failed in `$join_asof()`.
      Caused by error:
      ! `strategy` must be one of "backward", "forward", or "nearest", not "fruitcake".


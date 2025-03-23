# $glimpse() works

    Code
      df$glimpse()
    Output
      Rows: 150
      Columns: 6
      $ Sepal.Length <f64>: 5.1, 4.9, 4.7, 4.6, 5, 5.4, 4.6, 5, 4.4, 4.9
      $ Sepal.Width  <f64>: 3.5, 3, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1
      $ Petal.Length <f64>: 1.4, 1.4, 1.3, 1.5, 1.4, 1.7, 1.4, 1.5, 1.4, 1.5
      $ Petal.Width  <f64>: 0.2, 0.2, 0.2, 0.2, 0.2, 0.4, 0.3, 0.2, 0.2, 0.1
      $ Species      <cat>: setosa, setosa, setosa, setosa, setosa, setosa, setosa, setosa, setosa, setosa
      $ literal       <i8>: 42, 42, 42, 42, 42, 42, 42, 42, 42, 42

---

    Code
      df$glimpse(max_items_per_column = 2)
    Output
      Rows: 150
      Columns: 6
      $ Sepal.Length <f64>: 5.1, 4.9
      $ Sepal.Width  <f64>: 3.5, 3
      $ Petal.Length <f64>: 1.4, 1.4
      $ Petal.Width  <f64>: 0.2, 0.2
      $ Species      <cat>: setosa, setosa
      $ literal       <i8>: 42, 42

---

    Code
      df$glimpse(max_colname_length = 2)
    Output
      Rows: 150
      Columns: 6
      $ S... <f64>: 5.1, 4.9, 4.7, 4.6, 5, 5.4, 4.6, 5, 4.4, 4.9
      $ S... <f64>: 3.5, 3, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1
      $ P... <f64>: 1.4, 1.4, 1.3, 1.5, 1.4, 1.7, 1.4, 1.5, 1.4, 1.5
      $ P... <f64>: 0.2, 0.2, 0.2, 0.2, 0.2, 0.4, 0.3, 0.2, 0.2, 0.1
      $ S... <cat>: setosa, setosa, setosa, setosa, setosa, setosa, setosa, setosa, setosa, setosa
      $ l...  <i8>: 42, 42, 42, 42, 42, 42, 42, 42, 42, 42


# as_polars_expr works for classes chr (0)

    Code
      out
    Output
      Series[literal]

---

    Code
      selected_out
    Output
      shape: (0, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ str     │
      ╞═════════╡
      └─────────┘

# as_polars_expr works for classes chr (1)

    Code
      out
    Output
      String(foo)

---

    Code
      selected_out
    Output
      shape: (1, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ str     │
      ╞═════════╡
      │ foo     │
      └─────────┘

# as_polars_expr works for classes chr (2)

    Code
      out
    Output
      Series[literal]

---

    Code
      selected_out
    Output
      shape: (2, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ str     │
      ╞═════════╡
      │ foo     │
      │ bar     │
      └─────────┘

# as_polars_expr works for classes chr NA

    Code
      out
    Output
      null.strict_cast(String)

---

    Code
      selected_out
    Output
      shape: (1, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ str     │
      ╞═════════╡
      │ null    │
      └─────────┘

# as_polars_expr works for classes lgl (0)

    Code
      out
    Output
      Series[literal]

---

    Code
      selected_out
    Output
      shape: (0, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ bool    │
      ╞═════════╡
      └─────────┘

# as_polars_expr works for classes lgl (1)

    Code
      out
    Output
      true

---

    Code
      selected_out
    Output
      shape: (1, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ bool    │
      ╞═════════╡
      │ true    │
      └─────────┘

# as_polars_expr works for classes lgl (2)

    Code
      out
    Output
      Series[literal]

---

    Code
      selected_out
    Output
      shape: (2, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ bool    │
      ╞═════════╡
      │ true    │
      │ false   │
      └─────────┘

# as_polars_expr works for classes lgl NA

    Code
      out
    Output
      null.strict_cast(Boolean)

---

    Code
      selected_out
    Output
      shape: (1, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ bool    │
      ╞═════════╡
      │ null    │
      └─────────┘

# as_polars_expr works for classes int (0)

    Code
      out
    Output
      Series[literal]

---

    Code
      selected_out
    Output
      shape: (0, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ i32     │
      ╞═════════╡
      └─────────┘

# as_polars_expr works for classes int (1)

    Code
      out
    Output
      dyn int: 1

---

    Code
      selected_out
    Output
      shape: (1, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ i32     │
      ╞═════════╡
      │ 1       │
      └─────────┘

# as_polars_expr works for classes int (2)

    Code
      out
    Output
      Series[literal]

---

    Code
      selected_out
    Output
      shape: (2, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ i32     │
      ╞═════════╡
      │ 1       │
      │ 2       │
      └─────────┘

# as_polars_expr works for classes int NA

    Code
      out
    Output
      null.strict_cast(Int32)

---

    Code
      selected_out
    Output
      shape: (1, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ i32     │
      ╞═════════╡
      │ null    │
      └─────────┘

# as_polars_expr works for classes dbl (0)

    Code
      out
    Output
      Series[literal]

---

    Code
      selected_out
    Output
      shape: (0, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ f64     │
      ╞═════════╡
      └─────────┘

# as_polars_expr works for classes dbl (1)

    Code
      out
    Output
      dyn float: 1.0

---

    Code
      selected_out
    Output
      shape: (1, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ f64     │
      ╞═════════╡
      │ 1.0     │
      └─────────┘

# as_polars_expr works for classes dbl (2)

    Code
      out
    Output
      Series[literal]

---

    Code
      selected_out
    Output
      shape: (2, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ f64     │
      ╞═════════╡
      │ 1.0     │
      │ 2.0     │
      └─────────┘

# as_polars_expr works for classes dbl NA

    Code
      out
    Output
      null.strict_cast(Float64)

---

    Code
      selected_out
    Output
      shape: (1, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ f64     │
      ╞═════════╡
      │ null    │
      └─────────┘

# as_polars_expr works for classes raw (0)

    Code
      out
    Output
      [binary value]

---

    Code
      selected_out
    Output
      shape: (1, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ binary  │
      ╞═════════╡
      │ b""     │
      └─────────┘

# as_polars_expr works for classes raw (1)

    Code
      out
    Output
      [binary value]

---

    Code
      selected_out
    Output
      shape: (1, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ binary  │
      ╞═════════╡
      │ b"a"    │
      └─────────┘

# as_polars_expr works for classes raw (2)

    Code
      out
    Output
      [binary value]

---

    Code
      selected_out
    Output
      shape: (1, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ binary  │
      ╞═════════╡
      │ b"ab"   │
      └─────────┘

# as_polars_expr works for classes NULL

    Code
      out
    Output
      null

---

    Code
      selected_out
    Output
      shape: (1, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ null    │
      ╞═════════╡
      │ null    │
      └─────────┘

# as_polars_expr works for classes list (0)

    Code
      out
    Output
      Series[literal]

---

    Code
      selected_out
    Output
      shape: (0, 1)
      ┌────────────┐
      │ literal    │
      │ ---        │
      │ list[null] │
      ╞════════════╡
      └────────────┘

# as_polars_expr works for classes list (1)

    Code
      out
    Output
      [true]

---

    Code
      selected_out
    Output
      shape: (1, 1)
      ┌────────────┐
      │ literal    │
      │ ---        │
      │ list[bool] │
      ╞════════════╡
      │ [true]     │
      └────────────┘

# as_polars_expr works for classes list (2)

    Code
      out
    Output
      Series[literal]

---

    Code
      selected_out
    Output
      shape: (2, 1)
      ┌────────────┐
      │ literal    │
      │ ---        │
      │ list[bool] │
      ╞════════════╡
      │ [true]     │
      │ [false]    │
      └────────────┘

# as_polars_expr works for classes Date (0)

    Code
      out
    Output
      Series[literal]

---

    Code
      selected_out
    Output
      shape: (0, 1)
      ┌─────────┐
      │ literal │
      │ ---     │
      │ date    │
      ╞═════════╡
      └─────────┘

# as_polars_expr works for classes Date (1)

    Code
      out
    Output
      1970-01-01

---

    Code
      selected_out
    Output
      shape: (1, 1)
      ┌────────────┐
      │ literal    │
      │ ---        │
      │ date       │
      ╞════════════╡
      │ 1970-01-01 │
      └────────────┘

# as_polars_expr works for classes Date (2)

    Code
      out
    Output
      Series[literal]

---

    Code
      selected_out
    Output
      shape: (2, 1)
      ┌────────────┐
      │ literal    │
      │ ---        │
      │ date       │
      ╞════════════╡
      │ 1970-01-01 │
      │ 1970-01-02 │
      └────────────┘

# as_polars_expr works for classes series (0)

    Code
      out
    Output
      Series

---

    Code
      selected_out
    Output
      shape: (0, 1)
      ┌──────┐
      │      │
      │ ---  │
      │ bool │
      ╞══════╡
      └──────┘

# as_polars_expr works for classes series (1)

    Code
      out
    Output
      Series

---

    Code
      selected_out
    Output
      shape: (1, 1)
      ┌──────┐
      │      │
      │ ---  │
      │ bool │
      ╞══════╡
      │ true │
      └──────┘

# as_polars_expr works for classes series (2)

    Code
      out
    Output
      Series

---

    Code
      selected_out
    Output
      shape: (2, 1)
      ┌───────┐
      │       │
      │ ---   │
      │ bool  │
      ╞═══════╡
      │ true  │
      │ false │
      └───────┘


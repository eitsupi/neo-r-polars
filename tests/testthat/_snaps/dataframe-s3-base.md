# `[` operator works to subset columns only

    Code
      test[, 10:12]
    Condition
      Error in `test[, 10:12]`:
      ! Can't subset columns past the end.
      i Location(s) 10, 11, and 12 don't exist.
      i There are only 3 columns.

---

    Code
      test[, -2:1]
    Condition
      Error in `test[, -2:1]`:
      ! Can't subset columns with `-2:1`.
      x Negative and positive locations can't be mixed.
      i Subscript `-2:1` has a positive value at location 4.

---

    Code
      test[, 1:-2]
    Condition
      Error in `test[, 1:-2]`:
      ! Can't subset columns with `1:-2`.
      x Negative and positive locations can't be mixed.
      i Subscript `1:-2` has a negative value at location 3.

# `[` operator works to subset rows only

    Code
      test[-2:1, ]
    Condition
      Error in `test[-2:1, ]`:
      ! Can't subset rows with `-2:1`.
      x Negative and positive locations can't be mixed.
      i Subscript `-2:1` has a positive value at location 4.

---

    Code
      test[1:-2, ]
    Condition
      Error in `test[1:-2, ]`:
      ! Can't subset rows with `1:-2`.
      x Negative and positive locations can't be mixed.
      i Subscript `1:-2` has a negative value at location 3.

---

    Code
      test[mean, ]
    Condition
      Error in `test[mean, ]`:
      ! Can't subset rows with `mean`.
      i `mean` must be logical, numeric, or character, not a function.

---

    Code
      test[list(1), ]
    Condition
      Error in `test[list(1), ]`:
      ! Can't subset rows with `list(1)`.
      i `list(1)` must be logical, numeric, or character, not a list.


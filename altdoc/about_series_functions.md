# About Series functions

All functions that can be applied to an `Expr` can be applied to a `Series`.
For instance, one could use `str$to_uppercase()` in a `DataFrame` like this:

```r
pl$DataFrame(x = letters[1:3])$select(pl$col("x")$str$to_uppercase())

#> shape: (3, 1)
#> ┌─────┐
#> │ x   │
#> │ --- │
#> │ str │
#> ╞═════╡
#> │ A   │
#> │ B   │
#> │ C   │
#> └─────┘
```

If you had a `Series` instead, you could use it like this:

```r
pl$Series("x", letters[1:3])$str$to_uppercase()

#> shape: (3,)
#> Series: 'x' [str]
#> [
#> 	"A"
#> 	"B"
#> 	"C"
#> ]
```

Some functions in `Series` may not be available on `Expr`, such as
`$to_r_vector()`:

```r
pl$Series("x", letters[1:3])$to_r_vector()
#> [1] "a" "b" "c"
```

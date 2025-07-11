# NEWS

## polars (development version)

This is a completely rewritten new version of the polars R package.
The old version and this version are same in that they are based on Python Polars,
so the basic functions are the same, but the internals are completely different,
so there is a possibility that they will behave differently.

Major changes in the API are as follows:

### Class names and internal structures

In the previous version, the classes which are bindings to Rust structs like polars DataFrame
were external pointers.

```r
# Previous version
as_polars_df(mtcars) |> str()
#> Class 'RPolarsDataFrame' <externalptr>
```

In the new version, the classes such as DataFrame that users manipulate are custom environment objects
created entirely on the R side. (Similar to `{R6}` classes)

Class names are in snake case.

```r
# New version
as_polars_df(mtcars) |> str()
#> Classes 'polars_data_frame', 'polars_object' <environment: 0x5625e081b908>
```

Objects created from Rust structs are enclosed as members like `_df`, as in Python Polars.

To aboid confusion and conflicts, the names of classes defined on the Rust side
have been changed from the previous ones. (e.g. `RPolarsDataFrame` -> `PlRDataFrame` here)

```r
# New version
as_polars_df(mtcars)$`_df` |> str()
#> Class 'PlRDataFrame' <environment: 0x5625e0cfbf88>
```

### Type mapping

#### Conversion from R to Polars types

In the previous version, conversion from R to Polars was defined by the S3 method of
`as_polars_series()` generic function on the R side and branching by match arm on the Rust side.
And, conversion to Expression was done entirely on the Rust side.

In the new version, the default method of `as_polars_series()` has been changed to raise an error,
so all R classes converted to Polars need to define S3 methods for `as_polars_series()`.
In addition, a new function `as_polars_expr()` has been added, can be used to create a scalar literal expression.

Due to this change, conversion from unknown classes to Polars objects will fail.

```r
# Previous version
a <- 1
class(a) <- "foo"
as_polars_series(a)
#> polars Series: shape: (1,)
#> Series: '' [f64]
#> [
#>         1.0
#> ]
```

```r
# New version
a <- 1
class(a) <- "foo"
as_polars_series(a)
#> Error:
#> a <foo> object can't be converted to a polars Series.
#> Run `rlang::last_trace()` to see where the error occurred.
```

#### Conversion from Polars to R types

In the previous version, there were multiple methods for converting Series or DataFrame to R vectors or R lists,
but in the new version, they have been unified to `$to_r_vector()` of Series.

#### Treat Series of length 1 as scalar

In Polars, there is a Scalar class that represents a single value, different from a Series of length 1.
On the other hand, in R, a vector of length 1 is treated as a scalar.

In this new version, when converting a Series of length 1 to a Polars expression,
it is automatically converted to a scalar, allowing it to be treated like an R vector.

```r
# Previous version
series <- pl$Series("foo", 1)

pl$lit(series)
#> polars Expr: Series[foo]

# It works because vectors of length 1 are treated as scalar values
pl$DataFrame(bar = 1:2)$with_columns(foo = 1)
#> shape: (2, 2)
#> ┌─────┬─────┐
#> │ bar ┆ foo │
#> │ --- ┆ --- │
#> │ i32 ┆ f64 │
#> ╞═════╪═════╡
#> │ 1   ┆ 1.0 │
#> │ 2   ┆ 1.0 │
#> └─────┴─────┘

# But this one causes error
pl$DataFrame(bar = 1:2)$with_columns(series)
#> Error: Execution halted with the following contexts
#>    0: In R: in $with_columns()
#>    0: During function call [pl$DataFrame(bar = 1:2)$with_columns(series)]
#>    1: Encountered the following error in Rust-Polars:
#>         Series foo, length 1 doesn't match the DataFrame height of 2
#>
#>       If you want expression: Series[foo] to be broadcasted, ensure it is a scalar (for instance by adding '.first()').
```

```r
# New version
series <- pl$Series("foo", 1)

pl$lit(series)
#> 1.0

# To prevent conversion to scalar, specify `keep_series = TRUE` in as_polars_expr()
as_polars_expr(series, keep_series = TRUE)
#> Series[foo]

# It works (Note that the name of the Series will be lost when conversion to scalar)
pl$DataFrame(bar = 1:2)$with_columns(series)
#> shape: (2, 2)
#> ┌─────┬─────────┐
#> │ bar ┆ literal │
#> │ --- ┆ ---     │
#> │ i32 ┆ f64     │
#> ╞═════╪═════════╡
#> │ 1   ┆ 1.0     │
#> │ 2   ┆ 1.0     │
#> └─────┴─────────┘
```

#### Proprietary vector classes are removed

Related to type conversion, the previous version had its own vector classes introduced to represent
Polars' `Time` and `Binary` types in R (The class names are `PTime` and `rpolars_raw_list`).

Since these are alternatives to the classes provided by widely used external packages `{hms}` and `{blob}`,
they were dropped in favor of them.

We think it is rare for users to handle data saved as these classes, but we can migrate data to the other classes
by the following steps.

- `PTime` objects can be converted to `hms` objects by `hms::as_hms()` after `as.vector(<PTime>)`
  to remove the attributes of the `PTime` object.
  However, it can be converted as it is only when the time unit of `PTime` is seconds.
  In other cases, division processing to convert the unit to seconds is required after removing the attributes.
- `blob::as_blob(unclass(<rpolars_raw_list>))` can be used to convert `rpolars_raw_list` to `blob`.

The new version does not support these proprietary classes at all,
and `hms` and `blob` are fully supported.

```r
# Previous version
r_df <- tibble::tibble(
  time = hms::as_hms(c("12:00:00", NA, "14:00:00")),
  binary = blob::as_blob(c(1L, NA, 2L)),
)

## R to Polars
pl_df <- as_polars_df(r_df)
pl_df
#> shape: (3, 2)
#> ┌─────────┬──────────────┐
#> │ time    ┆ binary       │
#> │ ---     ┆ ---          │
#> │ f64     ┆ list[binary] │
#> ╞═════════╪══════════════╡
#> │ 43200.0 ┆ [b"\x01"]    │
#> │ null    ┆ []           │
#> │ 50400.0 ┆ [b"\x02"]    │
#> └─────────┴──────────────┘

## Polars to R
tibble::as_tibble(pl_df)
#> # A tibble: 3 × 2
#>    time binary
#>   <dbl> <list>
#> 1 43200 <rplrs_r_ [1]>
#> 2    NA <rplrs_r_ [0]>
#> 3 50400 <rplrs_r_ [1]>
```

```r
# New version
r_df <- tibble::tibble(
  time = hms::as_hms(c("12:00:00", NA, "14:00:00")),
  binary = blob::as_blob(c(1L, NA, 2L)),
)

## R to Polars
pl_df <- as_polars_df(r_df)
pl_df
#> shape: (3, 2)
#> ┌──────────┬─────────┐
#> │ time     ┆ binary  │
#> │ ---      ┆ ---     │
#> │ time     ┆ binary  │
#> ╞══════════╪═════════╡
#> │ 12:00:00 ┆ b"\x01" │
#> │ null     ┆ null    │
#> │ 14:00:00 ┆ b"\x02" │
#> └──────────┴─────────┘

## Polars to R
tibble::as_tibble(pl_df)
#> # A tibble: 3 × 2
#>   time      binary
#>   <time>    <blob>
#> 1 12:00  <raw 1 B>
#> 2    NA         NA
#> 3 14:00  <raw 1 B>
```

### Dynamic dots features

This package has started to use [dynamic-dots](https://rlang.r-lib.org/reference/dyn-dots.html) actively.

### Single string handling

In the previous version, we could give a character vector of length 2 or more to `pl$col()`,
but in the new version, all elements must be of length 1.
If we want to use a vector of length 2 or more, we need to expand it using `!!!`.

```r
# Previous version
pl$col(c("foo", "bar"), "baz")
#> polars Expr: cols(["foo", "bar", "baz"])
```

```r
# New version
pl$col(!!!c("foo", "bar"), "baz")
#> cols(["foo", "bar", "baz"])
```

#### Argument name changes

Argument names of functions which have dynamic-dots may have dot (`.`) prefix.

```r
# Previous version
as_polars_df(mtcars)$group_by("cyl", maintain_order = TRUE)$agg()
#> shape: (3, 1)
#> ┌─────┐
#> │ cyl │
#> │ --- │
#> │ f64 │
#> ╞═════╡
#> │ 6.0 │
#> │ 4.0 │
#> │ 8.0 │
#> └─────┘

as_polars_df(mtcars)$group_by("cyl", .maintain_order = TRUE)$agg()
#> shape: (3, 2)
#> ┌─────┬─────────────────┐
#> │ cyl ┆ .maintain_order │
#> │ --- ┆ ---             │
#> │ f64 ┆ bool            │
#> ╞═════╪═════════════════╡
#> │ 8.0 ┆ true            │
#> │ 4.0 ┆ true            │
#> │ 6.0 ┆ true            │
#> └─────┴─────────────────┘
```

```r
# New version
as_polars_df(mtcars)$group_by("cyl", maintain_order = TRUE)$agg()
#> shape: (3, 2)
#> ┌─────┬────────────────┐
#> │ cyl ┆ maintain_order │
#> │ --- ┆ ---            │
#> │ f64 ┆ bool           │
#> ╞═════╪════════════════╡
#> │ 8.0 ┆ true           │
#> │ 4.0 ┆ true           │
#> │ 6.0 ┆ true           │
#> └─────┴────────────────┘
#> Warning message:
#> ! In `$group_by()`, `...` contain an argument named `maintain_order`.
#> ℹ You may want to specify the argument `.maintain_order` instead.

as_polars_df(mtcars)$group_by("cyl", .maintain_order = TRUE)$agg()
#> shape: (3, 1)
#> ┌─────┐
#> │ cyl │
#> │ --- │
#> │ f64 │
#> ╞═════╡
#> │ 6.0 │
#> │ 4.0 │
#> │ 8.0 │
#> └─────┘
```

#### Simplification of class constructor functions

In the new version, since conversion from R classes to Polars classes is completely done through
generic functions like `as_polars_df()`, functions that mimic class constructors of Python Polars
such as `pl$DataFrame()` have basically become shortcuts to `as_polars_*` functions.
For example, `pl$DataFrame(...)` is a shortcut for `rlang::list2(...) |> as_polars_df()`.

In previous versions, `pl$DataFrame()` had special handling when passed a data frame as an argument,
but such things have been removed, so you need to switch to `as_polars_df()` or use `!!!` to
combine arguments in `rlang::list2()`.

```r
# Previous version
pl$DataFrame(data.frame(x = 1, y = "a"))
#> shape: (1, 2)
#> ┌─────┬─────┐
#> │ x   ┆ y   │
#> │ --- ┆ --- │
#> │ f64 ┆ str │
#> ╞═════╪═════╡
#> │ 1.0 ┆ a   │
#> └─────┴─────┘
```

```r
# New version
pl$DataFrame(data.frame(x = 1, y = "a"))
#> shape: (1, 1)
#> ┌───────────┐
#> │           │
#> │ ---       │
#> │ struct[2] │
#> ╞═══════════╡
#> │ {1.0,"a"} │
#> └───────────┘

pl$DataFrame(!!!data.frame(x = 1, y = "a"))
#> shape: (1, 2)
#> ┌─────┬─────┐
#> │ x   ┆ y   │
#> │ --- ┆ --- │
#> │ f64 ┆ str │
#> ╞═════╪═════╡
#> │ 1.0 ┆ a   │
#> └─────┴─────┘
```

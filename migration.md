# Migration

`polars` v1.0.0 [TODO: maybe change this] is completely rewritten new version of the `polars` R package.
The old version and this version are similar in that they are based on Python Polars, so most of the functions of the package have the same behavior (see the "Syntax changes" section below), but the internals are completely different, so there is a possibility that they will behave differently.

This guide documents the major changes in the API as well as the steps needed to adapt code from pre-v1 to post-v1.

## Class names and internal structures

### Class names

Before, class names of `polars` objects had camel case, such as `RPolarsDataFrame` and `RPolarsSeries`. In the new version, all class names are in snake case, e.g. `polars_data_frame` and `polars_series`.

On the Rust side, the names of classes now start with `PlR`, e.g. `PlRDataFrame` and `PlRDataType`.

### Internal structures

In the previous version, the classes which are bindings to Rust structs like polars DataFrame were external pointers.

In the new version, the classes such as DataFrame that users manipulate are custom environment objects created entirely on the R side^[This is similar to `R6` classes for instance.].

Objects created from Rust structs are enclosed as members like `_df`, as in Python Polars.

## Conversion between R and Polars

### Conversion from R to Polars types

Converting R objects to `polars` is still done via `as_polars_series()`.
However, in the new version, the default method of `as_polars_series()` has been changed to raise an error, so all R classes converted to Polars need to define S3 methods for `as_polars_series()`.

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

In addition, there is now a function `as_polars_expr()` to create a scalar literal expression.

### Conversion from Polars to R types

In the previous version, there were multiple methods for converting Series or DataFrame to R vectors or R lists. For instance, one could transform a polars DataFrame to an R data.frame with `$to_data_frame()` or to a list with `$to_list()`.

In the new version, converting polars structures to R requires using standard `as.*` functions, such as `as.data.frame()`, `as.list()`, or `as.vector()`.

<!-- TODO: why do we export `to_r_vector()`? Isn't `as.vector()` enough? -->
<!-- but in the new version, they have been unified to `$to_r_vector()` of Series. -->

### Proprietary vector classes

The previous version had its own vector classes introduced to represent Polars `Time` and `Binary` types in R. Those own vector classes were `PTime` and `rpolars_raw_list`.

In the new version, those classes are dropped. Instead, conversion of those types from Polars to R uses classes provided by the widely used external packages `{hms}` and `{blob}`.

We think it is rare for users to handle data saved as these classes, but we can migrate data to the other classes by the following steps.

- `PTime` objects can be converted to `hms` objects by `hms::as_hms()` after `as.vector(<PTime>)` to remove the attributes of the `PTime` object.
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

## Syntax changes

### Inputs of `pl$col()`

In the previous version, we could give a character vector of length 2 or more to `pl$col()`,
but in the new version, all elements must be of length 1.
If we want to use a vector of length 2 or more, we need to expand it using [`!!!`](https://rlang.r-lib.org/reference/splice-operator.html).

```r
# Previous version
pl$col(c("foo", "bar"), "baz")
#> polars Expr: cols(["foo", "bar", "baz"])
```

```r
# New version
pl$col(c("foo", "bar"), "baz")
#> Error in `neopolars::pl$col()`:
#> ! Evaluation failed in `$col()`.
#> Caused by error in `neopolars::pl$col()`:
#> ! Invalid input for `pl$col()`
#> ℹ `pl$col()` accepts either single strings or polars data types
pl$col(!!!c("foo", "bar"), "baz")
#> cols(["foo", "bar", "baz"])
```

### Argument name changes

This package has started to use [dynamic-dots](https://rlang.r-lib.org/reference/dyn-dots.html) actively.

Consequently, argument names of functions which have dynamic-dots have a dot (`.`) prefix, e.g. `.maintain_order` or `.strict`.

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

# New version
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

Be careful about this change since passing `maintain_order = TRUE` is a valid group!

```r
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
```

### Constructing Polars objects

One can still construct DataFrames and LazyFrames with `pl$DataFrame()` and `pl$LazyFrame()`, e.g.

```r
pl$DataFrame(x = 1:3, b = c("a", "b", "c"), c = factor(letters[4:6]))
#> shape: (3, 3)
#> ┌─────┬─────┬─────┐
#> │ x   ┆ b   ┆ c   │
#> │ --- ┆ --- ┆ --- │
#> │ i32 ┆ str ┆ cat │
#> ╞═════╪═════╪═════╡
#> │ 1   ┆ a   ┆ d   │
#> │ 2   ┆ b   ┆ e   │
#> │ 3   ┆ c   ┆ f   │
#> └─────┴─────┴─────┘
```

However, their behavior with `data.frame` inputs have changed.
In previous versions, `pl$DataFrame()` had special handling when passed a data frame as an argument, so that `pl$DataFrame(mtcars)` was valid code.
This introduced unneeded complexity and this has been removed in the new version.
Converting a `data.frame` to a DataFrame now requires either `as_polars_df()` or using `!!!`:

```r
df <- data.frame(x = 1:3, b = c("a", "b", "c"), c = factor(letters[4:6]))
as_polars_df(df)
#> shape: (3, 3)
#> ┌─────┬─────┬─────┐
#> │ x   ┆ b   ┆ c   │
#> │ --- ┆ --- ┆ --- │
#> │ i32 ┆ str ┆ cat │
#> ╞═════╪═════╪═════╡
#> │ 1   ┆ a   ┆ d   │
#> │ 2   ┆ b   ┆ e   │
#> │ 3   ┆ c   ┆ f   │
#> └─────┴─────┴─────┘
pl$DataFrame(!!!df)
#> shape: (3, 3)
#> ┌─────┬─────┬─────┐
#> │ x   ┆ b   ┆ c   │
#> │ --- ┆ --- ┆ --- │
#> │ i32 ┆ str ┆ cat │
#> ╞═════╪═════╪═════╡
#> │ 1   ┆ a   ┆ d   │
#> │ 2   ┆ b   ┆ e   │
#> │ 3   ┆ c   ┆ f   │
#> └─────┴─────┴─────┘
```

Passing a `data.frame` to `pl$DataFrame()` is still valid code but now creates a `Struct`:


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
```

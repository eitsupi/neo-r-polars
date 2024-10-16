# TODO: link to data type docs
# TODO: section for name spaces
# source: https://docs.pola.rs/user-guide/concepts/expressions
#' Polars expression class (`polars_expr`)
#'
#' An expression is a tree of operations that describe how to construct one or more [Series].
#' As the outputs are [Series], it is straightforward to apply a sequence of expressions each of
#' which transforms the output from the previous step.
#' See examples for details. expression
#' @seealso
#' - [`pl$lit()`][pl__lit]: Create a literal expression.
#' - [`pl$col()`][pl__col]: Create an expression representing column(s) in a [DataFrame].
#' @name polars_expr
#' @examples
#' # An expression:
#' # 1. Select column `foo`,
#' # 2. Then sort the column (not in reversed order)
#' # 3. Then take the first two values of the sorted output
#' pl$col("foo")$sort()$head(2)
#'
#' # Expressions will be evaluated inside a context, such as `<DataFrame>$select()`
#' df <- pl$DataFrame(
#'   foo = c(1, 2, 1, 2, 3),
#'   bar = c(5, 4, 3, 2, 1),
#' )
#'
#' df$select(
#'   pl$col("foo")$sort()$head(3), # Return 3 values
#'   pl$col("bar")$filter(pl$col("foo") == 1)$sum(), # Return a single value
#' )
NULL

# The env storing expr namespaces
polars_namespaces_expr <- new.env(parent = emptyenv())

# The env storing expr methods
polars_expr__methods <- new.env(parent = emptyenv())

#' @export
wrap.PlRExpr <- function(x, ...) {
  self <- new.env(parent = emptyenv())
  self$`_rexpr` <- x

  lapply(names(polars_expr__methods), function(name) {
    fn <- polars_expr__methods[[name]]
    environment(fn) <- environment()
    assign(name, fn, envir = self)
  })

  lapply(names(polars_namespaces_expr), function(namespace) {
    makeActiveBinding(namespace, function() polars_namespaces_expr[[namespace]](self), self)
  })

  class(self) <- c("polars_expr", "polars_object")
  self
}

pl__deserialize_expr <- function(data, ..., format = c("binary", "json")) {
  wrap({
    check_dots_empty0(...)

    format <- arg_match0(format, c("binary", "json"))

    switch(format,
      binary = PlRExpr$deserialize_binary(data),
      json = PlRExpr$deserialize_json(data),
      abort("Unreachable")
    )
  })
}

#' Add two expressions
#'
#' Method equivalent of addition operator `expr + other`.
#' @param other numeric or string value; accepts expression input.
#' @return [Expr][expr__class]
#' @seealso
#' - [Arithmetic operators][S3_arithmetic]
#' @examples
#' df <- pl$DataFrame(x = 1:5)
#'
#' df$with_columns(
#'   `x+int` = pl$col("x")$add(2L),
#'   `x+expr` = pl$col("x")$add(pl$col("x")$cum_prod())
#' )
#'
#' df <- pl$DataFrame(
#'   x = c("a", "d", "g"),
#'   y = c("b", "e", "h"),
#'   z = c("c", "f", "i")
#' )
#'
#' df$with_columns(
#'   pl$col("x")$add(pl$col("y"))$add(pl$col("z"))$alias("xyz")
#' )
expr__add <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$add(other$`_rexpr`)
  })
}

#' Substract two expressions
#'
#' Method equivalent of subtraction operator `expr - other`.
#' @inherit expr__truediv params return
#' @seealso
#' - [Arithmetic operators][S3_arithmetic]
#' @examples
#' df <- pl$DataFrame(x = 0:4)
#'
#' df$with_columns(
#'   `x-2` = pl$col("x")$sub(2),
#'   `x-expr` = pl$col("x")$sub(pl$col("x")$cum_sum())
#' )
expr__sub <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$sub(other$`_rexpr`)
  })
}

#' Multiply two expressions
#'
#' Method equivalent of multiplication operator `expr * other`.
#' @inherit expr__truediv params return
#' @seealso
#' - [Arithmetic operators][S3_arithmetic]
#' @examples
#' df <- pl$DataFrame(x = c(1, 2, 4, 8, 16))
#'
#' df$with_columns(
#'   `x*2` = pl$col("x")$mul(2),
#'   `x * xlog2` = pl$col("x")$mul(pl$col("x")$log(2))
#' )
expr__mul <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$mul(other$`_rexpr`)
  })
}

#' Divide two expressions
#'
#' Method equivalent of float division operator `expr / other`.
#'
#' Zero-division behaviour follows IEEE-754:
#' - `0/0`: Invalid operation - mathematically undefined, returns `NaN`.
#' - `n/0`: On finite operands gives an exact infinite result, e.g.: ±infinity.
#' @inherit expr__add return
#' @param other Numeric literal or expression value.
#' @seealso
#' - [Arithmetic operators][S3_arithmetic]
#' - [`<Expr>$floor_div()`][Expr_floor_div]
#' @examples
#' df <- pl$DataFrame(
#'   x = -2:2,
#'   y = c(0.5, 0, 0, -4, -0.5)
#' )
#'
#' df$with_columns(
#'   `x/2` = pl$col("x")$div(2),
#'   `x/y` = pl$col("x")$div(pl$col("y"))
#' )
expr__truediv <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$div(other$`_rexpr`)
  })
}

#' Exponentiation using two expressions
#'
#' Method equivalent of exponentiation operator `expr ^ exponent`.
#'
#' @param exponent Numeric literal or expression value.
#' @inherit expr__truediv return
#' @seealso
#' - [Arithmetic operators][S3_arithmetic]
#' @examples
#' df <- pl$DataFrame(x = c(1, 2, 4, 8))
#'
#' df$with_columns(
#'   cube = pl$col("x")$pow(3),
#'   `x^xlog2` = pl$col("x")$pow(pl$col("x")$log(2))
#' )
expr__pow <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$pow(other$`_rexpr`)
  })
}

#' Modulo using two expressions
#'
#' Method equivalent of modulus operator `expr %% other`.
#' @inherit expr__truediv params return
#' @seealso
#' - [Arithmetic operators][S3_arithmetic]
#' - [`<Expr>$floor_div()`][expr__floor_div]
#' @examples
#' df <- pl$DataFrame(x = -5L:5L)
#'
#' df$with_columns(
#'   `x%%2` = pl$col("x")$mod(2)
#' )
expr__mod <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$rem(other$`_rexpr`)
  })
}

#' Floor divide using two expressions
#'
#' Method equivalent of floor division operator `expr %/% other`.
#' @inherit expr__truediv params return
#' @seealso
#' - [Arithmetic operators][S3_arithmetic]
#' - [`<Expr>$div()`][expr__truediv]
#' - [`<Expr>$mod()`][expr__mod]
#' @examples
#' df <- pl$DataFrame(x = 1:5)
#'
#' df$with_columns(
#'   `x/2` = pl$col("x")$div(2),
#'   `x%/%2` = pl$col("x")$floor_div(2)
#' )
expr__floor_div <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$floor_div(other$`_rexpr`)
  })
}


expr__neg <- function() {
  self$`_rexpr`$neg() |>
    wrap()
}

#' Check equality
#'
#' @inherit expr__add description params return
#'
#' @seealso [expr__eq_missing]
#' @examples
#' pl$lit(2) == 2
#' pl$lit(2) == pl$lit(2)
#' pl$lit(2)$eq(pl$lit(2))
expr__eq <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$eq(other$`_rexpr`)
  })
}

#' Check equality without `null` propagation
#'
#' @inherit expr__add description params return
#'
#' @seealso [expr__eq]
#' @examples
#' df <- pl$DataFrame(x = c(NA, FALSE, TRUE), y = c(TRUE, TRUE, TRUE))
#' df$with_columns(
#'   eq = pl$col("x")$eq("y"),
#'   eq_missing = pl$col("x")$eq_missing("y")
#' )
expr__eq_missing <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$eq_missing(other$`_rexpr`)
  })
}

#' Check inequality
#'
#' @inherit expr__add description params return
#'
#' @seealso [expr__neq_missing]
#' @examples
#' pl$lit(1) != 2
#' pl$lit(1) != pl$lit(2)
#' pl$lit(1)$neq(pl$lit(2))
expr__neq <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$neq(other$`_rexpr`)
  })
}

#' Check inequality without `null` propagation
#'
#' @inherit expr__add description params return
#'
#' @seealso [expr__neq]
#' @examples
#' df <- pl$DataFrame(x = c(NA, FALSE, TRUE), y = c(TRUE, TRUE, TRUE))
#' df$with_columns(
#'   neq = pl$col("x")$neq("y"),
#'   neq_missing = pl$col("x")$neq_missing("y")
#' )
expr__neq_missing <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$neq_missing(other$`_rexpr`)
  })
}

#' Check greater or equal inequality
#'
#' @inherit expr__add description params return
#'
#' @examples
#' pl$lit(2) >= 2
#' pl$lit(2) >= pl$lit(2)
#' pl$lit(2)$gt_eq(pl$lit(2))
expr__gt <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$gt(other$`_rexpr`)
  })
}

#' Check greater or equal inequality
#'
#' @inherit expr__add description params return
#'
#' @examples
#' pl$lit(2) >= 2
#' pl$lit(2) >= pl$lit(2)
#' pl$lit(2)$gt_eq(pl$lit(2))
expr__gt_eq <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$gt_eq(other$`_rexpr`)
  })
}

#' Check lower or equal inequality
#'
#' @inherit expr__add description params return
#'
#' @examples
#' pl$lit(2) <= 2
#' pl$lit(2) <= pl$lit(2)
#' pl$lit(2)$lt_eq(pl$lit(2))
expr__lt_eq <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$lt_eq(other$`_rexpr`)
  })
}

#' Check strictly lower inequality
#'
#' @inherit expr__add description params return
#'
#' @examples
#' pl$lit(5) < 10
#' pl$lit(5) < pl$lit(10)
#' pl$lit(5)$lt(pl$lit(10))
expr__lt <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$lt(other$`_rexpr`)
  })
}

#' Rename Expr output
#'
#' Rename the output of an expression.
#'
#' @param name New name of output
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$col("bob")$alias("alice")
expr__alias <- function(name) {
  self$`_rexpr`$alias(name) |>
    wrap()
}

#' Negate a boolean expression
#'
#' Method equivalent of negation operator `!expr`.
#' @inherit expr__add return
#' @examples
#' # two syntaxes same result
#' pl$lit(TRUE)$not()
#' !pl$lit(TRUE)
expr__not <- function() {
  self$`_rexpr`$not() |>
    wrap()
}

# Beacuse the $not method and the $invert method are distinguished in the selector,
# this is only necessary to map the $invert method to the `!` operator.

expr__invert <-
  expr__not


expr__is_null <- function() {
  self$`_rexpr`$is_null() |>
    wrap()
}

#' Check if elements are NULL
#'
#' Returns a boolean Series indicating which values are null.
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(x = c(1, NA, 3))$select(pl$col("x")$is_null())
expr__is_not_null <- function() {
  self$`_rexpr`$is_not_null() |>
    wrap()
}

#' Check if elements are infinite
#'
#' Returns a boolean Series indicating which values are infinite.
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(alice = c(0, NaN, NA, Inf, -Inf))$
#'   with_columns(infinite = pl$col("alice")$is_infinite())
expr__is_infinite <- function() {
  self$`_rexpr`$is_infinite() |>
    wrap()
}

#' Check if elements are finite
#'
#' Returns a boolean Series indicating which values are finite.
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(alice = c(0, NaN, NA, Inf, -Inf))$
#'   with_columns(finite = pl$col("alice")$is_finite())
expr__is_finite <- function() {
  self$`_rexpr`$is_finite() |>
    wrap()
}

#' Check if elements are NaN
#'
#' Returns a boolean Series indicating which values are NaN.
#'
#' @inherit as_polars_expr return
#'
#' @examples
#' pl$DataFrame(alice = c(0, NaN, NA, Inf, -Inf))$
#'   with_columns(nan = pl$col("alice")$is_nan())
expr__is_nan <- function() {
  self$`_rexpr`$is_nan() |>
    wrap()
}

#' Check if elements are not NaN
#'
#' Returns a boolean Series indicating which values are not NaN. Syntactic sugar
#' for `$is_nan()$not()`.
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(alice = c(0, NaN, NA, Inf, -Inf))$
#'   with_columns(not_nan = pl$col("alice")$is_not_nan())
expr__is_not_nan <- function() {
  self$`_rexpr`$is_not_nan() |>
    wrap()
}

#' Get minimum value
#'
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(x = c(1, NA, 3))$
#'   with_columns(min = pl$col("x")$min())
expr__min <- function() {
  self$`_rexpr`$min() |>
    wrap()
}

#' Get maximum value
#'
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(x = c(1, NA, 3))$
#'   with_columns(max = pl$col("x")$max())
expr__max <- function() {
  self$`_rexpr`$max() |>
    wrap()
}

#' Get maximum value with NaN
#'
#' Get maximum value, but returns `NaN` if there are any.
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(x = c(1, NA, 3, NaN, Inf))$
#'   with_columns(nan_max = pl$col("x")$nan_max())
expr__nan_max <- function() {
  self$`_rexpr`$nan_max() |>
    wrap()
}

#' Get minimum value with NaN
#'
#' Get minimum value, but returns `NaN` if there are any.
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(x = c(1, NA, 3, NaN, Inf))$
#'   with_columns(nan_min = pl$col("x")$nan_min())
expr__nan_min <- function() {
  self$`_rexpr`$nan_min() |>
    wrap()
}

#' Get mean value
#'
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(x = c(1L, NA, 2L))$
#'   with_columns(mean = pl$col("x")$mean())
expr__mean <- function() {
  self$`_rexpr`$mean() |>
    wrap()
}

#' Get median value
#'
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(x = c(1L, NA, 2L))$
#'   with_columns(median = pl$col("x")$median())
expr__median <- function() {
  self$`_rexpr`$median() |>
    wrap()
}

#' Get sum value
#'
#' @details
#' The dtypes Int8, UInt8, Int16 and UInt16 are cast to Int64 before summing to
#' prevent overflow issues.
#'
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(x = c(1L, NA, 2L))$
#'   with_columns(sum = pl$col("x")$sum())
expr__sum <- function() {
  self$`_rexpr`$sum() |>
    wrap()
}

#' Cast between DataType
#'
#' @param dtype DataType to cast to.
#' @param strict If `TRUE` (default), an error will be thrown if cast failed at
#' resolve time.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = 1:3, b = c(1, 2, 3))
#' df$with_columns(
#'   pl$col("a")$cast(pl$dtypes$Float64),
#'   pl$col("b")$cast(pl$dtypes$Int32)
#' )
#'
#' # strict FALSE, inserts null for any cast failure
#' pl$lit(c(100, 200, 300))$cast(pl$dtypes$UInt8, strict = FALSE)$to_series()
#'
#' # strict TRUE, raise any failure as an error when query is executed.
#' tryCatch(
#'   {
#'     pl$lit("a")$cast(pl$dtypes$Float64, strict = TRUE)$to_series()
#'   },
#'   error = function(e) e
#' )
expr__cast <- function(dtype, ..., strict = TRUE, wrap_numerical = FALSE) {
  wrap({
    check_dots_empty0(...)
    check_polars_dtype(dtype)

    self$`_rexpr`$cast(dtype$`_dt`, strict, wrap_numerical)
  })
}

#' Sort an Expr
#'
#' Sort this column. If used in a groupby context, the groups are sorted.
#'
#' @inheritParams Series_sort
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(6, 1, 0, NA, Inf, NaN))$
#'   with_columns(sorted = pl$col("a")$sort())
expr__sort <- function(..., descending = FALSE, nulls_last = FALSE) {
  wrap({
    check_dots_empty0(...)
    self$`_rexpr`$sort_with(descending, nulls_last)
  })
}

#' Index of a sort
#'
#' Get the index values that would sort this column.
#'
#' @inherit expr__sort params return
#' @seealso [pl$arg_sort_by()][pl_arg_sort_by()] to find the row indices that would
#' sort multiple columns.
#' @examples
#' pl$DataFrame(
#'   a = c(6, 1, 0, NA, Inf, NaN)
#' )$with_columns(arg_sorted = pl$col("a")$arg_sort())
expr__arg_sort <- function(..., descending = FALSE, nulls_last = FALSE) {
  wrap({
    check_dots_empty0(...)
    self$`_rexpr`$arg_sort(descending, nulls_last)
  })
}

# TODO: rewrite `by` to `...` <https://github.com/pola-rs/r-polars/pull/997>
#' Sort Expr by order of others
#'
#' Sort this column by the ordering of another column, or multiple other columns.
#' If used in a groupby context, the groups are sorted.
#'
#' @param by One expression or a list of expressions and/or strings (interpreted
#'  as column names).
#' @param maintain_order A logical to indicate whether the order should be maintained
#' if elements are equal.
#' @inheritParams Series_sort
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   group = c("a", "a", "a", "b", "b", "b"),
#'   value1 = c(98, 1, 3, 2, 99, 100),
#'   value2 = c("d", "f", "b", "e", "c", "a")
#' )
#'
#' # by one column/expression
#' df$with_columns(
#'   sorted = pl$col("group")$sort_by("value1")
#' )
#'
#' # by two columns/expressions
#' df$with_columns(
#'   sorted = pl$col("group")$sort_by(
#'     list("value2", pl$col("value1")),
#'     descending = c(TRUE, FALSE)
#'   )
#' )
#'
#' # by some expression
#' df$with_columns(
#'   sorted = pl$col("group")$sort_by(pl$col("value1")$sort(descending = TRUE))
#' )
expr__sort_by <- function(
    ...,
    descending = FALSE,
    nulls_last = FALSE,
    multithreaded = TRUE,
    maintain_order = FALSE) {
  wrap({
    check_dots_unnamed()

    by <- parse_into_list_of_expressions(...)
    descending <- extend_bool(descending, length(by), "descending", "...")
    nulls_last <- extend_bool(nulls_last, length(by), "nulls_last", "...")

    self$`_rexpr`$sort_by(by, descending, nulls_last, multithreaded, maintain_order)
  })
}

#' Reverse a variable
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = 1:5)$select(pl$col("a")$reverse())
expr__reverse <- function() {
  self$`_rexpr`$reverse() |>
    wrap()
}


#' Get a slice of an Expr
#'
#' Performing a slice of length 1 on a subset of columns will recycle this value
#' in those columns but will not change the number of rows in the data. See
#' examples.
#'
#' @param offset Numeric or expression, zero-indexed. Indicates where to start
#' the slice. A negative value is one-indexed and starts from the end.
#' @param length Maximum number of elements contained in the slice. Default is
#' full data.
#'
#'
#' @inherit as_polars_expr return
#' @examples
#' # as head
#' pl$DataFrame(a = 0:100)$select(
#'   pl$all()$slice(0, 6)
#' )
#'
#' # as tail
#' pl$DataFrame(a = 0:100)$select(
#'   pl$all()$slice(-6, 6)
#' )
#'
#' pl$DataFrame(a = 0:100)$select(
#'   pl$all()$slice(80)
#' )
#'
#' # recycling
#' pl$DataFrame(mtcars)$with_columns(pl$col("mpg")$slice(0, 1)$first())
expr__slice <- function(offset, length = NULL) {
  self$`_rexpr`$slice(
    as_polars_expr(
      offset,
      as_lit = TRUE
    )$`_rexpr`$cast(pl$Int64$`_dt`, strict = FALSE, wrap_numerical = TRUE),
    as_polars_expr(
      length,
      as_lit = TRUE
    )$`_rexpr`$cast(pl$Int64$`_dt`, strict = FALSE, wrap_numerical = TRUE)
  ) |>
    wrap()
}

#' Get the first n elements
#'
#' @param n Number of elements to take.
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(x = 1:11)$select(pl$col("x")$head(3))
expr__head <- function(n = 10) {
  self$slice(0, n) |>
    wrap()
}

#' Get the last n elements
#'
#' @inheritParams expr__head
#'
#' @inherit as_polars_expr return
#'
#' @examples
#' pl$DataFrame(x = 1:11)$select(pl$col("x")$tail(3))
expr__tail <- function(n = 10) {
  wrap({
    # Supports unsigned integers
    offset <- -as_polars_expr(n, as_lit = TRUE)$cast(pl$Int64, strict = FALSE, wrap_numerical = TRUE)
    self$slice(offset, n)
  })
}

#' Get the first value.
#'
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(x = 3:1)$with_columns(first = pl$col("x")$first())
expr__first <- function() {
  self$`_rexpr`$first() |>
    wrap()
}

#' Get the last value
#'
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(x = 3:1)$with_columns(last = pl$col("x")$last())
expr__last <- function() {
  self$`_rexpr`$last() |>
    wrap()
}

#' Compute expressions over the given groups
#'
#' This expression is similar to performing a group by aggregation and
#' joining the result back into the original [DataFrame][DataFrame_class].
#' The outcome is similar to how window functions work in
#' [PostgreSQL](https://www.postgresql.org/docs/current/tutorial-window.html).
#'
#' @param ... Column(s) to group by. Accepts expression input.
#' Characters are parsed as column names.
#' @param order_by Order the window functions/aggregations with the partitioned
#' groups by the result of the expression passed to `order_by`. Can be an Expr.
#' Strings are parsed as column names.
#' @param mapping_strategy One of the following:
#' * `"group_to_rows"` (default): if the aggregation results in multiple values,
#'   assign them back to their position in the DataFrame. This can only be done
#'   if the group yields the same elements before aggregation as after.
#' * `"join"`: join the groups as `List<group_dtype>` to the row positions. Note
#'   that this can be memory intensive.
#' * `"explode"`: don’t do any mapping, but simply flatten the group. This only
#'   makes sense if the input data is sorted.
#'
#' @inherit as_polars_expr return
#' @examples
#' # Pass the name of a column to compute the expression over that column.
#' df <- pl$DataFrame(
#'   a = c("a", "a", "b", "b", "b"),
#'   b = c(1, 2, 3, 5, 3),
#'   c = c(5, 4, 2, 1, 3)
#' )
#'
#' df$with_columns(
#'   pl$col("c")$max()$over("a")$name$suffix("_max")
#' )
#'
#' # Expression input is supported.
#' df$with_columns(
#'   pl$col("c")$max()$over(pl$col("b") %/% 2)$name$suffix("_max")
#' )
#'
#' # Group by multiple columns by passing a character vector of column names
#' # or list of expressions.
#' df$with_columns(
#'   pl$col("c")$min()$over(c("a", "b"))$name$suffix("_min")
#' )
#'
#' df$with_columns(
#'   pl$col("c")$min()$over(list(pl$col("a"), pl$col("b")))$name$suffix("_min")
#' )
#'
#' # Or use positional arguments to group by multiple columns in the same way.
#' df$with_columns(
#'   pl$col("c")$min()$over("a", pl$col("b") %% 2)$name$suffix("_min")
#' )
#'
#' # Alternative mapping strategy: join values in a list output
#' df$with_columns(
#'   top_2 = pl$col("c")$top_k(2)$over("a", mapping_strategy = "join")
#' )
#'
#' # order_by specifies how values are sorted within a group, which is
#' # essential when the operation depends on the order of values
#' df <- pl$DataFrame(
#'   g = c(1, 1, 1, 1, 2, 2, 2, 2),
#'   t = c(1, 2, 3, 4, 4, 1, 2, 3),
#'   x = c(10, 20, 30, 40, 10, 20, 30, 40)
#' )
#'
#' # without order_by, the first and second values in the second group would
#' # be inverted, which would be wrong
#' df$with_columns(
#'   x_lag = pl$col("x")$shift(1)$over("g", order_by = "t")
#' )
expr__over <- function(
    ...,
    order_by = NULL,
    mapping_strategy = c("group_to_rows", "join", "explode")) {
  wrap({
    check_dots_unnamed()

    partition_by <- parse_into_list_of_expressions(...)
    if (!is.null(order_by)) {
      order_by <- parse_into_list_of_expressions(!!!order_by)
    }
    mapping_strategy <- arg_match0(mapping_strategy, c("group_to_rows", "join", "explode"))

    self$`_rexpr`$over(
      partition_by,
      order_by = order_by,
      order_by_descending = FALSE, # does not work yet
      order_by_nulls_last = FALSE, # does not work yet
      mapping_strategy = mapping_strategy
    )
  })
}

#' Filter a single column.
#'
#' Mostly useful in an aggregation context. If you want to filter on a
#' DataFrame level, use `DataFrame$filter()` (or `LazyFrame$filter()`).
#'
#' @param predicate An Expr or something coercible to an Expr. Must return a
#' boolean.
#'
#' @inherit as_polars_expr return
#'
#' @examples
#' df <- pl$DataFrame(
#'   group_col = c("g1", "g1", "g2"),
#'   b = c(1, 2, 3)
#' )
#' df
#'
#' df$group_by("group_col")$agg(
#'   lt = pl$col("b")$filter(pl$col("b") < 2),
#'   gte = pl$col("b")$filter(pl$col("b") >= 2)
#' )
expr__filter <- function(...) {
  parse_predicates_constraints_into_expression(...) |>
    self$`_rexpr`$filter() |>
    wrap()
}

## TODO Better explain aggregate list

#' Map an expression with an R function
#'
#' @param f a function to map with
#' @param output_type `NULL` or a type available in `names(pl$dtypes)`. If `NULL`
#' (default), the output datatype will match the input datatype. This is used
#' to inform schema of the actual return type of the R function. Setting this wrong
#' could theoretically have some downstream implications to the query.
#' @param agg_list Aggregate list. Map from vector to group in group_by context.
#' @param in_background Logical. Whether to execute the map in a background R
#' process. Combined with setting e.g. `options(polars.rpool_cap = 4)` it can speed
#' up some slow R functions as they can run in parallel R sessions. The
#' communication speed between processes is quite slower than between threads.
#' This will likely only give a speed-up in a "low IO - high CPU" use case.
#' If there are multiple [`$map_batches(in_background = TRUE)`][expr__map_batches]
#' calls in the query, they will be run in parallel.
#'
#'
#' @inherit as_polars_expr return
#' @details
#' It is sometimes necessary to apply a specific R function on one or several
#' columns. However, note that using R code in [`$map_batches()`][expr__map_batches]
#' is slower than native polars.
#' The user function must take one polars `Series` as input and the return
#' should be a `Series` or any Robj convertible into a `Series` (e.g. vectors).
#' Map fully supports `browser()`.
#'
#' If `in_background = FALSE` the function can access any global variable of the
#' R session. However, note that several calls to [`$map_batches()`][expr__map_batches]
#' will sequentially share the same main R session,
#' so the global environment might change between the start of the query and the moment
#' a [`$map_batches()`][expr__map_batches] call is evaluated. Any native
#' polars computations can still be executed meanwhile. If `in_background = TRUE`,
#' the map will run in one or more other R sessions and will not have access
#' to global variables. Use `options(polars.rpool_cap = 4)` and
#' `polars_options()$rpool_cap` to set and view number of parallel R sessions.
#'
#' @examples
#' pl$DataFrame(iris)$
#'   select(
#'   pl$col("Sepal.Length")$map_batches(\(x) {
#'     paste("cheese", as.character(x$to_vector()))
#'   }, pl$dtypes$String)
#' )
#'
#' # R parallel process example, use Sys.sleep() to imitate some CPU expensive
#' # computation.
#'
#' # map a,b,c,d sequentially
#' pl$LazyFrame(a = 1, b = 2, c = 3, d = 4)$select(
#'   pl$all()$map_batches(\(s) {
#'     Sys.sleep(.1)
#'     s * 2
#'   })
#' )$collect() |> system.time()
#'
#' # map in parallel 1: Overhead to start up extra R processes / sessions
#' options(polars.rpool_cap = 0) # drop any previous processes, just to show start-up overhead
#' options(polars.rpool_cap = 4) # set back to 4, the default
#' polars_options()$rpool_cap
#' pl$LazyFrame(a = 1, b = 2, c = 3, d = 4)$select(
#'   pl$all()$map_batches(\(s) {
#'     Sys.sleep(.1)
#'     s * 2
#'   }, in_background = TRUE)
#' )$collect() |> system.time()
#'
#' # map in parallel 2: Reuse R processes in "polars global_rpool".
#' polars_options()$rpool_cap
#' pl$LazyFrame(a = 1, b = 2, c = 3, d = 4)$select(
#'   pl$all()$map_batches(\(s) {
#'     Sys.sleep(.1)
#'     s * 2
#'   }, in_background = TRUE)
#' )$collect() |> system.time()
expr__map_batches <- function(
    lambda,
    return_dtype = NULL,
    ...,
    agg_list = FALSE) {
  wrap({
    check_dots_empty0(...)
    check_function(lambda)
    check_polars_dtype(return_dtype, allow_null = TRUE)

    self$`_rexpr`$map_batches(
      lambda = function(series) {
        as_polars_series(lambda(wrap(.savvy_wrap_PlRSeries(series))))$`_s`
      },
      output_type = return_dtype$`_dt`,
      agg_list = agg_list
    )
  })
}

#' Apply logical AND on two expressions
#'
#' Combine two boolean expressions with AND.
#' @inherit expr__add params return
#' @examples
#' pl$lit(TRUE) & TRUE
#' pl$lit(TRUE)$and(pl$lit(TRUE))
expr__and <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$and(other$`_rexpr`)
  })
}

#' Apply logical OR on two expressions
#'
#' Combine two boolean expressions with OR.
#'
#' @inherit expr__add params return
#' @examples
#' pl$lit(TRUE) | FALSE
#' pl$lit(TRUE)$or(pl$lit(TRUE))
expr__or <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$or(other$`_rexpr`)
  })
}

#' Apply logical XOR on two expressions
#'
#' Combine two boolean expressions with XOR.
#' @inherit expr__add params return
#' @examples
#' pl$lit(TRUE)$xor(pl$lit(FALSE))
expr__xor <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$xor(other$`_rexpr`)
  })
}

#' Difference
#'
#' Calculate the n-th discrete difference.
#'
#' @param n Number of slots to shift.
#' @param null_behavior String, either `"ignore"` (default), else `"drop"`.
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(20L, 10L, 30L, 40L))$with_columns(
#'   diff_default = pl$col("a")$diff(),
#'   diff_2_ignore = pl$col("a")$diff(2, "ignore")
#' )
expr__diff <- function(n = 1, null_behavior = c("ignore", "drop")) {
  wrap({
    null_behavior <- arg_match0(null_behavior, c("ignore", "drop"))
    self$`_rexpr`$diff(n, null_behavior)
  })
}

# TODO: link to ExprList_explode
#' Reshape this Expr to a flat Series or a Series of Lists
#'
#' @param dimensions A integer vector of length of the dimension size.
#' If `-1` is used in any of the dimensions, that dimension is inferred.
#' Currently, more than two dimensions not supported.
#' @param nested_type The nested data type to create. [List][DataType_List] only
#' supports 2 dimensions, whereas [Array][DataType_Array] supports an arbitrary
#' number of dimensions.
#' @return [Expr][expr__class].
#' If a single dimension is given, results in an expression of the original data
#' type. If a multiple dimensions are given, results in an expression of data
#' type List with shape equal to the dimensions.
#' @examples
#' df <- pl$DataFrame(foo = 1:9)
#'
#' df$select(pl$col("foo")$reshape(9))
#' df$select(pl$col("foo")$reshape(c(3, 3)))
#'
#' # Use `-1` to infer the other dimension
#' df$select(pl$col("foo")$reshape(c(-1, 3)))
#' df$select(pl$col("foo")$reshape(c(3, -1)))
#'
#' # One can specify more than 2 dimensions by using the Array type
#' df <- pl$DataFrame(foo = 1:12)
#' df$select(
#'   pl$col("foo")$reshape(c(3, 2, 2), nested_type = pl$Array(pl$Float32, 2))
#' )
expr__reshape <- function(dimensions) {
  self$`_rexpr`$reshape(dimensions) |>
    wrap()
}

#' Apply logical OR on a column
#'
#' Check if any boolean value in a Boolean column is `TRUE`.
#'
#' @inherit expr__all params return
#' @examples
#' df <- pl$DataFrame(
#'   a = c(TRUE, FALSE),
#'   b = c(FALSE, FALSE),
#'   c = c(NA, FALSE)
#' )
#'
#' df$select(pl$col("*")$any())
#'
#' # If we set ignore_nulls = FALSE, then we don't know if any values in column
#' # "c" is TRUE, so it returns null
#' df$select(pl$col("*")$any(ignore_nulls = FALSE))
expr__any <- function(..., ignore_nulls = TRUE) {
  wrap({
    check_dots_empty0(...)
    self$`_rexpr`$any(ignore_nulls)
  })
}

#' Apply logical AND on a column
#'
#' Check if all values in a Boolean column are `TRUE`. This method is an
#' expression - not to be confused with [`pl$all()`][pl_all] which is a function
#' to select all columns.
#'
#' @inheritParams rlang::check_dots_empty0
#' @param ignore_nulls If `TRUE` (default), ignore null values. If `FALSE`,
#' [Kleene logic](https://en.wikipedia.org/wiki/Three-valued_logic) is used to
#' deal with nulls: if the column contains any null values and no `TRUE` values,
#' the output is null.
#'
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   a = c(TRUE, TRUE),
#'   b = c(TRUE, FALSE),
#'   c = c(NA, TRUE),
#'   d = c(NA, NA)
#' )
#'
#' # By default, ignore null values. If there are only nulls, then all() returns
#' # TRUE.
#' df$select(pl$col("*")$all())
#'
#' # If we set ignore_nulls = FALSE, then we don't know if all values in column
#' # "c" are TRUE, so it returns null
#' df$select(pl$col("*")$all(ignore_nulls = FALSE))
expr__all <- function(..., ignore_nulls = TRUE) {
  wrap({
    check_dots_empty0(...)
    self$`_rexpr`$all(ignore_nulls)
  })
}

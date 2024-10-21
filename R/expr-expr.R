# TODO: link to data type docs
# TODO: section for name spaces
# source: https://docs.pola.rs/user-guide/concepts/expressions
#' Polars expression class (`polars_expr`)
#'
#' An expression is a tree of operations that describe how to construct one or more [Series].
#' As the outputs are [Series], it is straightforward to apply a sequence of expressions each of
#' which transforms the output from the previous step.
#' See examples for details.
#' @name polars_expr
#' @aliases Expr expression
#' @seealso
#' - [`pl$lit()`][pl__lit]: Create a literal expression.
#' - [`pl$col()`][pl__col]: Create an expression representing column(s) in a [DataFrame].
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
#' @param other Element to add. Can be a string (only if `expr` is a string), a
#' numeric value or an other expression.
#' @inherit as_polars_expr return
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
#' @inheritParams expr__truediv
#' @inherit as_polars_expr return
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
#' @inheritParams expr__truediv
#' @inherit as_polars_expr return
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
#' @inherit as_polars_expr return
#' @param other Numeric literal or expression value.
#' @seealso
#' - [Arithmetic operators][S3_arithmetic]
#' - [`<Expr>$floordiv()`][Expr_floordiv]
#' @examples
#' df <- pl$DataFrame(
#'   x = -2:2,
#'   y = c(0.5, 0, 0, -4, -0.5)
#' )
#'
#' df$with_columns(
#'   `x/2` = pl$col("x")$truediv(2),
#'   `x/y` = pl$col("x")$truediv(pl$col("y"))
#' )
expr__truediv <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$true_div(other$`_rexpr`)
  })
}

#' Exponentiation using two expressions
#'
#' Method equivalent of exponentiation operator `expr ^ exponent`.
#'
#' @param exponent Numeric literal or expression value.
#' @inherit as_polars_expr return
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
#' @inheritParams expr__truediv
#' @inherit as_polars_expr return
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
#' @inheritParams expr__truediv
#' @inherit as_polars_expr return
#' @seealso
#' - [Arithmetic operators][S3_arithmetic]
#' - [`<Expr>$truediv()`][expr__truediv]
#' - [`<Expr>$mod()`][expr__mod]
#' @examples
#' df <- pl$DataFrame(x = 1:5)
#'
#' df$with_columns(
#'   `x/2` = pl$col("x")$truediv(2),
#'   `x%/%2` = pl$col("x")$floordiv(2)
#' )
expr__floordiv <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$floor_div(other$`_rexpr`)
  })
}

# TODO-REWRITE: remove before next release
expr__floor_div <- function(other) {
  wrap({
    deprecate_warn("$floor_div() is deprecated. Use $floordiv() instead.")
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
#' @inherit expr__add description params
#' @inherit as_polars_expr return
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
#' @inherit expr__add description params
#' @inherit as_polars_expr return
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
#' @inherit expr__add description params
#' @inherit as_polars_expr return
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
#' @inherit expr__add description params
#' @inherit as_polars_expr return
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
#' @inherit expr__add description params
#' @inherit as_polars_expr return
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
#' @inherit expr__add description params
#' @inherit as_polars_expr return
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
#' @inherit expr__add description params
#' @inherit as_polars_expr return
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
#' @inherit expr__add description params
#' @inherit as_polars_expr return
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

#' Rename the expression
#'
#' @param name The new name.
#'
#' @inherit as_polars_expr return
#' @examples
#' # Rename an expression to avoid overwriting an existing column
#' df <- pl$DataFrame(a = 1:3, b = c("x", "y", "z"))
#' df$with_columns(
#'   pl$col("a") + 10,
#'   pl$col("b")$str$to_uppercase()$alias("c")
#' )
#'
#' # Overwrite the default name of literal columns to prevent errors due to
#' # duplicate column names.
#' df$with_columns(
#'   pl$lit(TRUE)$alias("c"),
#'   pl$lit(4)$alias("d")
#' )
expr__alias <- function(name) {
  self$`_rexpr`$alias(name) |>
    wrap()
}

# TODO-REWRITE: how should we handle the columns + *more_columns arguments of
# Python?
# #' Exclude columns from a multi-column expression.
# expr__exclude <- function(columns, ...) {
#   self$`_rexpr`$not() |>
#     wrap()
# }


#' Negate a boolean expression
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = c(TRUE, FALSE, FALSE, NA))
#'
#' df$with_columns(a_not = pl$col("a")$not())
#'
#' # Same result with "!"
#' df$with_columns(a_not = !pl$col("a"))
expr__not <- function() {
  self$`_rexpr`$not() |>
    wrap()
}

# Beacuse the $not method and the $invert method are distinguished in the selector,
# this is only necessary to map the $invert method to the `!` operator.

expr__invert <-
  expr__not

#' Check if elements are NULL
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   a = c(1, 2, NA, 1, 5),
#'   b = c(1, 2, NaN, 1, 5)
#' )
#' df$with_columns(
#'   a_null = pl$col("a")$is_null(),
#'   b_null = pl$col("b")$is_null()
#' )
expr__is_null <- function() {
  self$`_rexpr`$is_null() |>
    wrap()
}

#' Check if elements are not NULL
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   a = c(1, 2, NA, 1, 5),
#'   b = c(1, 2, NaN, 1, 5)
#' )
#' df$with_columns(
#'   a_not_null = pl$col("a")$is_not_null(),
#'   b_not_null = pl$col("b")$is_not_null()
#' )
expr__is_not_null <- function() {
  self$`_rexpr`$is_not_null() |>
    wrap()
}

#' Check if elements are infinite
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = c(1, 2), b = c(3, Inf))
#' df$with_columns(
#'   a_infinite = pl$col("a")$is_infinite(),
#'   b_infinite = pl$col("b")$is_infinite()
#' )
expr__is_infinite <- function() {
  self$`_rexpr`$is_infinite() |>
    wrap()
}

#' Check if elements are finite
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = c(1, 2), b = c(3, Inf))
#' df$with_columns(
#'   a_finite = pl$col("a")$is_finite(),
#'   b_finite = pl$col("b")$is_finite()
#' )
expr__is_finite <- function() {
  self$`_rexpr`$is_finite() |>
    wrap()
}

#' Check if elements are NaN
#'
#' Floating point `NaN` (Not A Number) should not be confused with missing data
#' represented as `NA` (in R) or `null` (in Polars).
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   a = c(1, 2, NA, 1, 5),
#'   b = c(1, 2, NaN, 1, 5)
#' )
#' df$with_columns(
#'   a_nan = pl$col("a")$is_nan(),
#'   b_nan = pl$col("b")$is_nan()
#' )
expr__is_nan <- function() {
  self$`_rexpr`$is_nan() |>
    wrap()
}

#' Check if elements are not NaN
#'
#' @inherit expr__is_nan description
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   a = c(1, 2, NA, 1, 5),
#'   b = c(1, 2, NaN, 1, 5)
#' )
#' df$with_columns(
#'   a_not_nan = pl$col("a")$is_not_nan(),
#'   b_not_nan = pl$col("b")$is_not_nan()
#' )
expr__is_not_nan <- function() {
  self$`_rexpr`$is_not_nan() |>
    wrap()
}

#' Get the minimum value
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(x = c(1, NaN, 3))$
#'   with_columns(min = pl$col("x")$min())
expr__min <- function() {
  self$`_rexpr`$min() |>
    wrap()
}

#' Get the maximum value
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(x = c(1, NaN, 3))$
#'   with_columns(max = pl$col("x")$max())
expr__max <- function() {
  self$`_rexpr`$max() |>
    wrap()
}

#' Get the maximum value with NaN
#'
#' This returns `NaN` if there are any.
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(x = c(1, NA, 3, NaN, Inf))$
#'   with_columns(nan_max = pl$col("x")$nan_max())
expr__nan_max <- function() {
  self$`_rexpr`$nan_max() |>
    wrap()
}

#' Get the minimum value with NaN
#'
#' This returns `NaN` if there are any.
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
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(x = c(1, 3, 4, NA))$
#'   with_columns(mean = pl$col("x")$mean())
expr__mean <- function() {
  self$`_rexpr`$mean() |>
    wrap()
}

#' Get median value
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(x = c(1, 3, 4, NA))$
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
#' @inheritParams rlang::check_dots_empty0
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
#' @inheritParams rlang::check_dots_empty0
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
#' @inheritParams expr__sort
#' @inherit as_polars_expr return
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

#' Get the first value
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
#' @inheritParams rlang::check_dots_empty0
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
# TODO: remove the noRd tag
#' @noRd
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
#' @inheritParams expr__add
#' @inherit as_polars_expr return
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
#' @inheritParams expr__add
#' @inherit as_polars_expr return
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
#' @inheritParams expr__add
#' @inherit as_polars_expr return
#' @examples
#' pl$lit(TRUE)$xor(pl$lit(FALSE))
expr__xor <- function(other) {
  wrap({
    other <- as_polars_expr(other, as_lit = TRUE)
    self$`_rexpr`$xor(other$`_rexpr`)
  })
}

#' Calculate the n-th discrete difference between elements
#'
#' @param n Integer indicating the number of slots to shift.
#' @param null_behavior How to handle null values. Must be `"ignore"` (default),
#' or `"drop"`.
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(20, 10, 30, 25, 35))$with_columns(
#'   diff_default = pl$col("a")$diff(),
#'   diff_2_ignore = pl$col("a")$diff(2, "ignore")
#' )
expr__diff <- function(n = 1, null_behavior = c("ignore", "drop")) {
  wrap({
    null_behavior <- arg_match0(null_behavior, c("ignore", "drop"))
    self$`_rexpr`$diff(n, null_behavior)
  })
}

#' Compute the dot/inner product between two Expressions
#'
#' @param other Expression to compute dot product with.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = c(1, 3, 5), b = c(2, 4, 6))
#' df$select(pl$col("a")$dot(pl$col("b")))
expr__dot <- function(expr) {
  self$`_rexpr`$dot(as_polars_expr(expr)$`_rexpr`) |>
    wrap()
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
#' @inherit as_polars_expr return
#'
#' @details
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

#' Check if any boolean value in a column is true
#'
#' @inheritParams expr__all
#' @inherit as_polars_expr return
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

#' Check if all boolean values in a column are true
#'
#' This method is an expression - not to be confused with [`pl$all()`][pl__all]
#' which is a function to select all columns.
#'
#' @inheritParams rlang::check_dots_empty0
#' @param ignore_nulls If `TRUE` (default), ignore null values. If `FALSE`,
#' [Kleene logic](https://en.wikipedia.org/wiki/Three-valued_logic) is used to
#' deal with nulls: if the column contains any null values and no `TRUE` values,
#' the output is null.
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

#' Return the cumulative sum computed at every element.
#'
#' @param reverse If `TRUE`, start with the total sum of elements and substract
#' each row one by one.
#'
#' @inherit as_polars_expr return
#' @details
#' The Dtypes Int8, UInt8, Int16 and UInt16 are cast to Int64 before summing to
#' prevent overflow issues.
#' @examples
#' pl$DataFrame(a = 1:4)$with_columns(
#'   cum_sum = pl$col("a")$cum_sum(),
#'   cum_sum_reversed = pl$col("a")$cum_sum(reverse = TRUE)
#' )
expr__cum_sum <- function(reverse = FALSE) {
  self$`_rexpr`$cum_sum(reverse) |>
    wrap()
}


#' Return the cumulative product computed at every element.
#'
#' @param reverse If `TRUE`, start with the total product of elements and divide
#' each row one by one.
#' @inherit expr__cum_sum return details
#' @examples
#' pl$DataFrame(a = 1:4)$with_columns(
#'   cum_prod = pl$col("a")$cum_prod(),
#'   cum_prod_reversed = pl$col("a")$cum_prod(reverse = TRUE)
#' )
expr__cum_prod <- function(reverse = FALSE) {
  self$`_rexpr`$cum_prod(reverse) |>
    wrap()
}

#' Return the cumulative min computed at every element.
#'
#' @param reverse If `TRUE`, start from the last value.
#' @inherit expr__cum_sum return details
#' @examples
#' pl$DataFrame(a = c(1:4, 2L))$with_columns(
#'   cum_min = pl$col("a")$cum_min(),
#'   cum_min_reversed = pl$col("a")$cum_min(reverse = TRUE)
#' )
expr__cum_min <- function(reverse = FALSE) {
  self$`_rexpr`$cum_min(reverse) |>
    wrap()
}

#' Return the cumulative max computed at every element.
#'
#' @inheritParams expr__cum_min
#' @inherit expr__cum_sum return details
#' @examples
#' pl$DataFrame(a = c(1:4, 2L))$with_columns(
#'   cum_max = pl$col("a")$cum_max(),
#'   cum_max_reversed = pl$col("a")$cum_max(reverse = TRUE)
#' )
expr__cum_max <- function(reverse = FALSE) {
  self$`_rexpr`$cum_max(reverse) |>
    wrap()
}

#' Return the cumulative count of the non-null values in the column
#'
#' @param reverse If `TRUE`, reverse the count.
#' @inherit as_polars_expr return
#'
#' @examples
#' pl$DataFrame(a = 1:4)$with_columns(
#'   cum_count = pl$col("a")$cum_count(),
#'   cum_count_reversed = pl$col("a")$cum_count(reverse = TRUE)
#' )
expr__cum_count <- function(reverse = FALSE) {
  self$`_rexpr`$cum_count(reverse) |>
    wrap()
}

#' Return the cumulative count of the non-null values in the column
#'
#' @param expr Expression to evaluate.
#' @inheritParams rlang::check_dots_empty0
#' @param min_periods Number of valid values (i.e. `length - null_count`) there
#' should be in the window before the expression is evaluated.
#' @param parallel Run in parallel. Don’t do this in a group by or another
#' operation that already has much parallelization.
#'
#' @details
#' This can be really slow as it can have `O(n^2)` complexity. Don’t use this
#' for operations that visit all elements.
#'
#' @inherit as_polars_expr return
#'
#' @examples
#' df <- pl$DataFrame(values = 1:5)
#' df$with_columns(
#'   pl$col("values")$cumulative_eval(
#'     pl$element()$first() - pl$element()$last()**2
#'   )
#' )
expr__cumulative_eval <- function(expr, ..., min_periods = 1, parallel = FALSE) {
  wrap({
    check_dots_empty0(...)
    self$`_rexpr`$cumulative_eval(
      as_polars_expr(expr)$`_rexpr`,
      min_periods,
      parallel
    )
  })
}

#' Get the group indexes of the group by operation
#'
#' Should be used in aggregation context only.
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   group = rep(c("one", "two"), each = 3),
#'   value = c(94, 95, 96, 97, 97, 99)
#' )
#'
#' df$group_by("group", maintain_order = TRUE)$agg(pl$col("value")$agg_groups())
expr__agg_groups <- function() {
  self$`_rexpr`$agg_groups() |>
    wrap()
}

#' Get the index of the maximal value
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = c(20, 10, 30))
#' df$select(pl$col("a")$arg_max())
expr__arg_max <- function() {
  self$`_rexpr`$arg_max() |>
    wrap()
}

#' Get the index of the minimal value
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = c(20, 10, 30))
#' df$select(pl$col("a")$arg_min())
expr__arg_min <- function() {
  self$`_rexpr`$arg_min() |>
    wrap()
}

#' Get the index of the first unique value
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = 1:3, b = c(NA, 4, 4))
#' df$select(pl$col("a")$arg_unique())
#' df$select(pl$col("b")$arg_unique())
expr__arg_unique <- function() {
  self$`_rexpr`$arg_unique() |>
    wrap()
}

#' Get the number of non-null elements in the column
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = 1:3, b = c(NA, 4, 4))
#' df$select(pl$all()$count())
expr__count <- function() {
  self$`_rexpr`$count() |>
    wrap()
}

#' Aggregate values into a list
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = 1:3, b = 4:6)
#' df$with_columns(pl$col("a")$implode())
expr__implode <- function() {
  self$`_rexpr`$implode() |>
    wrap()
}

#' Return the number of elements in the column
#'
#' Null values are counted in the total.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = 1:3, b = c(NA, 4, 4))
#' df$select(pl$all()$len())
expr__len <- function() {
  self$`_rexpr`$len() |>
    wrap()
}

#' Compute the product of an expression.
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = 1:3, b = c(NA, 4, 4))$
#'   select(pl$all()$product())
expr__product <- function() {
  self$`_rexpr`$product() |>
    wrap()
}

#' Get quantile value(s)
#'
#' @param quantile Quantile between 0.0 and 1.0.
#' @param interpolation Interpolation method. Must be one of `"nearest"`,
#' `"higher"`, `"lower"`, `"midpoint"`, `"linear"`.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = 0:5)
#' df$select(pl$col("a")$quantile(0.3))
#' df$select(pl$col("a")$quantile(0.3, interpolation = "higher"))
#' df$select(pl$col("a")$quantile(0.3, interpolation = "lower"))
#' df$select(pl$col("a")$quantile(0.3, interpolation = "midpoint"))
#' df$select(pl$col("a")$quantile(0.3, interpolation = "linear"))
expr__quantile <- function(
    quantile,
    interpolation = c("nearest", "higher", "lower", "midpoint", "linear")) {
  wrap({
    interpolation <- arg_match0(
      interpolation,
      values = c("nearest", "higher", "lower", "midpoint", "linear")
    )
    self$`_rexpr`$quantile(as_polars_expr(quantile, as_lit = TRUE)$`_rexpr`, interpolation)
  })
}

#' Compute the standard deviation
#'
#' @inheritParams DataFrame_var
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(1, 3, 5, 6))$
#'   select(pl$all()$std())
expr__std <- function(ddof = 1) {
  self$`_rexpr`$std(ddof) |>
    wrap()
}

#' Compute the variance
#'
#' @inheritParams DataFrame_var
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(1, 3, 5, 6))$
#'   select(pl$all()$var())
expr__var <- function(ddof = 1) {
  self$`_rexpr`$var(ddof) |>
    wrap()
}

#' Check whether the expression contains one or more null values
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   a = c(NA, 1, NA),
#'   b = c(10, NA, 300),
#'   c = c(350, 650, 850)
#' )
#' df$select(pl$all()$has_nulls())
expr__has_nulls <- function() {
  self$null_count() > 0 |>
    wrap()
}

#' Check if an expression is between the given lower and upper bounds
#'
#' @param lower_bound Lower bound value. Accepts expression input. Strings are
#'  parsed as column names, other non-expression inputs are parsed as literals.
#' @param upper_bound Upper bound value. Accepts expression input. Strings are
#' parsed as column names, other non-expression inputs are parsed as literals.
#' @param closed Define which sides of the interval are closed (inclusive). Must
#' be one of `"left"`, `"right"`, `"both"` or `"none"`.
#'
#' @details
#' If the value of the `lower_bound` is greater than that of the `upper_bound`
#' then the result will be `FALSE`, as no value can satisfy the condition.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(num = 1:5)
#' df$with_columns(
#'   is_between = pl$col("num")$is_between(2, 4)
#' )
#'
#' # Use the closed argument to include or exclude the values at the bounds:
#' df$with_columns(
#'   is_between = pl$col("num")$is_between(2, 4, closed = "left")
#' )
#'
#' # You can also use strings as well as numeric/temporal values (note: ensure
#' # that string literals are wrapped with lit so as not to conflate them with
#' # column names):
#' df <- pl$DataFrame(a = letters[1:5])
#' df$with_columns(
#'   is_between = pl$col("a")$is_between(pl$lit("a"), pl$lit("c"))
#' )
#'
#' # Use column expressions as lower/upper bounds, comparing to a literal value:
#' df <- pl$DataFrame(a = 1:5, b = 5:1)
#' df$with_columns(
#'   between_ab = pl$lit(3)$is_between(pl$col("a"), pl$col("b"))
#' )
expr__is_between <- function(
    lower_bound,
    upper_bound,
    closed = c("both", "left", "right", "none")) {
  wrap({
    closed <- arg_match0(closed, values = c("both", "left", "right", "none"))
    self$`_rexpr`$is_between(
      as_polars_expr(lower_bound)$`_rexpr`,
      as_polars_expr(upper_bound)$`_rexpr`,
      closed
    )
  })
}

#' Return a boolean mask indicating duplicated values
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = c(1, 1, 2, 3, 2))
#' df$select(pl$col("a")$is_duplicated())
expr__is_duplicated <- function() {
  self$`_rexpr`$is_duplicated() |>
    wrap()
}

#' Return a boolean mask indicating the first occurrence of each distinct value
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = c(1, 1, 2, 3, 2))
#' df$with_columns(
#'   is_first_distinct = pl$col("a")$is_first_distinct()
#' )
expr__is_first_distinct <- function() {
  self$`_rexpr`$is_first_distinct() |>
    wrap()
}

#' Return a boolean mask indicating the last occurrence of each distinct value
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = c(1, 1, 2, 3, 2))
#' df$with_columns(
#'   is_last_distinct = pl$col("a")$is_last_distinct()
#' )
expr__is_last_distinct <- function() {
  self$`_rexpr`$is_last_distinct() |>
    wrap()
}

#' Check if elements of an expression are present in another expression
#'
#' @param other Series or sequence of primitive type.
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(
#'   sets = list(1:3, 1:2, 9:10),
#'   optional_members = 1:3
#' )
#' df$with_columns(
#'   contains = pl$col("optional_members")$is_in("sets")
#' )
expr__is_in <- function(other) {
  self$`_rexpr`$is_in(as_polars_expr(other)$`_rexpr`) |>
    wrap()
}

#' Return a boolean mask indicating unique values
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = c(1, 1, 2, 3, 2))
#' df$select(pl$col("a")$is_unique())
expr__is_unique <- function() {
  self$`_rexpr`$is_unique() |>
    wrap()
}

#' Compute absolute values
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = -1:2)
#' df$with_columns(abs = pl$col("a")$abs())
expr__abs <- function() {
  self$`_rexpr`$abs() |>
    wrap()
}

#' Approximate count of unique values
#'
#' This is done using the HyperLogLog++ algorithm for cardinality estimation.
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(n = c(1, 1, 2))
#' df$select(pl$col("n")$approx_n_unique())
#'
#' df <- pl$DataFrame(n = 0:1000)
#' df$select(
#'   exact = pl$col("n")$n_unique(),
#'   approx = pl$col("n")$approx_n_unique()
#' )
expr__approx_n_unique <- function() {
  self$`_rexpr`$approx_n_unique() |>
    wrap()
}

#' Compute sine
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(0, pi / 2, pi, NA))$
#'   with_columns(sine = pl$col("a")$sin())
expr__sin <- function() {
  self$`_rexpr`$sin() |>
    wrap()
}

#' Compute cosine
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(0, pi / 2, pi, NA))$
#'   with_columns(cosine = pl$col("a")$cos())
expr__cos <- function() {
  self$`_rexpr`$cos() |>
    wrap()
}

#' Compute cotangent
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(0, pi / 2, -5, NA))$
#'   with_columns(cotangent = pl$col("a")$cot())
expr__cot <- function() {
  self$`_rexpr`$cot() |>
    wrap()
}

#' Compute tangent
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(0, pi / 2, pi, NA))$
#'   with_columns(tangent = pl$col("a")$tan())
expr__tan <- function() {
  self$`_rexpr`$tan() |>
    wrap()
}

#' Compute inverse sine
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(-1, sin(0.5), 0, 1, NA))$
#'   with_columns(arcsin = pl$col("a")$arcsin())
expr__arcsin <- function() {
  self$`_rexpr`$arcsin() |>
    wrap()
}

#' Compute inverse cosine
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(-1, cos(0.5), 0, 1, NA))$
#'   with_columns(arccos = pl$col("a")$arccos())
expr__arccos <- function() {
  self$`_rexpr`$arccos() |>
    wrap()
}

#' Compute inverse tangent
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(-1, tan(0.5), 0, 1, NA_real_))$
#'   with_columns(arctan = pl$col("a")$arctan())
expr__arctan <- function() {
  self$`_rexpr`$arctan() |>
    wrap()
}

#' Compute hyperbolic sine
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(-1, asinh(0.5), 0, 1, NA))$
#'   with_columns(sinh = pl$col("a")$sinh())
expr__sinh <- function() {
  self$`_rexpr`$sinh() |>
    wrap()
}

#' Compute hyperbolic cosine
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(-1, acosh(2), 0, 1, NA))$
#'   with_columns(cosh = pl$col("a")$cosh())
expr__cosh <- function() {
  self$`_rexpr`$cosh() |>
    wrap()
}

#' Compute hyperbolic tangent
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(-1, atanh(0.5), 0, 1, NA))$
#'   with_columns(tanh = pl$col("a")$tanh())
expr__tanh <- function() {
  self$`_rexpr`$tanh() |>
    wrap()
}

#' Compute inverse hyperbolic sine
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(-1, sinh(0.5), 0, 1, NA))$
#'   with_columns(arcsinh = pl$col("a")$arcsinh())
expr__arcsinh <- function() {
  self$`_rexpr`$arcsinh() |>
    wrap()
}

#' Compute inverse hyperbolic cosine
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(-1, cosh(0.5), 0, 1, NA))$
#'   with_columns(arccosh = pl$col("a")$arccosh())
expr__arccosh <- function() {
  self$`_rexpr`$arccosh() |>
    wrap()
}

#' Compute inverse hyperbolic tangent
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(-1, tanh(0.5), 0, 1, NA))$
#'   with_columns(arctanh = pl$col("a")$arctanh())
expr__arctanh <- function() {
  self$`_rexpr`$arctanh() |>
    wrap()
}

#' Compute cube root
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(1, 2, 4))$
#'   with_columns(cbrt = pl$col("a")$cbrt())
expr__cbrt <- function() {
  self$`_rexpr`$cbrt() |>
    wrap()
}

#' Convert from radians to degrees
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(1, 2, 4) * pi)$
#'   with_columns(degrees = pl$col("a")$degrees())
expr__degrees <- function() {
  self$`_rexpr`$degrees() |>
    wrap()
}

#' Compute entropy
#'
#' Uses the formula `-sum(pk * log(pk)` where `pk` are discrete probabilities.
#'
#' @param base Numeric value used as base, defaults to `exp(1)`.
#' @inheritParams rlang::check_dots_empty0
#' @param normalize Normalize `pk` if it doesn’t sum to 1.
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = 1:3)
#' df$select(pl$col("a")$entropy(base = 2))
#' df$select(pl$col("a")$entropy(base = 2, normalize = FALSE))
expr__entropy <- function(base = exp(1), ..., normalize = TRUE) {
  wrap({
    check_dots_empty0(...)
    self$`_rexpr`$entropy(base, normalize)
  })
}

#' Compute the exponential
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(1, 2, 4))$
#'   with_columns(exp = pl$col("a")$exp())
expr__exp <- function() {
  self$`_rexpr`$exp() |>
    wrap()
}

#' Compute the logarithm
#'
#' @inheritParams expr__entropy
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(1, 2, 4))$
#'   with_columns(
#'   log = pl$col("a")$log(),
#'   log_base_2 = pl$col("a")$log(base = 2)
#' )
expr__log <- function(base = exp(1)) {
  self$`_rexpr`$log(base) |>
    wrap()
}

#' Compute the base-10 logarithm
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(1, 2, 4))$
#'   with_columns(log10 = pl$col("a")$log10())
expr__log10 <- function() {
  self$log(10)
}

#' Compute the natural logarithm plus one
#'
#' This computes `log(1 + x)` but is more numerically stable for `x` close to
#' zero.
#'
#' @inherit as_polars_expr return
#' @examples
#' pl$DataFrame(a = c(1, 2, 4))$
#'   with_columns(log1p = pl$col("a")$log1p())
expr__log1p <- function() {
  self$`_rexpr`$log1p() |>
    wrap()
}

#' Hash elements
#'
#' @param seed Integer, random seed parameter. Defaults to 0.
#' @param seed_1,seed_2,seed_3 Integer, random seed parameters. Default to
#' `seed` if not set.
#' @inherit as_polars_expr return
#'
#' @details
#' This implementation of hash does not guarantee stable results across
#' different Polars versions. Its stability is only guaranteed within a single
#' version.
#'
#' @examples
#' df <- pl$DataFrame(a = c(1, 2, NA), b = c("x", NA, "z"))
#' df$with_columns(pl$all()$hash(10, 20, 30, 40))
expr__hash <- function(seed = 0, seed_1 = NULL, seed_2 = NULL, seed_3 = NULL) {
  self$`_rexpr`$hash(seed, seed_1, seed_2, seed_3) |>
    wrap()
}

#' Compute the most occurring value(s)
#'
#' @inherit as_polars_expr return
#' @examples
#' df <- pl$DataFrame(a = c(1, 1, 2, 3), b = c(1, 1, 2, 2))
#' df$select(pl$col("a")$mode())
#' df$select(pl$col("b")$mode())
expr__mode <- function() {
  self$`_rexpr`$mode() |>
    wrap()
}

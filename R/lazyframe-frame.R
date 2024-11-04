# TODO: link to dataframe__lazy
#' Polars LazyFrame class (`polars_lazy_frame`)
#'
#' Representation of a Lazy computation graph/query against a [DataFrame].
#' This allows for whole-query optimisation in addition to parallelism,
#' and is the preferred (and highest-performance) mode of operation for polars.
#'
#' The `pl$LazyFrame(...)` function is a shortcut for `pl$DataFrame(...)$lazy()`.
#' @aliases polars_lazy_frame LazyFrame
#' @inheritParams pl__DataFrame
#' @return A polars [LazyFrame]
#' @seealso
#' - [`<LazyFrame>$collect()`][lazyframe__collect]: Materialize a [LazyFrame] into a [DataFrame].
#' @examples
#' # Constructing a LazyFrame from vectors:
#' pl$LazyFrame(a = 1:2, b = 3:4)
#'
#' # Constructing a LazyFrame from Series:
#' pl$LazyFrame(pl$Series("a", 1:2), pl$Series("b", 3:4))
#'
#' # Constructing a LazyFrame from a list:
#' data <- list(a = 1:2, b = 3:4)
#'
#' ## Using dynamic dots feature
#' pl$LazyFrame(!!!data)
pl__LazyFrame <- function(..., .schema_overrides = NULL, .strict = TRUE) {
  pl$DataFrame(..., .schema_overrides = .schema_overrides, .strict = .strict)$lazy()
}

# The env for storing lazyrame methods
polars_lazyframe__methods <- new.env(parent = emptyenv())

#' @export
wrap.PlRLazyFrame <- function(x, ...) {
  self <- new.env(parent = emptyenv())
  self$`_ldf` <- x

  lapply(names(polars_lazyframe__methods), function(name) {
    fn <- polars_lazyframe__methods[[name]]
    environment(fn) <- environment()
    assign(name, fn, envir = self)
  })

  class(self) <- c("polars_lazy_frame", "polars_object")
  self
}

# TODO: link to pl__select
#' Select and modify columns of a LazyFrame
#'
#' @description
#' Select and perform operations on a subset of columns only. This discards
#' unmentioned columns (like `.()` in `data.table` and contrarily to
#' `dplyr::mutate()`).
#'
#' One cannot use new variables in subsequent expressions in the same
#' `$select()` call. For instance, if you create a variable `x`, you will only
#' be able to use it in another `$select()` or `$with_columns()` call.
#'
#' @inherit pl__LazyFrame return
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]>
#' Name-value pairs of objects to be converted to polars [expressions][Expr]
#' by the [as_polars_expr()] function.
#' Characters are parsed as column names, other non-expression inputs are parsed as [literals][pl__lit].
#' Each name will be used as the expression name.
#' @examples
#' # Pass the name of a column to select that column.
#' lf <- pl$LazyFrame(
#'   foo = 1:3,
#'   bar = 6:8,
#'   ham = letters[1:3]
#' )
#' lf$select("foo")$collect()
#'
#' # Multiple columns can be selected by passing a list of column names.
#' lf$select("foo", "bar")$collect()
#'
#' # Expressions are also accepted.
#' lf$select(pl$col("foo"), pl$col("bar") + 1)$collect()
#'
#' # Name expression (used as the column name of the output DataFrame)
#' lf$select(
#'   threshold = pl$when(pl$col("foo") > 2)$then(10)$otherwise(0)
#' )$collect()
#'
#' # Expressions with multiple outputs can be automatically instantiated
#' # as Structs by setting the `POLARS_AUTO_STRUCTIFY` environment variable.
#' # (Experimental)
#' if (requireNamespace("withr", quietly = TRUE)) {
#'   withr::with_envvar(c(POLARS_AUTO_STRUCTIFY = "1"), {
#'     lf$select(
#'       is_odd = ((pl$col(pl$Int32) %% 2) == 1)$name$suffix("_is_odd"),
#'     )$collect()
#'   })
#' }
lazyframe__select <- function(...) {
  wrap({
    structify <- parse_env_auto_structify()

    parse_into_list_of_expressions(..., `__structify` = structify) |>
      self$`_ldf`$select()
  })
}

lazyframe__group_by <- function(..., .maintain_order = FALSE) {
  wrap({
    exprs <- parse_into_list_of_expressions(...)
    self$`_ldf`$group_by(exprs, .maintain_order)
  })
}

# TODO: see also section
#' Materialize this LazyFrame into a DataFrame
#'
#' By default, all query optimizations are enabled.
#' Individual optimizations may be disabled by setting the corresponding parameter to `FALSE`.
#' @inherit pl__DataFrame return
#' @inheritParams rlang::args_dots_empty
#' @param type_coercion A logical, indicats type coercion optimization.
#' @param predicate_pushdown A logical, indicats predicate pushdown optimization.
#' @param projection_pushdown A logical, indicats projection pushdown optimization.
#' @param simplify_expression A logical, indicats simplify expression optimization.
#' @param slice_pushdown A logical, indicats slice pushdown optimization.
#' @param comm_subplan_elim A logical, indicats tring to cache branching subplans that occur on self-joins or unions.
#' @param comm_subexpr_elim A logical, indicats tring to cache common subexpressions.
#' @param cluster_with_columns A logical, indicats to combine sequential independent calls to with_columns.
#' @param no_optimization A logical. If `TRUE`, turn off (certain) optimizations.
#' @param streaming A logical. If `TRUE`, process the query in batches to handle larger-than-memory data.
#' If `FALSE` (default), the entire query is processed in a single batch.
#' Note that streaming mode is considered unstable.
#' It may be changed at any point without it being considered a breaking change.
#' @param _eager A logical, indicates to turn off multi-node optimizations and the other optimizations.
#' This option is intended for internal use only.
#' @examples
#' lf <- pl$LazyFrame(
#'   a = c("a", "b", "a", "b", "b", "c"),
#'   b = 1:6,
#'   c = 6:1,
#' )
#' lf$group_by("a")$agg(pl$all()$sum())$collect()
#'
#' # Collect in streaming mode
#' lf$group_by("a")$agg(pl$all()$sum())$collect(
#'   streaming = TRUE
#' )
lazyframe__collect <- function(
    ...,
    type_coercion = TRUE,
    predicate_pushdown = TRUE,
    projection_pushdown = TRUE,
    simplify_expression = TRUE,
    slice_pushdown = TRUE,
    comm_subplan_elim = TRUE,
    comm_subexpr_elim = TRUE,
    cluster_with_columns = TRUE,
    no_optimization = FALSE,
    streaming = FALSE,
    `_eager` = FALSE) {
  wrap({
    check_dots_empty0(...)

    if (isTRUE(no_optimization) || isTRUE(`_eager`)) {
      predicate_pushdown <- FALSE
      projection_pushdown <- FALSE
      slice_pushdown <- FALSE
      comm_subplan_elim <- FALSE
      comm_subexpr_elim <- FALSE
      cluster_with_columns <- FALSE
    }

    ldf <- self$`_ldf`$optimization_toggle(
      type_coercion = type_coercion,
      predicate_pushdown = predicate_pushdown,
      projection_pushdown = projection_pushdown,
      simplify_expression = simplify_expression,
      slice_pushdown = slice_pushdown,
      comm_subplan_elim = comm_subplan_elim,
      comm_subexpr_elim = comm_subexpr_elim,
      cluster_with_columns = cluster_with_columns,
      streaming = streaming,
      `_eager` = `_eager`
    )

    ldf$collect()
  })
}

lazyframe__explain <- function(
    ...,
    format = c("plain", "tree"),
    optimized = TRUE,
    type_coercion = TRUE,
    predicate_pushdown = TRUE,
    projection_pushdown = TRUE,
    simplify_expression = TRUE,
    slice_pushdown = TRUE,
    comm_subplan_elim = TRUE,
    comm_subexpr_elim = TRUE,
    cluster_with_columns = TRUE,
    streaming = FALSE) {
  wrap({
    check_dots_empty0(...)

    format <- arg_match0(format, c("plain", "tree"))

    if (isTRUE(optimized)) {
      ldf <- self$`_ldf`$optimization_toggle(
        type_coercion = type_coercion,
        predicate_pushdown = predicate_pushdown,
        projection_pushdown = projection_pushdown,
        simplify_expression = simplify_expression,
        slice_pushdown = slice_pushdown,
        comm_subplan_elim = comm_subplan_elim,
        comm_subexpr_elim = comm_subexpr_elim,
        cluster_with_columns = cluster_with_columns,
        streaming = streaming,
        `_eager` = FALSE
      )

      if (format == "tree") {
        ldf$describe_optimized_plan_tree()
      } else {
        ldf$describe_optimized_plan()
      }
    } else {
      if (format == "tree") {
        self$`_ldf`$describe_plan_tree()
      } else {
        self$`_ldf`$describe_plan()
      }
    }
  })
}

lazyframe__cast <- function(..., .strict = TRUE) {
  wrap({
    check_bool(.strict)
    dtypes <- parse_into_list_of_datatypes(...)

    if (length(dtypes) == 1L && !is_named(dtypes)) {
      self$`_ldf`$cast_all(dtypes[[1]], strict = .strict)
    } else {
      self$`_ldf`$cast(dtypes, strict = .strict)
    }
  })
}

lazyframe__filter <- function(...) {
  parse_predicates_constraints_into_expression(...) |>
    self$`_ldf`$filter() |>
    wrap()
}

lazyframe__sort <- function(
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

    self$`_ldf`$sort_by_exprs(
      by,
      descending = descending,
      nulls_last = nulls_last,
      multithreaded = multithreaded,
      maintain_order = maintain_order
    )
  })
}

#' Modify/append column(s) of a LazyFrame
#'
#' @description
#' Add columns or modify existing ones with expressions. This is similar to
#' `dplyr::mutate()` as it keeps unmentioned columns (unlike `$select()`).
#'
#' However, unlike `dplyr::mutate()`, one cannot use new variables in subsequent
#' expressions in the same `$with_columns()`call. For instance, if you create a
#' variable `x`, you will only be able to use it in another `$with_columns()`
#' or `$select()` call.
#'
#' @inherit pl__LazyFrame return
#' @inheritParams lazyframe__select
#' @examples
#' # Pass an expression to add it as a new column.
#' lf <- pl$LazyFrame(
#'   a = 1:4,
#'   b = c(0.5, 4, 10, 13),
#'   c = c(TRUE, TRUE, FALSE, TRUE),
#' )
#' lf$with_columns((pl$col("a")^2)$alias("a^2"))$collect()
#'
#' # Added columns will replace existing columns with the same name.
#' lf$with_columns(a = pl$col("a")$cast(pl$Float64))$collect()
#'
#' # Multiple columns can be added
#' lf$with_columns(
#'   (pl$col("a")^2)$alias("a^2"),
#'   (pl$col("b") / 2)$alias("b/2"),
#'   (pl$col("c")$not())$alias("not c"),
#' )$collect()
#'
#' # Name expression instead of `$alias()`
#' lf$with_columns(
#'   `a^2` = pl$col("a")^2,
#'   `b/2` = pl$col("b") / 2,
#'   `not c` = pl$col("c")$not(),
#' )$collect()
#'
#' # Expressions with multiple outputs can automatically be instantiated
#' # as Structs by enabling the experimental setting `POLARS_AUTO_STRUCTIFY`:
#' if (requireNamespace("withr", quietly = TRUE)) {
#'   withr::with_envvar(c(POLARS_AUTO_STRUCTIFY = "1"), {
#'     lf$drop("c")$with_columns(
#'       diffs = pl$col("a", "b")$diff()$name$suffix("_diff"),
#'     )$collect()
#'   })
#' }
lazyframe__with_columns <- function(...) {
  wrap({
    structify <- parse_env_auto_structify()

    parse_into_list_of_expressions(..., `__structify` = structify) |>
      self$`_ldf`$with_columns()
  })
}

lazyframe__drop <- function(..., strict = TRUE) {
  wrap({
    check_dots_unnamed()

    parse_into_list_of_expressions(...) |>
      self$`_ldf`$drop(strict)
  })
}

lazyframe__slice <- function(offset, length = NULL) {
  wrap({
    if (isTRUE(length < 0)) {
      abort(sprintf("negative slice length (%s) are invalid for LazyFrame", length))
    }
    self$`_ldf`$slice(offset, length)
  })
}

lazyframe__head <- function(n = 5) {
  self$slice(0, n) |>
    wrap()
}

lazyframe__tail <- function(n = 5) {
  self$`_ldf`$tail(n) |>
    wrap()
}


#' Get the first row of a LazyFrame
#'
#' @inherit as_polars_lf return
#' @examples
#' as_polars_lf(mtcars)$first()$collect()
lazyframe__first <- function() {
  wrap({
    self$`_rexpr`$first()
  })
}

#' Get the last row of a LazyFrame
#' @description Aggregate the columns in the LazyFrame to their maximum value.
#'
#' @inherit as_polars_lf return
#' @examples
#' as_polars_lf(mtcars)$last()$collect()
lazyframe__last <- function() {
  wrap({
    self$`_rexpr`$last()
  })
}

#' Max
#' @description Aggregate the columns in the LazyFrame to their maximum value.
#'
#' @inherit as_polars_lf return
#' @examples
#' as_polars_lf(mtcars)$max()$collect()
lazyframe__max <- function() {
  wrap({
    self$`_rexpr`$max()
  })
}

#' Mean
#' @description Aggregate the columns in the LazyFrame to their mean value.
#'
#' @inherit as_polars_lf return
#' @examples
#' as_polars_lf(mtcars)$mean()$collect()
lazyframe__mean <- function() {
  wrap({
    self$`_rexpr`$mean()
  })
}

#' Median
#' @description Aggregate the columns in the LazyFrame to their median value.
#'
#' @inherit as_polars_lf return
#' @examples
#' as_polars_lf(mtcars)$median()$collect()
lazyframe__median <- function() {
  wrap({
    self$`_rexpr`$median()
  })
}

#' Min
#' @description Aggregate the columns in the LazyFrame to their minimum value.
#'
#' @inherit as_polars_lf return
#' @examples
#' as_polars_lf(mtcars)$min()$collect()
lazyframe__min <- function() {
  wrap({
    self$`_rexpr`$min()
  })
}

#' Sum
#' @description Aggregate the columns of this LazyFrame to their sum values.
#'
#' @inherit as_polars_lf return
#' @examples
#' as_polars_lf(mtcars)$sum()$collect()
lazyframe__sum <- function() {
  wrap({
    self$`_rexpr`$sum()
  })
}

#' Var
#' @description Aggregate the columns of this LazyFrame to their variance values.
#'
#' @inheritParams DataFrame_var
#' @inherit as_polars_lf return
#' @examples
#' as_polars_lf(mtcars)$var()$collect()
lazyframe__var <- function(ddof = 1) {
  wrap({
    self$`_rexpr`$var(ddof)
  })
}

#' Std
#' @description Aggregate the columns of this LazyFrame to their standard
#' deviation values.
#'
#' @inheritParams DataFrame_std
#' @inherit as_polars_lf return
#' @examples
#' as_polars_lf(mtcars)$std()$collect()
lazyframe__std <- function(ddof = 1) {
  wrap({
    self$`_rexpr`$std(ddof)
  })
}

#' Quantile
#' @description Aggregate the columns in the DataFrame to a unique quantile
#' value. Use `$describe()` to specify several quantiles.
#' @inheritParams DataFrame_quantile
#' @inherit as_polars_lf return
#' @examples
#' as_polars_lf(mtcars)$quantile(.4)$collect()
lazyframe__quantile <- function(quantile, interpolation = "nearest") {
  wrap({
    self$`_rexpr`$quantile(wrap_e_result(quantile), interpolation)
  })
}

#' @inherit Expr_fill_nan title params
#'
#' @inherit as_polars_lf return
#' @examples
#' df <- pl$LazyFrame(
#'   a = c(1.5, 2, NaN, 4),
#'   b = c(1.5, NaN, NaN, 4)
#' )
#' df$fill_nan(99)$collect()
lazyframe__fill_nan <- function(value) {
  wrap({
    self$`_rexpr`$fill_nan(value)
  })
}

#' @inherit DataFrame_fill_null title description params
#'
#' @inherit as_polars_lf return
#' @examples
#' df <- pl$LazyFrame(
#'   a = c(1.5, 2, NA, 4),
#'   b = c(1.5, NA, NA, 4)
#' )
#' df$fill_null(99)$collect()
lazyframe__fill_null <- function(fill_value) {
  wrap({
    self$`_rexpr`$fill_null(wrap_e_result(fill_value))
  })
}

#' Shift a LazyFrame
#'
#' @inherit DataFrame_shift description params
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(a = 1:4, b = 5:8)
#'
#' lf$shift(2)$collect()
#'
#' lf$shift(-2)$collect()
#'
#' lf$shift(-2, fill_value = 100)$collect()
lazyframe__shift <- function(n = 1, fill_value = NULL) {
  self$`_rexpr`$shift(n, fill_value)
}

#' Drop columns of a LazyFrame
#'
#' @inheritParams DataFrame_drop
#'
#' @inherit as_polars_lf return
#' @examples
#' as_polars_lf(mtcars)$drop(c("mpg", "hp"))$collect()
#'
#' # equivalent
#' as_polars_lf(mtcars)$drop("mpg", "hp")$collect()
lazyframe__drop <- function(..., strict = TRUE) {
  cols <- unpack_list(..., .context = "in $drop():") |>
    unlist()
  if (length(cols) == 0) {
    return(self)
  }
  self$`_rexpr`$drop(cols, strict)
}

#' Reverse
#' @description Reverse the LazyFrame (the last row becomes the first one, etc.).
#'
#' @inherit as_polars_lf return
#' @examples
#' as_polars_lf(mtcars)$reverse()$collect()
lazyframe__reverse <- function() {
  wrap({
    self$`_rexpr`$reverse()
  })
}

#' Slice
#' @description Get a slice of the LazyFrame.
#' @inheritParams DataFrame_slice
#' @return A [LazyFrame][lazyframe__class]
#' @examples
#' as_polars_lf(mtcars)$slice(2, 4)$collect()
#' as_polars_lf(mtcars)$slice(30)$collect()
#' mtcars[2:6, ]
lazyframe__slice <- function(offset, length = NULL) {
  wrap({
    self$`_rexpr`$slice(offset, length)
  })
}

#' Get the last `n` rows.
#'
#' @inherit lazyframe__head return params
#' @inheritParams lazyframe__head
#' @seealso [`<LazyFrame>$head()`][lazyframe__head]
#' @examples
#' lf <- pl$LazyFrame(a = 1:6, b = 7:12)
#'
#' lf$tail()$collect()
#'
#' lf$tail(2)$collect()
lazyframe__tail <- function(n = 5L) {
  wrap({
    self$`_rexpr`$tail(n)
  })
}

#' @inherit DataFrame_drop_nulls title description params
#'
#' @inherit as_polars_lf return
#' @examples
#' tmp <- mtcars
#' tmp[1:3, "mpg"] <- NA
#' tmp[4, "hp"] <- NA
#' tmp <- pl$LazyFrame(tmp)
#'
#' # number of rows in `tmp` before dropping nulls
#' tmp$collect()$height
#'
#' tmp$drop_nulls()$collect()$height
#' tmp$drop_nulls("mpg")$collect()$height
#' tmp$drop_nulls(c("mpg", "hp"))$collect()$height
lazyframe__drop_nulls <- function(subset = NULL) {
  if (!is.null(subset)) subset <- as.list(subset)
  self$`_rexpr`$drop_nulls(subset)
}

#' @inherit DataFrame_unique title description params
#'
#' @inherit as_polars_lf return
#' @examples
#' df <- pl$LazyFrame(
#'   x = sample(10, 100, rep = TRUE),
#'   y = sample(10, 100, rep = TRUE)
#' )
#' df$collect()$height
#'
#' df$unique()$collect()$height
#' df$unique(subset = "x")$collect()$height
#'
#' df$unique(keep = "last")
#'
#' # only keep unique rows
#' df$unique(keep = "none")
lazyframe__unique <- function(
    subset = NULL,
    ...,
    keep = "any",
    maintain_order = FALSE) {
  wrap({
    self$`_rexpr`$unique(subset, keep, maintain_order)
  })
}

#' Group a LazyFrame
#' @description This doesn't modify the data but only stores information about
#' the group structure. This structure can then be used by several functions
#' (`$agg()`, `$filter()`, etc.).
#'
#' @param ... Column(s) to group by.
#' Accepts [expression][Expr_class] input. Characters are parsed as column names.
#' @param maintain_order Ensure that the order of the groups is consistent with the input data.
#' This is slower than a default group by.
#' Setting this to `TRUE` blocks the possibility to run on the streaming engine.
#' The default value can be changed with `options(polars.maintain_order = TRUE)`.
#' @return [LazyGroupBy][LazyGroupBy_class] (a LazyFrame with special groupby methods like `$agg()`)
#' @examples
#' lf <- pl$LazyFrame(
#'   a = c("a", "b", "a", "b", "c"),
#'   b = c(1, 2, 1, 3, 3),
#'   c = c(5, 4, 3, 2, 1)
#' )
#'
#' lf$group_by("a")$agg(pl$col("b")$sum())$collect()
#'
#' # Set `maintain_order = TRUE` to ensure the order of the groups is consistent with the input.
#' lf$group_by("a", maintain_order = TRUE)$agg(pl$col("c"))$collect()
#'
#' # Group by multiple columns by passing a list of column names.
#' lf$group_by(c("a", "b"))$agg(pl$max("c"))$collect()
#'
#' # Or pass some arguments to group by multiple columns in the same way.
#' # Expressions are also accepted.
#' lf$group_by("a", pl$col("b") %/% 2)$agg(
#'   pl$col("c")$mean()
#' )$collect()
#'
#' # The columns will be renamed to the argument names.
#' lf$group_by(d = "a", e = pl$col("b") %/% 2)$agg(
#'   pl$col("c")$mean()
#' )$collect()
lazyframe__group_by <- function(..., maintain_order = polars_options()$maintain_order) {
  self$`_rexpr`$group_by(unpack_list(..., .context = "in $group_by():"), maintain_order)
}

#' Join LazyFrames
#'
#' This function can do both mutating joins (adding columns based on matching
#' observations, for example with `how = "left"`) and filtering joins (keeping
#' observations based on matching observations, for example with `how =
#' "inner"`).
#'
#' @param other LazyFrame to join with.
#' @param on Either a vector of column names or a list of expressions and/or
#'   strings. Use `left_on` and `right_on` if the column names to match on are
#'   different between the two DataFrames.
#' @param how One of the following methods: "inner", "left", "right", "full",
#'   "semi", "anti", "cross".
#' @param ... Ignored.
#' @param left_on,right_on Same as `on` but only for the left or the right
#'   DataFrame. They must have the same length.
#' @param suffix Suffix to add to duplicated column names.
#' @param validate Checks if join is of specified type:
#' * `"m:m"` (default): many-to-many, doesn't perform any checks;
#' * `"1:1"`: one-to-one, check if join keys are unique in both left and right
#'   datasets;
#' * `"1:m"`: one-to-many, check if join keys are unique in left dataset
#' * `"m:1"`: many-to-one, check if join keys are unique in right dataset
#'
#' Note that this is currently not supported by the streaming engine, and is
#' only supported when joining by single columns.
#'
#' @param join_nulls Join on null values. By default null values will never
#'   produce matches.
#' @param allow_parallel Allow the physical plan to optionally evaluate the
#'   computation of both DataFrames up to the join in parallel.
#' @param force_parallel Force the physical plan to evaluate the computation of
#'   both DataFrames up to the join in parallel.
#' @param coalesce Coalescing behavior (merging of join columns).
#' - `NULL`: join specific.
#' - `TRUE`: Always coalesce join columns.
#' - `FALSE`: Never coalesce join columns.
#'
#' @inherit as_polars_lf return
#' @examples
#' # inner join by default
#' df1 <- pl$LazyFrame(list(key = 1:3, payload = c("f", "i", NA)))
#' df2 <- pl$LazyFrame(list(key = c(3L, 4L, 5L, NA_integer_)))
#' df1$join(other = df2, on = "key")
#'
#' # cross join
#' df1 <- pl$LazyFrame(x = letters[1:3])
#' df2 <- pl$LazyFrame(y = 1:4)
#' df1$join(other = df2, how = "cross")
#'
#' # use "validate" to ensure join keys are not duplicated
#' df1 <- pl$LazyFrame(x = letters[1:5], y = 1:5)
#' df2 <- pl$LazyFrame(x = c("a", letters[1:4]), y2 = 6:10)
#'
#' # this throws an error because there are two keys in df2 that match the key
#' # in df1
#' tryCatch(
#'   df1$join(df2, on = "x", validate = "1:1")$collect(),
#'   error = function(e) print(e)
#' )
lazyframe__join <- function(
    other,
    on = NULL,
    how = "inner",
    ...,
    left_on = NULL,
    right_on = NULL,
    suffix = "_right",
    validate = "m:m",
    join_nulls = FALSE,
    allow_parallel = TRUE,
    force_parallel = FALSE,
    coalesce = NULL) {
  uw <- \(res) wrap({
    res
  })


  if (!is_polars_lf(other)) {
    Err_plain("`other` must be a LazyFrame.") |> uw()
  }

  if (how == "cross") {
    if (!is.null(on) || !is.null(left_on) || !is.null(right_on)) {
      Err_plain("cross join should not pass join keys.") |> uw()
    }
    rexprs_left <- as.list(NULL)
    rexprs_right <- as.list(NULL)
  } else {
    if (!is.null(on)) {
      rexprs_right <- rexprs_left <- as.list(on)
    } else if ((!is.null(left_on) && !is.null(right_on))) {
      rexprs_left <- as.list(left_on)
      rexprs_right <- as.list(right_on)
    } else {
      Err_plain("must specify either `on`, or `left_on` and `right_on`.") |> uw()
    }
  }

  self$`_rexpr`$join(
    lf, other, rexprs_left, rexprs_right, how, validate, join_nulls, suffix,
    allow_parallel, force_parallel, coalesce
  ) |>
    uw()
}

#' Perform a join based on one or multiple (in)equality predicates
#'
#' @description
#' This performs an inner join, so only rows where all predicates are true are
#' included in the result, and a row from either LazyFrame may be included
#' multiple times in the result.
#'
#' Note that the row order of the input LazyFrames is not preserved.
#'
#' @param other LazyFrame to join with.
#' @param ... (In)Equality condition to join the two tables on. When a column
#' name occurs in both tables, the proper suffix must be applied in the
#' predicate. For example, if both tables have a column `"x"` that you want to
#' use in the conditions, you must refer to the column of the right table as
#' `"x<suffix>"`.
#' @param suffix Suffix to append to columns with a duplicate name.
#'
#' @return A LazyFrame
#'
#' @examples
#' east <- pl$LazyFrame(
#'   id = c(100, 101, 102),
#'   dur = c(120, 140, 160),
#'   rev = c(12, 14, 16),
#'   cores = c(2, 8, 4)
#' )
#'
#' west <- pl$LazyFrame(
#'   t_id = c(404, 498, 676, 742),
#'   time = c(90, 130, 150, 170),
#'   cost = c(9, 13, 15, 16),
#'   cores = c(4, 2, 1, 4)
#' )
#'
#' east$join_where(
#'   west,
#'   pl$col("dur") < pl$col("time"),
#'   pl$col("rev") < pl$col("cost")
#' )$collect()
lazyframe__join_where <- function(
    other,
    ...,
    suffix = "_right") {
  uw <- \(res) wrap({
    res
  })


  if (!is_polars_lf(other)) {
    Err_plain("`other` must be a LazyFrame.") |> uw()
  }

  self$`_rexpr`$join_where(lf, other, unpack_list(..., .context = "in $join_where():"), suffix) |>
    uw()
}



#' Sort the LazyFrame by the given columns
#'
#' @inheritParams Series_sort
#' @param by Column(s) to sort by. Can be character vector of column names,
#' a list of Expr(s) or a list with a mix of Expr(s) and column names.
#' @param ... More columns to sort by as above but provided one Expr per argument.
#' @param descending Logical. Sort in descending order (default is `FALSE`). This must be
#' either of length 1 or a logical vector of the same length as the number of
#' Expr(s) specified in `by` and `...`.
#' @param nulls_last A logical or logical vector of the same length as the number of columns.
#' If `TRUE`, place `null` values last insead of first.
#' @param maintain_order Whether the order should be maintained if elements are
#' equal. If `TRUE`, streaming is not possible and performance might be worse
#' since this requires a stable search.
#' @inherit as_polars_lf return
#' @keywords  LazyFrame
#' @examples
#' df <- mtcars
#' df$mpg[1] <- NA
#' df <- pl$LazyFrame(df)
#' df$sort("mpg")$collect()
#' df$sort("mpg", nulls_last = TRUE)$collect()
#' df$sort("cyl", "mpg")$collect()
#' df$sort(c("cyl", "mpg"))$collect()
#' df$sort(c("cyl", "mpg"), descending = TRUE)$collect()
#' df$sort(c("cyl", "mpg"), descending = c(TRUE, FALSE))$collect()
#' df$sort(pl$col("cyl"), pl$col("mpg"))$collect()
lazyframe__sort <- function(
    by,
    ...,
    descending = FALSE,
    nulls_last = FALSE,
    maintain_order = FALSE,
    multithreaded = TRUE) {
  self$`_rexpr`$sort_by_exprs(
    lf, wrap_elist_result(by, str_to_lit = FALSE), err_on_named_args(...),
    descending, nulls_last, maintain_order, multithreaded
  )
}


#' Perform joins on nearest keys
#'
#' This is similar to a left-join except that we match on nearest key rather
#' than equal keys.
#'
#' Both tables (DataFrames or LazyFrames) must be sorted by the asof_join key.
#' @param other LazyFrame
#' @param ...  Not used, blocks use of further positional arguments
#' @inheritParams DataFrame_join
#' @param by Join on these columns before performing asof join. Either a vector
#' of column names or a list of expressions and/or strings. Use `left_by` and
#' `right_by` if the column names to match on are different between the two
#' tables.
#' @param by_left,by_right Same as `by` but only for the left or the right
#' table. They must have the same length.
#' @param strategy Strategy for where to find match:
#' * "backward" (default): search for the last row in the right table whose `on`
#'   key is less than or equal to the left key.
#' * "forward": search for the first row in the right table whose `on` key is
#'   greater than or equal to the left key.
#' * "nearest": search for the last row in the right table whose value is nearest
#'   to the left key. String keys are not currently supported for a nearest
#'   search.
#' @param tolerance
#' Numeric tolerance. By setting this the join will only be done if the near
#' keys are within this distance. If an asof join is done on columns of dtype
#' "Date", "Datetime", "Duration" or "Time", use the Polars duration string language.
#' About the language, see the `Polars duration string language` section for details.
#'
#' There may be a circumstance where R types are not sufficient to express a
#' numeric tolerance. In that case, you can use the expression syntax like
#' `tolerance = pl$lit(42)$cast(pl$Uint64)`
#' @param coalesce Coalescing behavior (merging of `on` / `left_on` / `right_on`
#' columns):
#' * `TRUE`: Always coalesce join columns;
#' * `FALSE`: Never coalesce join columns.
#' Note that joining on any other expressions than `col` will turn off coalescing.
#'
#' @inheritSection polars_duration_string  Polars duration string language
#' @examples #
#' # create two LazyFrame to join asof
#' gdp <- pl$LazyFrame(
#'   date = as.Date(c("2015-1-1", "2016-1-1", "2017-5-1", "2018-1-1", "2019-1-1")),
#'   gdp = c(4321, 4164, 4411, 4566, 4696),
#'   group = c("b", "a", "a", "b", "b")
#' )
#'
#' pop <- pl$LazyFrame(
#'   date = as.Date(c("2016-5-12", "2017-5-12", "2018-5-12", "2019-5-12")),
#'   population = c(82.19, 82.66, 83.12, 83.52),
#'   group = c("b", "b", "a", "a")
#' )
#'
#' # optional make sure tables are already sorted with "on" join-key
#' gdp <- gdp$sort("date")
#' pop <- pop$sort("date")
#'
#'
#' # Left-join_asof LazyFrame pop with gdp on "date"
#' # Look backward in gdp to find closest matching date
#' pop$join_asof(gdp, on = "date", strategy = "backward")$collect()
#'
#' # .... and forward
#' pop$join_asof(gdp, on = "date", strategy = "forward")$collect()
#'
#' # join by a group: "only look within groups"
#' pop$join_asof(gdp, on = "date", by = "group", strategy = "backward")$collect()
#'
#' # only look 2 weeks and 2 days back
#' pop$join_asof(gdp, on = "date", strategy = "backward", tolerance = "2w2d")$collect()
#'
#' # only look 11 days back (numeric tolerance depends on polars type, <date> is in days)
#' pop$join_asof(gdp, on = "date", strategy = "backward", tolerance = 11)$collect()
lazyframe__join_asof <- function(
    other,
    ...,
    left_on = NULL,
    right_on = NULL,
    on = NULL,
    by_left = NULL,
    by_right = NULL,
    by = NULL,
    strategy = c("backward", "forward", "nearest"),
    suffix = "_right",
    tolerance = NULL,
    allow_parallel = TRUE,
    force_parallel = FALSE,
    coalesce = TRUE) {
  if (!is.null(by)) by_left <- by_right <- by
  if (!is.null(on)) left_on <- right_on <- on
  tolerance_str <- if (is.character(tolerance)) tolerance else NULL
  tolerance_num <- if (!is.character(tolerance)) tolerance else NULL

  self$`_rexpr`$join_asof(
    lf = self,
    other = other,
    left_on = left_on,
    right_on = right_on,
    left_by = by_left,
    right_by = by_right,
    allow_parallel = allow_parallel,
    force_parallel = force_parallel,
    suffix = suffix,
    strategy = strategy,
    tolerance = tolerance_num,
    tolerance_str = tolerance_str,
    coalesce = coalesce
  )
}


#' Unpivot a Frame from wide to long format
#'
#' @param on Values to use as identifier variables. If `value_vars` is
#' empty all columns that are not in `id_vars` will be used.
#' @param ... Not used.
#' @param index Columns to use as identifier variables.
#' @param variable_name Name to give to the new column containing the names of
#' the melted columns. Defaults to "variable".
#' @param value_name Name to give to the new column containing the values of
#' the melted columns. Defaults to `"value"`.
#'
#' @details
#' Optionally leaves identifiers set.
#'
#' This function is useful to massage a Frame into a format where one or more
#' columns are identifier variables (id_vars), while all other columns, considered
#' measured variables (value_vars), are "unpivoted" to the row axis, leaving just
#' two non-identifier columns, 'variable' and 'value'.
#'
#'
#'
#' @return A LazyFrame
#'
#' @examples
#' lf <- pl$LazyFrame(
#'   a = c("x", "y", "z"),
#'   b = c(1, 3, 5),
#'   c = c(2, 4, 6)
#' )
#' lf$unpivot(index = "a", on = c("b", "c"))$collect()
lazyframe__unpivot <- function(
    on = NULL,
    ...,
    index = NULL,
    variable_name = NULL,
    value_name = NULL) {
  self$`_rexpr`$unpivot(
    lf, on %||% character(), index %||% character(),
    value_name, variable_name
  ) |> unwrap("in $unpivot( ): ")
}

#' Rename column names of a LazyFrame
#'
#' @details
#' If existing names are swapped (e.g. `A` points to `B` and `B` points to `A`),
#' polars will block projection and predicate pushdowns at this node.
#' @inherit pl_LazyFrame return
#' @param ... One of the following:
#' - Key value pairs that map from old name to new name, like `old_name = "new_name"`.
#' - As above but with params wrapped in a list
#' - An R function that takes the old names character vector as input and
#'   returns the new names character vector.
#' @examples
#' lf <- pl$LazyFrame(
#'   foo = 1:3,
#'   bar = 6:8,
#'   ham = letters[1:3]
#' )
#'
#' lf$rename(foo = "apple")$collect()
#'
#' lf$rename(
#'   \(column_name) paste0("c", substr(column_name, 2, 100))
#' )$collect()
lazyframe__rename <- function(...) {
  uw <- \(res) wrap({
    res
  })


  if (!nargs()) {
    Err_plain("No arguments provided for `$rename()`.") |>
      uw()
  }

  mapping <- list2(...)
  if (is.function(mapping[[1L]])) {
    result({
      existing <- names(self)
      new <- mapping[[1L]](existing)
    }) |>
      uw()
  } else {
    if (is.list(mapping[[1L]])) {
      mapping <- mapping[[1L]]
    }
    new <- unname(unlist(mapping))
    existing <- names(mapping)
  }
  self$`_rexpr`$rename(existing, new) |>
    uw()
}

#' Fetch `n` rows of a LazyFrame
#'
#' This is similar to `$collect()` but limit the number of rows to collect. It
#' is mostly useful to check that a query works as expected.
#'
#'
#' @details
#' `$fetch()` does not guarantee the final number of rows in the DataFrame output.
#' It only guarantees that `n` rows are used at the beginning of the query.
#' Filters, join operations and a lower number of rows available in the scanned
#' file influence the final number of rows.
#'
#' @param n_rows Integer. Maximum number of rows to fetch.
#' @inheritParams lazyframe__collect
#' @return A DataFrame of maximum n_rows
#' @seealso
#'  - [`$collect()`][lazyframe__collect] - regular collect.
#'  - [`$profile()`][lazyframe__profile] - same as `$collect()` but also returns
#'    a table with each operation profiled.
#'  - [`$collect_in_background()`][lazyframe__collect_in_background] - non-blocking
#'    collect returns a future handle. Can also just be used via
#'    `$collect(collect_in_background = TRUE)`.
#'  - [`$sink_parquet()`][lazyframe__sink_parquet()] streams query to a parquet file.
#'  - [`$sink_ipc()`][lazyframe__sink_ipc()] streams query to a arrow file.
#'
#' @examples
#' # fetch 3 rows
#' pl$LazyFrame(iris)$fetch(3)
#'
#' # this fetch-query returns 4 rows, because we started with 3 and appended one
#' # row in the query (see section 'Details')
#' pl$LazyFrame(iris)$
#'   select(pl$col("Species")$append("flora gigantica, alien"))$
#'   fetch(3)
lazyframe__fetch <- function(
    n_rows = 500,
    ...,
    type_coercion = TRUE,
    predicate_pushdown = TRUE,
    projection_pushdown = TRUE,
    simplify_expression = TRUE,
    slice_pushdown = TRUE,
    comm_subplan_elim = TRUE,
    comm_subexpr_elim = TRUE,
    cluster_with_columns = TRUE,
    streaming = FALSE,
    no_optimization = FALSE) {
  if (isTRUE(no_optimization)) {
    predicate_pushdown <- FALSE
    projection_pushdown <- FALSE
    slice_pushdown <- FALSE
    comm_subplan_elim <- FALSE
    comm_subexpr_elim <- FALSE
    cluster_with_columns <- FALSE
  }

  if (isTRUE(streaming)) {
    comm_subplan_elim <- FALSE
  }

  lf <- self |>
    self$`_rexpr`$optimization_toggle(
      pe_coercion = type_coercion,
      predicate_pushdown = predicate_pushdown,
      projection_pushdown = projection_pushdown,
      simplify_expression = simplify_expression,
      slice_pushdown = slice_pushdown,
      comm_subplan_elim = comm_subplan_elim,
      comm_subexpr_elim = comm_subexpr_elim,
      cluster_with_columns = cluster_with_columns,
      streaming = streaming,
      eager = FALSE
    )

  self$`_rexpr`$fetch(n_rows)
}

#' Collect and profile a lazy query.
#' @description This will run the query and return a list containing the
#' materialized DataFrame and a DataFrame that contains profiling information
#' of each node that is executed.
#'
#' @inheritParams lazyframe__collect
#' @param show_plot Show a Gantt chart of the profiling result
#' @param truncate_nodes Truncate the label lengths in the Gantt chart to this
#' number of characters. If `0` (default), do not truncate.
#'
#' @details The units of the timings are microseconds.
#'
#'
#' @return List of two `DataFrame`s: one with the collected result, the other
#' with the timings of each step. If `show_graph = TRUE`, then the plot is
#' also stored in the list.
#' @seealso
#'  - [`$collect()`][lazyframe__collect] - regular collect.
#'  - [`$fetch()`][lazyframe__fetch] - fast limited query check
#'  - [`$collect_in_background()`][lazyframe__collect_in_background] - non-blocking
#'    collect returns a future handle. Can also just be used via
#'    `$collect(collect_in_background = TRUE)`.
#'  - [`$sink_parquet()`][lazyframe__sink_parquet()] streams query to a parquet file.
#'  - [`$sink_ipc()`][lazyframe__sink_ipc()] streams query to a arrow file.
#'
#' @examples
#' ## Simplest use case
#' pl$LazyFrame()$select(pl$lit(2) + 2)$profile()
#'
#' ## Use $profile() to compare two queries
#'
#' # -1-  map each Species-group with native polars, takes ~120us only
#' pl$LazyFrame(iris)$
#'   sort("Sepal.Length")$
#'   group_by("Species", maintain_order = TRUE)$
#'   agg(pl$col(pl$Float64)$first() + 5)$
#'   profile()
#'
#' # -2-  map each Species-group of each numeric column with an R function, takes ~7000us (slow!)
#'
#' # some R function, prints `.` for each time called by polars
#' r_func <- \(s) {
#'   cat(".")
#'   s$to_r()[1] + 5
#' }
#'
#' pl$LazyFrame(iris)$
#'   sort("Sepal.Length")$
#'   group_by("Species", maintain_order = TRUE)$
#'   agg(pl$col(pl$Float64)$map_elements(r_func))$
#'   profile()
lazyframe__profile <- function(
    type_coercion = TRUE,
    predicate_pushdown = TRUE,
    projection_pushdown = TRUE,
    simplify_expression = TRUE,
    slice_pushdown = TRUE,
    comm_subplan_elim = TRUE,
    comm_subexpr_elim = TRUE,
    cluster_with_columns = TRUE,
    streaming = FALSE,
    no_optimization = FALSE,
    collect_in_background = FALSE,
    show_plot = FALSE,
    truncate_nodes = 0) {
  if (isTRUE(no_optimization)) {
    predicate_pushdown <- FALSE
    projection_pushdown <- FALSE
    slice_pushdown <- FALSE
    comm_subplan_elim <- FALSE
    comm_subexpr_elim <- FALSE
    cluster_with_columns <- FALSE
  }

  if (isTRUE(streaming)) {
    comm_subplan_elim <- FALSE
  }

  lf <- self |>
    self$`_rexpr`$optimization_toggle(
      pe_coercion = type_coercion,
      predicate_pushdown = predicate_pushdown,
      projection_pushdown = projection_pushdown,
      simplify_expression = simplify_expression,
      slice_pushdown = slice_pushdown,
      comm_subplan_elim = comm_subplan_elim,
      comm_subexpr_elim = comm_subexpr_elim,
      cluster_with_columns = cluster_with_columns,
      streaming = streaming,
      eager = FALSE
    )

  out <- lf |>
    self$`_rexpr`$profile() >
    unwrap("in $profile()")

  if (isTRUE(show_plot)) {
    out[["plot"]] <- make_profile_plot(out, truncate_nodes) |>
      result() |>
      unwrap("in $profile()")
  }

  out
}

#' Explode columns containing a list of values
#' @description This will take every element of a list column and add it on an
#' additional row.
#'
#'
#'
#' @param ... Column(s) to be exploded as individual `Into<Expr>` or list/vector
#' of `Into<Expr>`. In a handful of places in rust-polars, only the plain variant
#' `Expr::Column` is accepted. This is currenly one of such places. Therefore
#' `pl$col("name")` and `pl$all()` is allowed, not `pl$col("name")$alias("newname")`.
#' `"name"` is implicitly converted to `pl$col("name")`.
#'
#' @details
#' Only columns of DataType `List` or `Array` can be exploded.
#'
#' Named expressions like `$explode(a = pl$col("b"))` will not implicitly trigger
#' `$alias("a")` here, due to only variant `Expr::Column` is supported in
#' rust-polars.
#'
#' @inherit as_polars_lf return
#' @examples
#' df <- pl$LazyFrame(
#'   letters = c("aa", "aa", "bb", "cc"),
#'   numbers = list(1, c(2, 3), c(4, 5), c(6, 7, 8)),
#'   numbers_2 = list(0, c(1, 2), c(3, 4), c(5, 6, 7)) # same structure as numbers
#' )
#' df
#'
#' # explode a single column, append others
#' df$explode("numbers")$collect()
#'
#' # explode two columns of same nesting structure, by names or the common dtype
#' # "List(Float64)"
#' df$explode("numbers", "numbers_2")$collect()
#' df$explode(pl$col(pl$List(pl$Float64)))$collect()
lazyframe__explode <- function(...) {
  dotdotdot_args <- unpack_list(..., .context = "in explode():")
  self$`_rexpr`$explode(dotdotdot_args)
}

#' Clone a LazyFrame
#'
#' This makes a very cheap deep copy/clone of an existing
#' [`LazyFrame`][lazyframe__class]. Rarely useful as `LazyFrame`s are nearly 100%
#' immutable. Any modification of a `LazyFrame` should lead to a clone anyways,
#' but this can be useful when dealing with attributes (see examples).
#'
#'
#' @return A LazyFrame
#' @examples
#' df1 <- pl$LazyFrame(iris)
#'
#' # Make a function to take a LazyFrame, add an attribute, and return a LazyFrame
#' give_attr <- function(data) {
#'   attr(data, "created_on") <- "2024-01-29"
#'   data
#' }
#' df2 <- give_attr(df1)
#'
#' # Problem: the original LazyFrame also gets the attribute while it shouldn't!
#' attributes(df1)
#'
#' # Use $clone() inside the function to avoid that
#' give_attr <- function(data) {
#'   data <- data$clone()
#'   attr(data, "created_on") <- "2024-01-29"
#'   data
#' }
#' df1 <- pl$LazyFrame(iris)
#' df2 <- give_attr(df1)
#'
#' # now, the original LazyFrame doesn't get this attribute
#' attributes(df1)
lazyframe__clone <- function() {
  self$`_rexpr`$clone_in_rust()
}


#' Unnest the Struct columns of a LazyFrame
#'
#' @inheritParams DataFrame_unnest
#'
#' @return A LazyFrame where some or all columns of datatype Struct are unnested.
#' @examples
#' lf <- pl$LazyFrame(
#'   a = 1:5,
#'   b = c("one", "two", "three", "four", "five"),
#'   c = 6:10
#' )$
#'   select(
#'   pl$struct("b"),
#'   pl$struct(c("a", "c"))$alias("a_and_c")
#' )
#' lf$collect()
#'
#' # by default, all struct columns are unnested
#' lf$unnest()$collect()
#'
#' # we can specify specific columns to unnest
#' lf$unnest("a_and_c")$collect()
lazyframe__unnest <- function(...) {
  columns <- unpack_list(..., .context = "in $unnest():")
  if (length(columns) == 0) {
    columns <- names(which(dtypes_are_struct(self$`_rexpr`$schema(ok))))
  } else {
    columns <- unlist(columns)
  }
  wrap({
    self$`_rexpr`$unnest(columns)
  })
}

#' Add an external context to the computation graph
#'
#' This allows expressions to also access columns from DataFrames or LazyFrames
#' that are not part of this one.
#'
#' @param other Data/LazyFrame to have access to. This can be a list of DataFrames
#' and LazyFrames.
#' @return A LazyFrame
#'
#' @examples
#' lf <- pl$LazyFrame(a = c(1, 2, 3), b = c("a", "c", NA))
#' lf_other <- pl$LazyFrame(c = c("foo", "ham"))
#'
#' lf$with_context(lf_other)$select(
#'   pl$col("b") + pl$col("c")$first()
#' )$collect()
#'
#' # Fill nulls with the median from another lazyframe:
#' train_lf <- pl$LazyFrame(
#'   feature_0 = c(-1.0, 0, 1), feature_1 = c(-1.0, 0, 1)
#' )
#' test_lf <- pl$LazyFrame(
#'   feature_0 = c(-1.0, NA, 1), feature_1 = c(-1.0, 0, 1)
#' )
#'
#' test_lf$with_context(train_lf$select(pl$all()$name$suffix("_train")))$select(
#'   pl$col("feature_0")$fill_null(pl$col("feature_0_train")$median())
#' )$collect()
lazyframe__with_context <- function(other) {
  self$`_rexpr`$with_context(other)
}


#' Create rolling groups based on a date/time or integer column
#'
#' @inherit Expr_rolling description details params
#' @param index_column Column used to group based on the time window. Often of
#' type Date/Datetime. This column must be sorted in ascending order (or, if `by`
#' is specified, then it must be sorted in ascending order within each group). In
#' case of a rolling group by on indices, dtype needs to be either Int32 or Int64.
#' Note that Int32 gets temporarily cast to Int64, so if performance matters use
#' an Int64 column.
#' @param group_by Also group by this column/these columns.
#'
#' @inheritSection polars_duration_string  Polars duration string language
#' @return A [LazyGroupBy][LazyGroupBy_class] object
#' @seealso
#' - [`<LazyFrame>$group_by_dynamic()`][lazyframe__group_by_dynamic]
#' @examples
#' dates <- c(
#'   "2020-01-01 13:45:48",
#'   "2020-01-01 16:42:13",
#'   "2020-01-01 16:45:09",
#'   "2020-01-02 18:12:48",
#'   "2020-01-03 19:45:32",
#'   "2020-01-08 23:16:43"
#' )
#'
#' df <- pl$LazyFrame(dt = dates, a = c(3, 7, 5, 9, 2, 1))$with_columns(
#'   pl$col("dt")$str$strptime(pl$Datetime())$set_sorted()
#' )
#'
#' df$rolling(index_column = "dt", period = "2d")$agg(
#'   sum_a = pl$sum("a"),
#'   min_a = pl$min("a"),
#'   max_a = pl$max("a")
#' )$collect()
lazyframe__rolling <- function(
    index_column,
    ...,
    period,
    offset = NULL,
    closed = "right",
    group_by = NULL) {
  period <- parse_as_polars_duration_string(period)
  offset <- parse_as_polars_duration_string(offset) %||% negate_duration_string(period)
  self$`_rexpr`$rolling(
    lf, index_column, period, offset, closed,
    wrap_elist_result(group_by, str_to_lit = FALSE)
  )
}


#' Group based on a date/time or integer column
#'
#' @inherit lazyframe__rolling description details params
#'
#' @param every Interval of the window.
#' @param include_boundaries Add two columns `"_lower_boundary"` and
#' `"_upper_boundary"` columns that show the boundaries of the window. This will
#' impact performance because it’s harder to parallelize.
#' @param label Define which label to use for the window:
#' * `"left"`: lower boundary of the window
#' * `"right"`: upper boundary of the window
#' * `"datapoint"`: the first value of the index column in the given window. If
#' you don’t need the label to be at one of the boundaries, choose this option
#' for maximum performance.
#' @param start_by The strategy to determine the start of the first window by:
#' * `"window"`: start by taking the earliest timestamp, truncating it with `every`,
#'   and then adding `offset`. Note that weekly windows start on Monday.
#' * `"datapoint"`: start from the first encountered data point.
#' * a day of the week (only takes effect if `every` contains `"w"`): `"monday"`
#'   starts the window on the Monday before the first data point, etc.
#'
#' @return A [LazyGroupBy][LazyGroupBy_class] object
#' @seealso
#' - [`<LazyFrame>$rolling()`][lazyframe__rolling]
#' @examples
#' lf <- pl$LazyFrame(
#'   time = pl$datetime_range(
#'     start = strptime("2021-12-16 00:00:00", format = "%Y-%m-%d %H:%M:%S", tz = "UTC"),
#'     end = strptime("2021-12-16 03:00:00", format = "%Y-%m-%d %H:%M:%S", tz = "UTC"),
#'     interval = "30m"
#'   ),
#'   n = 0:6
#' )
#' lf$collect()
#'
#' # get the sum in the following hour relative to the "time" column
#' lf$group_by_dynamic("time", every = "1h")$agg(
#'   vals = pl$col("n"),
#'   sum = pl$col("n")$sum()
#' )$collect()
#'
#' # using "include_boundaries = TRUE" is helpful to see the period considered
#' lf$group_by_dynamic("time", every = "1h", include_boundaries = TRUE)$agg(
#'   vals = pl$col("n")
#' )$collect()
#'
#' # in the example above, the values didn't include the one *exactly* 1h after
#' # the start because "closed = 'left'" by default.
#' # Changing it to "right" includes values that are exactly 1h after. Note that
#' # the value at 00:00:00 now becomes included in the interval [23:00:00 - 00:00:00],
#' # even if this interval wasn't there originally
#' lf$group_by_dynamic("time", every = "1h", closed = "right")$agg(
#'   vals = pl$col("n")
#' )$collect()
#' # To keep both boundaries, we use "closed = 'both'". Some values now belong to
#' # several groups:
#' lf$group_by_dynamic("time", every = "1h", closed = "both")$agg(
#'   vals = pl$col("n")
#' )$collect()
#'
#' # Dynamic group bys can also be combined with grouping on normal keys
#' lf <- lf$with_columns(
#'   groups = as_polars_series(c("a", "a", "a", "b", "b", "a", "a"))
#' )
#' lf$collect()
#'
#' lf$group_by_dynamic(
#'   "time",
#'   every = "1h",
#'   closed = "both",
#'   group_by = "groups",
#'   include_boundaries = TRUE
#' )$agg(pl$col("n"))$collect()
#'
#' # We can also create a dynamic group by based on an index column
#' lf <- pl$LazyFrame(
#'   idx = 0:5,
#'   A = c("A", "A", "B", "B", "B", "C")
#' )$with_columns(pl$col("idx")$set_sorted())
#' lf$collect()
#'
#' lf$group_by_dynamic(
#'   "idx",
#'   every = "2i",
#'   period = "3i",
#'   include_boundaries = TRUE,
#'   closed = "right"
#' )$agg(A_agg_list = pl$col("A"))$collect()
lazyframe__group_by_dynamic <- function(
    index_column,
    ...,
    every,
    period = NULL,
    offset = NULL,
    include_boundaries = FALSE,
    closed = "left",
    label = "left",
    group_by = NULL,
    start_by = "window") {
  every <- parse_as_polars_duration_string(every)
  offset <- parse_as_polars_duration_string(offset) %||% negate_duration_string(every)
  period <- parse_as_polars_duration_string(period) %||% every

  self$`_rexpr`$group_by_dynamic(
    lf, index_column, every, period, offset, label, include_boundaries, closed,
    wrap_elist_result(group_by, str_to_lit = FALSE), start_by
  )
}

#' Plot the query plan
#'
#' This only returns the "dot" output that can be passed to other packages, such
#' as `DiagrammeR::grViz()`.
#'
#' @param ... Not used..
#' @param optimized Optimize the query plan.
#' @inheritParams lazyframe__explain
#'
#' @return A character vector
#'
#' @examples
#' lf <- pl$LazyFrame(
#'   a = c("a", "b", "a", "b", "b", "c"),
#'   b = 1:6,
#'   c = 6:1
#' )
#'
#' query <- lf$group_by("a", maintain_order = TRUE)$agg(
#'   pl$all()$sum()
#' )$sort(
#'   "a"
#' )
#'
#' query$to_dot() |> cat()
#'
#' # You could print the graph by using DiagrammeR for example, with
#' # query$to_dot() |> DiagrammeR::grViz().
lazyframe__to_dot <- function(
    ...,
    optimized = TRUE,
    type_coercion = TRUE,
    predicate_pushdown = TRUE,
    projection_pushdown = TRUE,
    simplify_expression = TRUE,
    slice_pushdown = TRUE,
    comm_subplan_elim = TRUE,
    comm_subexpr_elim = TRUE,
    cluster_with_columns = TRUE,
    streaming = FALSE) {
  lf <- self |>
    self$`_rexpr`$optimization_toggle(
      pe_coercion = type_coercion,
      predicate_pushdown = predicate_pushdown,
      projection_pushdown = projection_pushdown,
      simplify_expression = simplify_expression,
      slice_pushdown = slice_pushdown,
      comm_subplan_elim = comm_subplan_elim,
      comm_subexpr_elim = comm_subexpr_elim,
      cluster_with_columns = cluster_with_columns,
      streaming = streaming,
      eager = FALSE
    )

  self$`_rexpr`$to_dot(optimized)
}

#' Create an empty or n-row null-filled copy of the LazyFrame
#'
#' Returns a n-row null-filled LazyFrame with an identical schema. `n` can be
#' greater than the current number of rows in the LazyFrame.
#'
#' @inheritParams DataFrame_clear
#'
#' @return A n-row null-filled LazyFrame with an identical schema
#'
#' @examples
#' df <- pl$LazyFrame(
#'   a = c(NA, 2, 3, 4),
#'   b = c(0.5, NA, 2.5, 13),
#'   c = c(TRUE, TRUE, FALSE, NA)
#' )
#'
#' df$clear()
#'
#' df$clear(n = 5)
lazyframe__clear <- function(n = 0) {
  pl$DataFrame(schema = self$schema)$clear(n)$lazy()
}


# TODO: we can't use % in the SQL query
# <https://github.com/r-lib/roxygen2/issues/1616>
#' Execute a SQL query against the LazyFrame
#'
#' The calling frame is automatically registered as a table in the SQL context
#' under the name `"self"`. All [DataFrames][DataFrame_class] and
#' [LazyFrames][lazyframe__class] found in the `envir` are also registered,
#' using their variable name.
#' More control over registration and execution behaviour is available by
#' the [SQLContext][SQLContext_class] object.
#'
#' This functionality is considered **unstable**, although it is close to
#' being considered stable. It may be changed at any point without it being
#' considered a breaking change.
#' @inherit pl_LazyFrame return
#' @inheritParams SQLContext_execute
#' @inheritParams SQLContext_register_globals
#' @param table_name `NULL` (default) or a character of an explicit name for the table
#' that represents the calling frame (the alias `"self"` will always be registered/available).
#' @seealso
#' - [SQLContext][SQLContext_class]
#' @examplesIf polars_info()$features$sql
#' lf1 <- pl$LazyFrame(a = 1:3, b = 6:8, c = c("z", "y", "x"))
#' lf2 <- pl$LazyFrame(a = 3:1, d = c(125, -654, 888))
#'
#' # Query the LazyFrame using SQL:
#' lf1$sql("SELECT c, b FROM self WHERE a > 1")$collect()
#'
#' # Join two LazyFrames:
#' lf1$sql(
#'   "
#' SELECT self.*, d
#' FROM self
#' INNER JOIN lf2 USING (a)
#' WHERE a > 1 AND b < 8
#' "
#' )$collect()
#'
#' # Apply SQL transforms (aliasing "self" to "frame") and subsequently
#' # filter natively (you can freely mix SQL and native operations):
#' lf1$sql(
#'   query = r"(
#' SELECT
#'  a,
#' MOD(a, 2) == 0 AS a_is_even,
#' (b::float / 2) AS 'b/2',
#' CONCAT_WS(':', c, c, c) AS c_c_c
#' FROM frame
#' ORDER BY a
#' )",
#'   table_name = "frame"
#' )$filter(!pl$col("c_c_c")$str$starts_with("x"))$collect()
lazyframe__sql <- function(query, ..., table_name = NULL, envir = parent.frame()) {
  result({
    ctx <- pl$SQLContext()$register_globals(envir = envir)$register("self", self)

    if (!is.null(table_name)) {
      ctx$register(table_name, self)
    }

    ctx$execute(query)
  })
}


#' Take every nth row in the LazyFrame
#'
#' @param n Gather every `n`-th row.
#' @param offset Starting index.
#'
#' @return A LazyFrame
#'
#' @examples
#' lf <- pl$LazyFrame(a = 1:4, b = 5:8)
#' lf$gather_every(2)$collect()
#'
#' lf$gather_every(2, offset = 1)$collect()
lazyframe__gather_every <- function(n, offset = 0) {
  self$select(pl$col("*")$gather_every(n, offset))
}


#' Cast LazyFrame column(s) to the specified dtype
#'
#' This allows to convert all columns to a datatype or to convert only specific
#' columns. Contrarily to the Python implementation, it is not possible to
#' convert all columns of a specific datatype to another datatype.
#'
#' @param dtypes Either a datatype or a list where the names are column names and
#' the values are the datatypes to convert to.
#' @param ... Ignored.
#' @param strict If `TRUE` (default), throw an error if a cast could not be done
#' (for instance, due to an overflow). Otherwise, return `null`.
#'
#' @return A LazyFrame
#'
#' @examples
#' lf <- pl$LazyFrame(
#'   foo = 1:3,
#'   bar = c(6, 7, 8),
#'   ham = as.Date(c("2020-01-02", "2020-03-04", "2020-05-06"))
#' )
#'
#' # Cast only some columns
#' lf$cast(list(foo = pl$Float32, bar = pl$UInt8))$collect()
#'
#' # Cast all columns to the same type
#' lf$cast(pl$String)$collect()
lazyframe__cast <- function(dtypes, ..., strict = TRUE) {
  if (!is.list(dtypes)) {
    self$`_rexpr`$cast_all(dtype = dtypes, strict = strict) |>
      unwrap("in $cast():")
  } else {
    self$`_rexpr`$cast(dtypes = dtypes, strict = strict) |>
      unwrap("in $cast():")
  }
}

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
#' @inherit as_polars_lf return
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]> Name-value pairs of objects
#' to be converted to polars [expressions][Expr] by the [as_polars_expr()]
#' function. Characters are parsed as column names, other non-expression inputs
#' are parsed as [literals][pl__lit]. Each name will be used as the expression
#' name.
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

#' Select columns from this LazyFrame
#'
#' This will run all expression sequentially instead of in parallel. Use this
#' when the work per expression is cheap.
#'
#' @inherit as_polars_lf return
#' @inheritParams lazyframe__select
#'
#' @examples
#' lf <- pl$LazyFrame(
#'   foo = 1:3,
#'   bar = 6:8,
#'   ham = letters[1:3]
#' )
#' lf$select_seq("foo")$collect()
lazyframe__select_seq <- function(...) {
  wrap({
    structify <- parse_env_auto_structify()
    parse_into_list_of_expressions(..., `__structify` = structify) |>
      self$`_ldf`$select_seq()
  })
}

#' Start a group by operation
#'
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]> Column(s) to group by.
#' Accepts expression input. Strings are parsed as column names.
#' @param .maintain_order Ensure that the order of the groups is consistent with
#' the input data. This is slower than a default group by. Setting this to
#' `TRUE` blocks the possibility to run on the streaming engine.
#'
# TODO: need a proper definition to link to
#' @return A lazy groupby
#' @examples
#' # Group by one column and call agg() to compute the grouped sum of another
#' # column.
#' lf <- pl$LazyFrame(
#'   a = c("a", "b", "a", "b", "c"),
#'   b = c(1, 2, 1, 3, 3),
#'   c = c(5, 4, 3, 2, 1)
#' )
#' lf$group_by("a")$agg(pl$col("b")$sum())$collect()
#'
#' # Set .maintain_order = TRUE to ensure the order of the groups is consistent
#' # with the input.
#' lf$group_by("a", .maintain_order = TRUE)$agg(pl$col("b")$sum())$collect()
#'
#' # Group by multiple columns by passing a vector of column names.
#' lf$group_by(c("a", "b"))$agg(pl$col("c")$max())$collect()
#'
#' # Or use positional arguments to group by multiple columns in the same way.
#' # Expressions are also accepted.
#' lf$
#'   group_by("a", pl$col("b") / 2)$
#'   agg(pl$col("c")$mean())$collect()
lazyframe__group_by <- function(..., .maintain_order = FALSE) {
  wrap({
    exprs <- parse_into_list_of_expressions(...)
    self$`_ldf`$group_by(exprs, .maintain_order)
  })
}

#' Materialize this LazyFrame into a DataFrame
#'
#' By default, all query optimizations are enabled.
#'
#' @inheritParams rlang::check_dots_empty0
#' @param type_coercion Logical. Coerce types such that operations succeed and
#' run on minimal required memory.
#' @param predicate_pushdown Logical. Applies filters as early as possible at
#' scan level.
#' @param projection_pushdown Logical. Select only the columns that are needed
#' at the scan level.
#' @param simplify_expression Logical. Various optimizations, such as constant
#' folding and replacing expensive operations with faster alternatives.
#' @param slice_pushdown Logical. Only load the required slice from the scan
#' level. Don't materialize sliced outputs (e.g. `join$head(10)`).
#' @param comm_subplan_elim Logical. Will try to cache branching subplans that
#'  occur on self-joins or unions.
#' @param comm_subexpr_elim Logical. Common subexpressions will be cached and
#' reused.
#' @param cluster_with_columns Combine sequential independent calls to
#' [`with_columns()`][lazyframe__with_columns].
#' @param streaming `r lifecycle::badge("experimental")` Logical. Process the
#' query in batches to handle larger-than-memory data. If `FALSE` (default),
#' the entire query is processed in a single batch.
#' @param _eager A logical, indicates to turn off multi-node optimizations and
#' the other optimizations. This option is intended for internal use only.
#'
#' @inherit as_polars_lf return
#'
#' @seealso
#'  - [`$fetch()`][lazyframe__fetch] - fast limited query check
#'  - [`$profile()`][lazyframe__profile] - same as `$collect()` but also returns
#'    a table with each operation profiled.
#'  - [`$collect_in_background()`][lazyframe__collect_in_background] - non-blocking
#'    collect returns a future handle. Can also just be used via
#'    `$collect(collect_in_background = TRUE)`.
#'  - [`$sink_parquet()`][lazyframe__sink_parquet()] streams query to a parquet file.
#'  - [`$sink_ipc()`][lazyframe__sink_ipc()] streams query to a arrow file.
#'
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

#' Resolve the schema of this LazyFrame
#'
#' This resolves the query plan but does not trigger computations.
#'
#' @return A named list with names indicating column names and values indicating
#' column data types.
#'
#' @examples
#' lf <- pl$LazyFrame(
#'   foo = 1:3,
#'   bar = 6:8,
#'   ham = c("a", "b", "c")
#' )
#'
#' lf$collect_schema()
#'
#' lf$with_columns(
#'   baz = (pl$col("foo") + pl$col("bar"))$cast(pl$String),
#'   pl$col("bar")$cast(pl$Int64)
#' )$collect_schema()
lazyframe__collect_schema <- function() {
  wrap({
    lapply(self$`_ldf`$collect_schema(), function(x) {
      .savvy_wrap_PlRDataType(x) |>
        wrap()
    })
  })
}

#' Collect and profile a lazy query.
#'
#' This will run the query and return a list containing the materialized
#' DataFrame and a DataFrame that contains profiling information of each node
#' that is executed.
#'
#' @inheritParams rlang::check_dots_empty0
#' @inheritParams lazyframe__collect
#' @param show_plot Show a Gantt chart of the profiling result
#' @param truncate_nodes Truncate the label lengths in the Gantt chart to this
#' number of characters. If `0` (default), do not truncate.
#'
#' @details
#' The units of the timings are microseconds.
#'
#' @return List of two `DataFrame`s: one with the collected result, the other
#' with the timings of each step. If `show_graph = TRUE`, then the plot is
#' also stored in the list.
#' @seealso
#'  - [`$collect()`][LazyFrame_collect] - regular collect.
#'  - [`$fetch()`][LazyFrame_fetch] - fast limited query check
#'  - [`$collect_in_background()`][LazyFrame_collect_in_background] - non-blocking
#'    collect returns a future handle. Can also just be used via
#'    `$collect(collect_in_background = TRUE)`.
#'  - [`$sink_parquet()`][LazyFrame_sink_parquet()] streams query to a parquet file.
#'  - [`$sink_ipc()`][LazyFrame_sink_ipc()] streams query to a arrow file.
#'
#' @examples
#' ## Simplest use case
#' pl$LazyFrame()$select(pl$lit(2) + 2)$profile()
#'
#' ## Use $profile() to compare two queries
#'
#' # -1-  map each Species-group with native polars
#' pl$LazyFrame(iris)$
#'   sort("Sepal.Length")$
#'   group_by("Species", maintain_order = TRUE)$
#'   agg(pl$col(pl$Float64)$first() + 5)$
#'   profile()
#'
#' # -2-  map each Species-group of each numeric column with an R function
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
    no_optimization = FALSE,
    collect_in_background = FALSE,
    show_plot = FALSE,
    truncate_nodes = 0) {
  wrap({
    check_dots_empty0(...)

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

    lf <- self$`_rexpr`$optimization_toggle(
      type_coercion = type_coercion,
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
      .pr$LazyFrame$profile()

    if (isTRUE(show_plot)) {
      out[["plot"]] <- make_profile_plot(out, truncate_nodes) |>
        result() |>
        unwrap("in $profile()")
    }
    out
  })
}

#' Create a string representation of the query plan
#'
#' The query plan is read from bottom to top. When `optimized = FALSE`, the
#' query as it was written by the user is shown. This is not what Polars runs.
#' Instead, it applies optimizations that are displayed by default by `$explain()`.
#' One classic example is the predicate pushdown, which applies the filter as
#' early as possible (i.e. at the bottom of the plan).
#'
#' @inheritParams rlang::check_dots_empty0
#' @inheritParams lazyframe__collect
#' @param format The format to use for displaying the logical plan. Must be
#' either `"plain"` (default) or `"tree"`.
#' @param optimized Return an optimized query plan. If `TRUE` (default), the
#' subsequent optimization flags control which optimizations run.
#'
#' @return A character value containing the query plan.
#' @examples
#' lazy_frame <- as_polars_lf(iris)
#'
#' # Prepare your query
#' lazy_query <- lazy_frame$sort("Species")$filter(pl$col("Species") != "setosa")
#'
#' # This is the query that was written by the user, without any optimizations
#' # (use cat() for better printing)
#' lazy_query$explain(optimized = FALSE) |> cat()
#'
#' # This is the query after `polars` optimizes it: instead of sorting first and
#' # then filtering, it is faster to filter first and then sort the rest.
#' lazy_query$explain() |> cat()
#'
#' # Also possible to see this as tree format
#' lazy_query$explain(format = "tree") |> cat()
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

lazyframe__collect_schema <- function() {
  self$`_ldf`$collect_schema() |>
    lapply(function(x) {
      .savvy_wrap_PlRDataType(x) |>
        wrap()
    }) |>
    wrap()
}

#' Cast LazyFrame column(s) to the specified dtype(s)
#'
#' This allows to convert all columns to a datatype or to convert only specific
#' columns. Contrarily to the Python implementation, it is not possible to
#' convert all columns of a specific datatype to another datatype.
#'
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]> Either a datatype to which
#' all columns will be cast, or a list where the names are column names and the
#' values are the datatypes to convert to.
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
#' lf$cast(foo = pl$Float32, bar = pl$UInt8)$collect()
#'
#' # Cast all columns to the same type
#' lf$cast(pl$String)$collect()
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

#' Filter the rows in the LazyFrame based on a predicate expression
#'
#' The original order of the remaining rows is preserved. Rows where the filter
#' does not evaluate to `TRUE` are discarded, including nulls.
#'
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]> Expression that evaluates to
#' a boolean Series.
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(
#'   foo = c(1, 2, 3, NA, 4, NA, 0),
#'   bar = c(6, 7, 8, NA, NA, 9, 0),
#'   ham = c("a", "b", "c", NA, "d", "e", "f")
#' )
#'
#' # Filter on one condition
#' lf$filter(pl$col("foo") > 1)$collect()
#'
#' # Filter on multiple conditions
#' lf$filter((pl$col("foo") < 3) & (pl$col("ham") == "a"))$collect()
#'
#' # Filter on an OR condition
#' lf$filter((pl$col("foo") == 1) | (pl$col("ham") == " c"))$collect()
#'
#' # Filter by comparing two columns against each other
#' lf$filter(pl$col("foo") == pl$col("bar"))$collect()
#' lf$filter(pl$col("foo") != pl$col("bar"))$collect()
#'
#' # Notice how the row with null values is filtered out$ In order to keep the
#' # rows with nulls, use:
#' lf$filter(pl$col("foo")$ne_missing(pl$col("bar")))$collect()
lazyframe__filter <- function(...) {
  parse_predicates_constraints_into_expression(...) |>
    self$`_ldf`$filter() |>
    wrap()
}

#' Sort the LazyFrame by the given columns
#'
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]> Column(s) to sort by. Can be
#' character values indicating column names or Expr(s).
#' @param descending Sort in descending order. When sorting by multiple
#' columns, this can be specified per column by passing a logical vector.
#' @param nulls_last Place null values last. When sorting by multiple
#' columns, this can be specified per column by passing a logical vector.
#' @param maintain_order Whether the order should be maintained if elements are
#' equal. If `TRUE`, streaming is not possible and performance might be worse
#' since this requires a stable search.
#' @param multithreaded Sort using multiple threads.
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(
#'   a = c(1, 2, NA, 4),
#'   b = c(6, 5, 4, 3),
#'   c = c("a", "c", "b", "a")
#' )
#'
#' # Pass a single column name to sort by that column.
#' lf$sort("a")$collect()
#'
#' # Sorting by expressions is also supported
#' lf$sort(pl$col("a") + pl$col("b") * 2, nulls_last = TRUE)$collect()
#'
#' # Sort by multiple columns by passing a vector of columns
#' lf$sort(c("c", "a"), descending = TRUE)$collect()
#'
#' # Or use positional arguments to sort by multiple columns in the same way
#' lf$sort("c", "a", descending = c(FALSE, TRUE))$collect()
lazyframe__sort <- function(
    ...,
    descending = FALSE,
    nulls_last = FALSE,
    multithreaded = TRUE,
    maintain_order = FALSE) {
  check_dots_unnamed()

  by <- parse_into_list_of_expressions(...)
  if (length(by) == 0) {
    abort("`...` must contain at least one element.")
  }
  descending <- extend_bool(descending, length(by), "descending", "...")
  nulls_last <- extend_bool(nulls_last, length(by), "nulls_last", "...")

  self$`_ldf`$sort_by_exprs(
    by,
    descending = descending,
    nulls_last = nulls_last,
    multithreaded = multithreaded,
    maintain_order = maintain_order
  ) |> wrap()
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
#' @inherit as_polars_lf return
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
  structify <- parse_env_auto_structify()

  parse_into_list_of_expressions(..., `__structify` = structify) |>
    self$`_ldf`$with_columns() |>
    wrap()
}

#' Modify/append column(s) of a LazyFrame
#'
#' @description
#' This will run all expression sequentially instead of in parallel. Use this
#' only when the work per expression is cheap.
#'
#' Add columns or modify existing ones with expressions. This is similar to
#' `dplyr::mutate()` as it keeps unmentioned columns (unlike `$select()`).
#'
#' However, unlike `dplyr::mutate()`, one cannot use new variables in subsequent
#' expressions in the same `$with_columns_seq()`call. For instance, if you create a
#' variable `x`, you will only be able to use it in another `$with_columns_seq()`
#' or `$select()` call.
#'
#' @inherit as_polars_lf return
#' @inheritParams lazyframe__select
#' @examples
#' # Pass an expression to add it as a new column.
#' lf <- pl$LazyFrame(
#'   a = 1:4,
#'   b = c(0.5, 4, 10, 13),
#'   c = c(TRUE, TRUE, FALSE, TRUE),
#' )
#' lf$with_columns_seq((pl$col("a")^2)$alias("a^2"))$collect()
#'
#' # Added columns will replace existing columns with the same name.
#' lf$with_columns_seq(a = pl$col("a")$cast(pl$Float64))$collect()
#'
#' # Multiple columns can be added
#' lf$with_columns_seq(
#'   (pl$col("a")^2)$alias("a^2"),
#'   (pl$col("b") / 2)$alias("b/2"),
#'   (pl$col("c")$not())$alias("not c"),
#' )$collect()
#'
#' # Name expression instead of `$alias()`
#' lf$with_columns_seq(
#'   `a^2` = pl$col("a")^2,
#'   `b/2` = pl$col("b") / 2,
#'   `not c` = pl$col("c")$not(),
#' )$collect()
#'
#' # Expressions with multiple outputs can automatically be instantiated
#' # as Structs by enabling the experimental setting `POLARS_AUTO_STRUCTIFY`:
#' if (requireNamespace("withr", quietly = TRUE)) {
#'   withr::with_envvar(c(POLARS_AUTO_STRUCTIFY = "1"), {
#'     lf$drop("c")$with_columns_seq(
#'       diffs = pl$col("a", "b")$diff()$name$suffix("_diff"),
#'     )$collect()
#'   })
#' }
lazyframe__with_columns_seq <- function(...) {
  wrap({
    structify <- parse_env_auto_structify()

    parse_into_list_of_expressions(..., `__structify` = structify) |>
      self$`_ldf`$with_columns_seq()
  })
}

#' Remove columns from the DataFrame
#'
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]> Names of the columns that
#' should be removed from the dataframe. Accepts column selector input.
#' @param strict Validate that all column names exist in the current schema,
#' and throw an exception if any do not.
#'
#' @inherit as_polars_lf return
#' @examples
#' # Drop columns by passing the name of those columns
#' lf <- pl$LazyFrame(
#'   foo = 1:3,
#'   bar = c(6, 7, 8),
#'   ham = c("a", "b", "c")
#' )
#' lf$drop("ham")$collect()
#' lf$drop("ham", "bar")$collect()
#'
#' # Drop multiple columns by passing a selector
#' lf$drop(cs$all())$collect()
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

#' Get the first `n` rows
#'
#' @param n Number of rows to return.
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(a = 1:6, b = 7:12)
#' lf$head()$collect()
#' lf$head(2)$collect()
lazyframe__head <- function(n = 5) {
  self$slice(0, n) |>
    wrap()
}

#' Get the first `n` rows
#'
#' Alias for [`<LazyFrame>$head()`][lazyframe__head].
#'
#' @inheritParams lazyframe__head
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(a = 1:6, b = 7:12)
#' lf$limit()$collect()
#' lf$limit(2)$collect()
lazyframe__limit <- function(n = 5) {
  wrap({
    self$head(n)
  })
}

#' Get the last `n` rows
#'
#' @inheritParams lazyframe__head
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(a = 1:6, b = 7:12)
#' lf$tail()$collect()
#' lf$tail(2)$collect()
lazyframe__tail <- function(n = 5) {
  self$`_ldf`$tail(n) |>
    wrap()
}


#' Get the first row of the LazyFrame
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(a = 1:4, b = c(1, 2, 1, 1))
#' lf$first()$collect()
lazyframe__first <- function() {
  wrap({
    self$slice(0, 1)
  })
}

#' Get the last row of the LazyFrame
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(a = 1:4, b = c(1, 2, 1, 1))
#' lf$last()$collect()
lazyframe__last <- function() {
  wrap({
    self$tail(1)
  })
}

#' Aggregate the columns in the LazyFrame to their maximum value
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(a = 1:4, b = c(1, 2, 1, 1))
#' lf$max()$collect()
lazyframe__max <- function() {
  wrap({
    self$`_ldf`$max()
  })
}

#' Aggregate the columns in the LazyFrame to their mean value
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(a = 1:4, b = c(1, 2, 1, 1))
#' lf$mean()$collect()
lazyframe__mean <- function() {
  wrap({
    self$`_ldf`$mean()
  })
}

#' Aggregate the columns in the LazyFrame to their median value
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(a = 1:4, b = c(1, 2, 1, 1))
#' lf$median()$collect()
lazyframe__median <- function() {
  wrap({
    self$`_ldf`$median()
  })
}

#' Aggregate the columns in the LazyFrame to their minimum value
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(a = 1:4, b = c(1, 2, 1, 1))
#' lf$min()$collect()
lazyframe__min <- function() {
  wrap({
    self$`_ldf`$min()
  })
}

#' Aggregate the columns of this LazyFrame to their sum values
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(a = 1:4, b = c(1, 2, 1, 1))
#' lf$sum()$collect()
lazyframe__sum <- function() {
  wrap({
    self$`_ldf`$sum()
  })
}

#' Aggregate the columns in the LazyFrame to their variance value
#'
#' @inheritParams DataFrame_var
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(a = 1:4, b = c(1, 2, 1, 1))
#' lf$var()$collect()
#' lf$var(ddof = 0)$collect()
lazyframe__var <- function(ddof = 1) {
  wrap({
    self$`_ldf`$var(ddof)
  })
}

#' Aggregate the columns of this LazyFrame to their standard deviation values
#'
#' @inheritParams DataFrame_std
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(a = 1:4, b = c(1, 2, 1, 1))
#' lf$std()$collect()
#' lf$std(ddof = 0)$collect()
lazyframe__std <- function(ddof = 1) {
  wrap({
    self$`_ldf`$std(ddof)
  })
}

#' Aggregate the columns in the DataFrame to a unique quantile value
#'
#' @inheritParams DataFrame_quantile
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(a = 1:4, b = c(1, 2, 1, 1))
#' lf$quantile(0.7)$collect()
lazyframe__quantile <- function(
    quantile,
    interpolation = c("nearest", "higher", "lower", "midpoint", "linear")) {
  wrap({
    interpolation <- arg_match0(
      interpolation,
      values = c("nearest", "higher", "lower", "midpoint", "linear")
    )
    self$`_ldf`$quantile(as_polars_expr(quantile, as_lit = TRUE)$`_rexpr`, interpolation)
  })
}

#' @inherit expr__fill_nan title params
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(
#'   a = c(1.5, 2, NaN, 4),
#'   b = c(1.5, NaN, NaN, 4)
#' )
#' lf$fill_nan(99)$collect()
lazyframe__fill_nan <- function(value) {
  wrap({
    self$`_ldf`$fill_nan(as_polars_expr(value)$`_rexpr`)
  })
}

#' @inherit DataFrame_fill_null title description params
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(
#'   a = c(1.5, 2, NA, 4),
#'   b = c(1.5, NA, NA, 4)
#' )
#' lf$fill_null(99)$collect()
lazyframe__fill_null <- function(fill_value) {
  wrap({
    self$`_ldf`$fill_null(as_polars_expr(fill_value)$`_rexpr`)
  })
}

#' Shift values by the given number of indices
#'
#' @inheritParams rlang::check_dots_empty0
#' @param n Number of indices to shift forward. If a negative value is passed,
#' values are shifted in the opposite direction instead.
#' @param fill_value Fill the resulting null values with this value. Accepts
#' expression input. Non-expression inputs are parsed as literals.
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(a = 1:4, b = 5:8)
#'
#' # By default, values are shifted forward by one index.
#' lf$shift()$collect()
#'
#' # Pass a negative value to shift in the opposite direction instead.
#' lf$shift(-2)$collect()
#'
#' # Specify fill_value to fill the resulting null values.
#' lf$shift(-2, fill_value = 100)$collect()
lazyframe__shift <- function(n = 1, ..., fill_value = NULL) {
  wrap({
    check_dots_empty0(...)
    self$`_ldf`$shift(as_polars_expr(n)$`_rexpr`, as_polars_expr(fill_value)$`_rexpr`)
  })
}

#' Reverse the LazyFrame
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(key = c("a", "b", "c"), val = 1:3)
#' lf$reverse()$collect()
lazyframe__reverse <- function() {
  wrap({
    self$`_ldf`$reverse()
  })
}

#' Get a slice of the LazyFrame.
#'
#' @param offset Start index. Negative indexing is supported.
#' @param length Length of the slice. If `NULL` (default), all rows starting at
#' the offset will be selected.
#'
#' @return A [LazyFrame][lazyframe__class]
#' @examples
#' lf <- pl$LazyFrame(x = c("a", "b", "c"), y = 1:3, z = 4:6)
#' lf$slice(1, 2)$collect()
lazyframe__slice <- function(offset, length = NULL) {
  wrap({
    self$`_ldf`$slice(offset, length)
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
    self$`_ldf`$tail(n)
  })
}

#' Drop all rows that contain null values
#'
#' The original order of the remaining rows is preserved.
#'
#' @param subset Column name(s) for which null values are considered. If `NULL`
#' (default), use all columns.
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(
#'   foo = 1:3,
#'   bar = c(6, NA, 8),
#'   ham = c("a", "b", NA)
#' )
#'
#' # The default behavior of this method is to drop rows where any single value
#' # of the row is null.
#' lf$drop_nulls()$collect()
#'
#' # This behaviour can be constrained to consider only a subset of columns, as
#' # defined by name or with a selector. For example, dropping rows if there is
#' # a null in any of the integer columns:
#' lf$drop_nulls(subset = cs$integer())$collect()
lazyframe__drop_nulls <- function(subset = NULL) {
  wrap({
    subset <- parse_into_list_of_expressions(!!!subset)
    self$`_ldf`$drop_nulls(subset)
  })
}

#' Drop duplicate rows from this DataFrame
#'
#' @inheritParams rlang::check_dots_empty0
#' @param subset Column name(s) or selector(s), to consider when identifying
#' duplicate rows. If `NULL` (default), use all columns.
#' @param keep Which of the duplicate rows to keep. Must be one of:
#' * `"any"`: does not give any guarantee of which row is kept. This allows
#'   more optimizations.
#' * `"none"`: don’t keep duplicate rows.
#' * `"first"`: keep first unique row.
#' * `"last"`: keep last unique row.
#' @param maintain_order Keep the same order as the original LazyFrame. This is
#' more expensive to compute. Setting this to `TRUE` blocks the possibility to
#' run on the streaming engine.
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(
#'   foo = c(1, 2, 3, 1),
#'   bar = c("a", "a", "a", "a"),
#'   ham = c("b", "b", "b", "b"),
#' )
#' lf$unique(maintain_order = TRUE)$collect()
#'
#' lf$unique(subset = c("bar", "ham"), maintain_order = TRUE)$collect()
#'
#' lf$unique(keep = "last", maintain_order = TRUE)$collect()
lazyframe__unique <- function(
    subset = NULL,
    ...,
    keep = c("any", "none", "first", "last"),
    maintain_order = FALSE) {
  wrap({
    check_dots_empty0(...)
    keep <- arg_match0(keep, values = c("any", "none", "first", "last"))
    if (!is.null(subset)) {
      subset <- parse_into_list_of_expressions(!!!subset)
    }
    self$`_ldf`$unique(subset = subset, keep = keep, maintain_order = maintain_order)
  })
}

#' Join LazyFrames
#'
#' This function can do both mutating joins (adding columns based on matching
#' observations, for example with `how = "left"`) and filtering joins (keeping
#' observations based on matching observations, for example with `how =
#' "inner"`).
#'
#' @inheritParams rlang::check_dots_empty0
#' @param other LazyFrame to join with.
#' @param on Either a vector of column names or a list of expressions and/or
#'   strings. Use `left_on` and `right_on` if the column names to match on are
#'   different between the two DataFrames.
#' @param how One of the following methods:
#' * "inner": returns rows that have matching values in both tables
#' * "left": returns all rows from the left table, and the matched rows from
#'   the right table
#' * "right": returns all rows from the right table, and the matched rows from
#'   the left table
#' * "full": returns all rows when there is a match in either left or right
#'   table
#' * "cross": returns the Cartesian product of rows from both tables
#' * "semi": returns rows from the left table that have a match in the right
#'   table.
#' * "anti": returns rows from the left table that have no match in the right
#'   table.
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
#' Note that this is currently not supported by the streaming engine.
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
#' Note that joining on any other expressions than `col` will turn off
#' coalescing.
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(
#'   foo = 1:3,
#'   bar = c(6, 7, 8),
#'   ham = c("a", "b", "c")
#' )
#' other_lf <- pl$LazyFrame(
#'   apple = c("x", "y", "z"),
#'   ham = c("a", "b", "d")
#' )
#' lf$join(other_lf, on = "ham")$collect()
#'
#' lf$join(other_lf, on = "ham", how = "full")$collect()
#'
#' lf$join(other_lf, on = "ham", how = "left", coalesce = TRUE)$collect()
#'
#' lf$join(other_lf, on = "ham", how = "semi")$collect()
#'
#' lf$join(other_lf, on = "ham", how = "anti")$collect()
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
  wrap({
    check_dots_empty0(...)
    check_polars_lf(other)
    how <- arg_match0(
      how,
      values = c("inner", "full", "left", "right", "semi", "anti", "cross")
    )
    validate <- arg_match0(validate, values = c("m:m", "1:m", "m:1", "1:1"))
    uses_on <- !is.null(on)
    uses_left_on <- !is.null(left_on)
    uses_right_on <- !is.null(right_on)
    uses_lr_on <- uses_left_on | uses_right_on
    if (uses_on && uses_lr_on) {
      abort("cannot use 'on' in conjunction with 'left_on' or 'right_on'.")
    }
    if (uses_left_on && !uses_right_on) {
      abort("'left_on' requires corresponding 'right_on'")
    }
    if (!uses_left_on && uses_right_on) {
      abort("'right_on' requires corresponding 'left_on'")
    }
    if (how == "cross") {
      if (uses_on | uses_lr_on) {
        abort("cross join should not pass join keys.")
      }
      return(
        self$`_ldf`$join(
          other$`_ldf`, as.list(NULL), as.list(NULL),
          how = how, validate = validate,
          join_nulls = join_nulls, suffix = suffix,
          allow_parallel = allow_parallel, force_parallel = force_parallel,
          coalesce = coalesce
        )
      )
    }

    if (uses_on) {
      rexprs_right <- rexprs_left <- parse_into_list_of_expressions(!!!on)
    } else if (uses_lr_on) {
      rexprs_left <- parse_into_list_of_expressions(!!!left_on)
      rexprs_right <- parse_into_list_of_expressions(!!!right_on)
    } else {
      abort("must specify either `on`, or `left_on` and `right_on`.")
    }
    self$`_ldf`$join(
      other$`_ldf`, rexprs_left, rexprs_right,
      how = how, validate = validate,
      join_nulls = join_nulls, suffix = suffix,
      allow_parallel = allow_parallel, force_parallel = force_parallel,
      coalesce = coalesce
    )
  })
}

#' Perform a join based on one or multiple (in)equality predicates
#'
#' @description
#' `r lifecycle::badge("experimental")`
#'
#' This performs an inner join, so only rows where all predicates are true are
#' included in the result, and a row from either LazyFrame may be included
#' multiple times in the result.
#'
#' Note that the row order of the input LazyFrames is not preserved.
#'
#' @param other LazyFrame to join with.
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]> (In)Equality condition to
#' join the two tables on. When a column name occurs in both tables, the proper
#' suffix must be applied in the predicate. For example, if both tables have a
#' column `"x"` that you want to use in the conditions, you must refer to the
#' column of the right table as `"x<suffix>"`.
#' @param suffix Suffix to append to columns with a duplicate name.
#'
#' @inherit as_polars_lf return
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
  wrap({
    check_polars_lf(other)
    by <- parse_into_list_of_expressions(...)
    self$`_ldf`$join_where(other$`_ldf`, by, suffix)
  })
}

#' Unpivot a LazyFrame from wide to long format
#'
#' This function is useful to massage a LazyFrame into a format where one or
#' more columns are identifier variables (`index`) while all other columns,
#' considered measured variables (`on`), are “unpivoted” to the row axis
#' leaving just two non-identifier columns, "variable" and "value".
#'
#' @inheritParams rlang::check_dots_empty0
#' @param on Values to use as identifier variables. If `value_vars` is
#' empty all columns that are not in `id_vars` will be used.
#' @param index Columns to use as identifier variables.
#' @param variable_name Name to give to the new column containing the names of
#' the melted columns. Defaults to "variable".
#' @param value_name Name to give to the new column containing the values of
#' the melted columns. Defaults to `"value"`.
#'
#' @inherit as_polars_lf return
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
  wrap({
    check_dots_empty0(...)
    if (!is.null(on)) {
      on <- parse_into_list_of_expressions(!!!on)
    }
    if (!is.null(index)) {
      index <- parse_into_list_of_expressions(!!!index)
    }
    self$`_ldf`$unpivot(on, index, value_name, variable_name)
  })
}

#' Rename column names
#'
#' @param mapping Either a function that takes a character vector as input and
#' returns one as input, or a named list where names are old column names and
#' values are the new ones.
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]> If `mapping` is missing,
#' those values are used.
#' @param strict Validate that all column names exist in the current schema,
#' and throw an error if any do not. (Note that this parameter is a no-op when
#' passing a function to `mapping`).
#'
#' @details
#' If existing names are swapped (e.g. 'A' points to 'B' and 'B' points to
#' 'A'), polars will block projection and predicate pushdowns at this node.
#'
#' @inherit as_polars_lf return
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
lazyframe__rename <- function(mapping, ..., strict = TRUE) {
  wrap({
    if (!missing(mapping) && is_function(mapping)) {
      check_dots_empty0(...)
      self$select(pl$all()$name$map(mapping))
    } else {
      if (missing(mapping) || !is.list(mapping)) {
        mapping <- list2(...)
      }
      existing <- names(mapping)
      new <- unlist(mapping)
      self$`_ldf`$rename(existing, new, strict)
    }
  })
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
#' as_polars_lf(iris)$fetch(3)
#'
#' # this fetch-query returns 4 rows, because we started with 3 and appended one
#' # row in the query (see section 'Details')
#' as_polars_lf(iris)$
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
    self$`_ldf`$optimization_toggle(
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

  self$`_ldf`$fetch(n_rows)
}

#' Collect and profile a lazy query
#'
#' @description
#' This will run the query and return a list containing the
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
#' as_polars_lf(iris)$
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
#' as_polars_lf(iris)$
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

  lf <- self$`_ldf`$optimization_toggle(
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

  out <- lapply(self$`_ldf`$profile(), \(x) {
    x |>
      .savvy_wrap_PlRDataFrame() |>
      wrap()
  })

  if (isTRUE(show_plot)) {
    out[["plot"]] <- make_profile_plot(out, truncate_nodes)
  }

  out
}

#' Serialize the logical plan of this LazyFrame to a string in JSON format
#'
#' @return A character value
#' @examples
#' lf <- pl$LazyFrame(a = 1:3)$sum()
#' lf$serialize()
lazyframe__serialize <- function() {
  wrap({
    self$`_ldf`$serialize()
  })
}

#' Read a logical plan from a file to construct a LazyFrame
#'
#' @param source String containing the LazyFrame logical plan in JSON format.
#'
#' @return A character value
#' @examples
#' lf <- pl$LazyFrame(a = 1:3)$sum()
#' ser <- lf$serialize()
#' pl$deserialize_lf(ser)
pl__deserialize_lf <- function(source) {
  wrap({
    deserialize_lf(source)
  })
}

#' Explode the DataFrame to long format by exploding the given columns
#'
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]> Column names, expressions, or
#' a selector defining them. The underlying columns being exploded must be of
#' the `List` or `Array` data type.
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(
#'   letters = c("a", "a", "b", "c"),
#'   numbers = list(1, c(2, 3), c(4, 5), c(6, 7, 8))
#' )
#'
#' lf$explode("numbers")$collect()
lazyframe__explode <- function(...) {
  wrap({
    check_dots_unnamed()
    by <- parse_into_list_of_expressions(...)
    self$`_ldf`$explode(by)
  })
}

#' Clone a LazyFrame
#'
#' This makes a very cheap deep copy/clone of an existing
#' [`LazyFrame`][lazyframe__class]. Rarely useful as `LazyFrame`s are nearly 100%
#' immutable. Any modification of a `LazyFrame` should lead to a clone anyways,
#' but this can be useful when dealing with attributes (see examples).
#'
#'
#' @inherit as_polars_lf return
#' @examples
#' df1 <- as_polars_lf(iris)
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
#' df1 <- as_polars_lf(iris)
#' df2 <- give_attr(df1)
#'
#' # now, the original LazyFrame doesn't get this attribute
#' attributes(df1)
lazyframe__clone <- function() {
  self$`_ldf`$clone_in_rust()
}


#' Decompose struct columns into separate columns for each of their fields
#'
#' The new columns will be inserted into the LazyFrame at the location of the
#' struct column.
#'
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]> Name of the struct column(s)
#' that should be unnested.
#'
#' @inherit as_polars_lf return
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
#' lf$unnest("a_and_c")$collect()
lazyframe__unnest <- function(...) {
  wrap({
    columns <- parse_into_list_of_expressions(...)
    self$`_ldf`$unnest(columns)
  })
}

#' Add an external context to the computation graph
#'
#' This allows expressions to also access columns from DataFrames or LazyFrames
#' that are not part of this one.
#'
#' @param other Data/LazyFrame to have access to. This can be a list of DataFrames
#' and LazyFrames.
#' @inherit as_polars_lf return
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
  self$`_ldf`$with_context(other)
}


#' Create rolling groups based on a date/time or integer column
#'
#' @description
#' Different from `group_by_dynamic`, the windows are now determined by the
#' individual values and are not of constant intervals. For constant intervals
#' use [`<LazyFrame>$group_by_dynamic()`][lazyframe__group_by_dynamic].
#'
#' If you have a time series `<t_0, t_1, ..., t_n>`, then by default the
#' windows created will be:
#' * `(t_0 - period, t_0]`
#' * `(t_1 - period, t_1]`
#' * …
#' * `(t_n - period, t_n]`
#'
#' whereas if you pass a non-default `offset`, then the windows will be:
#' * `(t_0 + offset, t_0 + offset + period]`
#' * `(t_1 + offset, t_1 + offset + period]`
#' * …
#' * `(t_n + offset, t_n + offset + period]`
#'
#' @inheritParams rlang::check_dots_empty0
#' @inheritParams lazyframe__group_by_dynamic
#' @param period Length of the window - must be non-negative.
#' @param offset Offset of the window. Default is `-period`.
#'
#' @inherit expr__rolling_max params details
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
#'   pl$col("dt")$str$strptime(pl$Datetime())
#' )
#'
#' df$rolling(index_column = "dt", period = "2d")$agg(
#'   sum_a = pl$col("a")$sum(),
#'   min_a = pl$col("a")$min(),
#'   max_a = pl$col("a")$max()
#' )$collect()
lazyframe__rolling <- function(
    index_column,
    ...,
    period,
    offset = NULL,
    closed = "right",
    group_by = NULL) {
  wrap({
    check_dots_empty0(...)
    closed <- arg_match0(closed, values = c("both", "left", "right", "none"))
    period <- parse_as_duration_string(period)
    if (!is.null(offset)) {
      offset <- parse_as_duration_string(offset)
    } else {
      offset <- negate_duration_string(period)
    }
    if (!is.null(group_by) && !is.list(group_by)) {
      group_by <- list(group_by)
    }
    by <- parse_into_list_of_expressions(!!!group_by)
    self$`_ldf`$rolling(
      as_polars_expr(index_column)$`_rexpr`, period, offset, closed, by
    )
  })
}


#' Group based on a date/time or integer column
#'
#' Time windows are calculated and rows are assigned to windows. Different from
#' a normal group by is that a row can be member of multiple groups. By
#' default, the windows look like:
#' * [start, start + period)
#' * [start + every, start + every + period)
#' * [start + 2*every, start + 2*every + period)
#' * …
#'
#' where `start` is determined by `start_by`, `offset`, `every`, and the
#' earliest datapoint. See the `start_by` argument description for details.
#'
#' @inheritParams rlang::check_dots_empty0
#' @param index_column Column used to group based on the time window. Often of
#' type Date/Datetime. This column must be sorted in ascending order (or, if
#' `group_by` is specified, then it must be sorted in ascending order within
#' each group).
#' In case of a dynamic group by on indices, the data type needs to be either
#' Int32 or In64. Note that Int32 gets temporarily cast to Int64, so if
#' performance matters, use an Int64 column.
#' @param every Interval of the window.
#' @param period Length of the window. If `NULL` (default), it will equal
#' `every`.
#' @param offset Offset of the window, does not take effect if
#' `start_by = "datapoint"`. Defaults to zero.
#' @param include_boundaries Add two columns `"_lower_boundary"` and
#' `"_upper_boundary"` columns that show the boundaries of the window. This will
#' impact performance because it’s harder to parallelize.
#' @param closed Define which sides of the interval are closed (inclusive).
#' Default is `"left"`.
#' @param label Define which label to use for the window:
#' * `"left"`: lower boundary of the window
#' * `"right"`: upper boundary of the window
#' * `"datapoint"`: the first value of the index column in the given window. If
#' you don’t need the label to be at one of the boundaries, choose this option
#' for maximum performance.
#' @param start_by The strategy to determine the start of the first window by:
#' * `"window"`: start by taking the earliest timestamp, truncating it with
#'   `every`, and then adding `offset`. Note that weekly windows start on
#'   Monday.
#' * `"datapoint"`: start from the first encountered data point.
#' * a day of the week (only takes effect if `every` contains `"w"`): `"monday"`
#'   starts the window on the Monday before the first data point, etc.
#'
#' @details
#' The `every`, `period`, and `offset` arguments are created with the following
#' string language:
#' - 1ns # 1 nanosecond
#' - 1us # 1 microsecond
#' - 1ms # 1 millisecond
#' - 1s  # 1 second
#' - 1m  # 1 minute
#' - 1h  # 1 hour
#' - 1d  # 1 day
#' - 1w  # 1 calendar week
#' - 1mo # 1 calendar month
#' - 1y  # 1 calendar year
#' These strings can be combined:
#'   - 3d12h4m25s # 3 days, 12 hours, 4 minutes, and 25 seconds
#'
#' In case of a `group_by_dynamic` on an integer column, the windows are
#' defined by:
#' - 1i # length 1
#' - 10i # length 10
#'
#' @return A [LazyGroupBy][LazyGroupBy_class] object
#' @seealso
#' - [`<LazyFrame>$rolling()`][lazyframe__rolling]
#'
#' @examples
#' lf <- pl$select(
#'   time = pl$datetime_range(
#'     start = strptime("2021-12-16 00:00:00", format = "%Y-%m-%d %H:%M:%S", tz = "UTC"),
#'     end = strptime("2021-12-16 03:00:00", format = "%Y-%m-%d %H:%M:%S", tz = "UTC"),
#'     interval = "30m"
#'   ),
#'   n = 0:6
#' )$lazy()
#' lf$collect()
#'
#' # Group by windows of 1 hour.
#' lf$group_by_dynamic("time", every = "1h", closed = "right")$agg(
#'   vals = pl$col("n")
#' )$collect()
#'
#' # The window boundaries can also be added to the aggregation result
#' lf$group_by_dynamic(
#'   "time",
#'   every = "1h", include_boundaries = TRUE, closed = "right"
#' )$agg(
#'   pl$col("n")$mean()
#' )$collect()
#'
#' # When closed = "left", the window excludes the right end of interval:
#' # [lower_bound, upper_bound)
#' lf$group_by_dynamic("time", every = "1h", closed = "left")$agg(
#'   pl$col("n")
#' )$collect()
#'
#' # When closed = "both" the time values at the window boundaries belong to 2
#' # groups.
#' lf$group_by_dynamic("time", every = "1h", closed = "both")$agg(
#'   pl$col("n")
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
  wrap({
    check_dots_empty0(...)
    closed <- arg_match0(closed, values = c("both", "left", "right", "none"))
    start_by <- arg_match0(
      start_by,
      values = c(
        "window", "datapoint", "monday", "tuesday", "wednesday", "thursday",
        "friday", "saturday", "sunday"
      )
    )
    every <- parse_as_duration_string(every)
    offset <- parse_as_duration_string(offset) %||% "0ns"
    period <- parse_as_duration_string(period) %||% every
    group_by <- parse_into_list_of_expressions(!!!group_by)

    self$`_ldf`$group_by_dynamic(
      as_polars_expr(index_column)$`_rexpr`, every, period, offset, label,
      include_boundaries, closed,
      group_by, start_by
    )
  })
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
    self$`_ldf`$optimization_toggle(
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

  self$`_ldf`$to_dot(optimized)
}

#' Create an empty or n-row null-filled copy of the LazyFrame
#'
#' Returns a n-row null-filled LazyFrame with an identical schema. `n` can be
#' greater than the current number of rows in the LazyFrame.
#'
#' @param n Number of (empty) rows to return in the cleared frame.
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
#' @inherit as_polars_lf return
#'
#' @examples
#' lf <- pl$LazyFrame(a = 1:4, b = 5:8)
#' lf$gather_every(2)$collect()
#'
#' lf$gather_every(2, offset = 1)$collect()
lazyframe__gather_every <- function(n, offset = 0) {
  self$select(pl$col("*")$gather_every(n, offset))
}

#' Return the number of non-null elements for each column
#'
#' @inherit as_polars_lf return
#'
#' @examples
#' lf <- pl$LazyFrame(a = 1:4, b = c(1, 2, 1, NA), c = rep(NA, 4))
#' lf$count()$collect()
lazyframe__count <- function() {
  wrap({
    self$`_ldf`$count()
  })
}

#' Return the number of null elements for each column
#'
#' @inherit as_polars_lf return
#'
#' @examples
#' lf <- pl$LazyFrame(a = 1:4, b = c(1, 2, 1, NA), c = rep(NA, 4))
#' lf$null_count()$collect()
lazyframe__null_count <- function() {
  wrap({
    self$`_ldf`$null_count()
  })
}

#' Return the `k` smallest rows
#'
#' @description
#' Non-null elements are always preferred over null elements, regardless of the
#' value of `reverse`. The output is not guaranteed to be in any particular
#' order, call `sort()` after this function if you wish the output to be sorted.
#'
#' @inheritParams rlang::check_dots_empty
#' @param k Number of rows to return.
#' @param by Column(s) used to determine the bottom rows. Accepts expression
#' input. Strings are parsed as column names.
#' @param reverse Consider the `k` largest elements of the by column(s)
#' (instead of the k smallest). This can be specified per column by passing a
#' sequence of booleans.
#'
#' @inherit as_polars_lf return
#'
#' @examples
#' lf <- pl$LazyFrame(
#'   a = c("a", "b", "a", "b", "b", "c"),
#'   b = c(2, 1, 1, 3, 2, 1)
#' )
#'
#' # Get the rows which contain the 4 smallest values in column b.
#' lf$bottom_k(4, by = "b")$collect()
#'
#' # Get the rows which contain the 4 smallest values when sorting on column a
#' # and b$
#' lf$bottom_k(4, by = c("a", "b"))$collect()
lazyframe__bottom_k <- function(k, ..., by, reverse = FALSE) {
  wrap({
    check_dots_empty0(...)
    by <- parse_into_list_of_expressions(!!!by)
    reverse <- extend_bool(reverse, length(by), "reverse", "...")
    self$`_ldf`$bottom_k(k, by, reverse)
  })
}

#' Return the `k` largest rows
#'
#' @inherit lazyframe__bottom_k description params
#' @inheritParams rlang::check_dots_empty0
#' @param reverse Consider the `k` smallest elements of the `by` column(s)
#' (instead of the `k` largest). This can be specified per column by passing a
#' sequence of booleans.

#' @inherit as_polars_lf return
#'
#' @examples
#' lf <- pl$LazyFrame(
#'   a = c("a", "b", "a", "b", "b", "c"),
#'   b = c(2, 1, 1, 3, 2, 1)
#' )
#'
#' # Get the rows which contain the 4 largest values in column b.
#' lf$top_k(4, by = "b")$collect()
#'
#' # Get the rows which contain the 4 largest values when sorting on column a
#' # and b$
#' lf$top_k(4, by = c("a", "b"))$collect()
lazyframe__top_k <- function(k, ..., by, reverse = FALSE) {
  wrap({
    check_dots_empty0(...)
    by <- parse_into_list_of_expressions(!!!by)
    reverse <- extend_bool(reverse, length(by), "reverse", "...")
    self$`_ldf`$top_k(k, by, reverse)
  })
}

#' Interpolate intermediate values
#'
#' The interpolation method is linear.
#' @inherit as_polars_lf return
#'
#' @examples
#' lf <- pl$LazyFrame(
#'   foo = c(1, NA, 9, 10),
#'   bar = c(6, 7, 9, NA),
#'   ham = c(1, NA, NA, 9)
#' )
#'
#' lf$interpolate()$collect()
lazyframe__interpolate <- function() {
  wrap({
    self$select(pl$col("*")$interpolate())
  })
}

#' Take two sorted DataFrames and merge them by the sorted key
#'
#' The output of this operation will also be sorted. It is the callers
#' responsibility that the frames are sorted by that key, otherwise the output
#' will not make sense. The schemas of both LazyFrames must be equal.
#'
#' @param other Other DataFrame that must be merged.
#' @param key Key that is sorted.
#'
#' @inherit as_polars_lf return
#'
#' @examples
#' lf1 <- pl$LazyFrame(
#'   name = c("steve", "elise", "bob"),
#'   age = c(42, 44, 18)
#' )$sort("age")
#'
#' lf2 <- pl$LazyFrame(
#'   name = c("anna", "megan", "steve", "thomas"),
#'   age = c(21, 33, 42, 20)
#' )$sort("age")
#'
#' lf1$merge_sorted(lf2, key = "age")$collect()
lazyframe__merge_sorted <- function(other, key) {
  wrap({
    self$`_ldf`$merge_sorted(other$`_ldf`, key)
  })
}

#' Indicate that one or multiple columns are sorted
#'
#' This can speed up future operations, but it can lead to incorrect results if
#' the data is **not** sorted! Use with care!
#'
#' @inheritParams rlang::check_dots_empty0
#' @param column Columns that are sorted.
#' @param descending Whether the columns are sorted in descending order.
#'
#' @inherit as_polars_lf return
lazyframe__set_sorted <- function(column, ..., descending = FALSE) {
  wrap({
    check_dots_empty0(...)
    self$with_columns(pl$col(column)$set_sorted(descending = descending))
  })
}

#' Add a row index as the first column in the LazyFrame
#'
#' @description
#' Using this function can have a negative effect on query performance. This
#' may, for instance, block predicate pushdown optimization.
#'
#' @inheritParams rlang::check_dots_empty0
#' @param name Name of the index column.
#' @param offset Start the index at this offset. Cannot be negative.
#'
#' @inherit as_polars_lf return
#' @examples
#' lf <- pl$LazyFrame(x = c(1, 3, 5), y = c(2, 4, 6))
#' lf$with_row_index()$collect()
#'
#' lf$with_row_index("id", offset = 1000)$collect()
#'
#' # An index column can also be created using the expressions int_range()
#' # and len()$
#' lf$with_columns(
#'   index = pl$int_range(pl$len(), dtype = pl$UInt32)
#' )$collect()
lazyframe__with_row_index <- function(name = "index", offset = 0) {
  wrap({
    self$`_ldf`$with_row_index(name, offset)
  })
}

#' Evaluate the query in streaming mode and write to a Parquet file
#'
#' @description
#' `r lifecycle::badge("experimental")`
#'
#' This allows streaming results that are larger than RAM to be written to disk.
#'
#' @inheritParams rlang::check_dots_empty0
#' @param path A character. File path to which the file should be written.
#' @param compression The compression method. Must be one of:
#' * `"lz4"`: fast compression/decompression.
#' * `"uncompressed"`
#' * `"snappy"`: this guarantees that the parquet file will be compatible with
#'   older parquet readers.
#' * `"gzip"`
#' * `"lzo"`
#' * `"brotli"`
#' * `"zstd"`: good compression performance.
#' @param compression_level `NULL` or integer. The level of compression to use.
#'  Only used if method is one of `"gzip"`, `"brotli"`, or `"zstd"`. Higher
#' compression means smaller files on disk:
#'  * `"gzip"`: min-level: 0, max-level: 10.
#'  * `"brotli"`: min-level: 0, max-level: 11.
#'  * `"zstd"`: min-level: 1, max-level: 22.
#' @param statistics Whether statistics should be written to the Parquet
#' headers. Possible values:
#' * `TRUE`: enable default set of statistics (default)
#' * `FALSE`: disable all statistics
#' * `"full"`: calculate and write all available statistics.
#' * A named list where all values must be `TRUE` or `FALSE`, e.g.
#'   `list(min = TRUE, max = FALSE)`. Statistics available are `"min"`, `"max"`,
#'   `"distinct_count"`, `"null_count"`.
#' @param row_group_size Size of the row groups in number of rows. If `NULL`
#' (default), the chunks of the DataFrame are used. Writing in smaller chunks
#' may reduce memory pressure and improve writing speeds.
#' @param data_page_size Size of the data page in bytes. If `NULL` (default), it
#' is set to 1024^2 bytes.
#' @param maintain_order Maintain the order in which data is processed. Setting
#' this to `FALSE` will be slightly faster.
#' @inheritParams lazyframe__collect
#'
#' @rdname IO_sink_parquet
#' @return Invisibly returns the input LazyFrame
#'
#' @examples
#' # sink table 'mtcars' from mem to parquet
#' tmpf <- tempfile()
#' as_polars_lf(mtcars)$sink_parquet(tmpf)
#'
#' # stream a query end-to-end
#' tmpf2 <- tempfile()
#' pl$scan_parquet(tmpf)$select(pl$col("cyl") * 2)$sink_parquet(tmpf2)
#'
#' # load parquet directly into a DataFrame / memory
#' pl$scan_parquet(tmpf2)$collect()
lazyframe__sink_parquet <- function(
    path,
    ...,
    compression = "zstd",
    compression_level = 3,
    statistics = TRUE,
    row_group_size = NULL,
    data_page_size = NULL,
    maintain_order = TRUE,
    type_coercion = TRUE,
    predicate_pushdown = TRUE,
    projection_pushdown = TRUE,
    simplify_expression = TRUE,
    slice_pushdown = TRUE,
    no_optimization = FALSE) {
  wrap({
    check_dots_empty0(...)
    compression <- arg_match0(
      compression,
      values = c("lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd")
    )

    if (isTRUE(no_optimization)) {
      predicate_pushdown <- FALSE
      projection_pushdown <- FALSE
      slice_pushdown <- FALSE
    }

    lf <- self$`_ldf`$optimization_toggle(
      type_coercion = type_coercion,
      predicate_pushdown = predicate_pushdown,
      projection_pushdown = projection_pushdown,
      simplify_expression = simplify_expression,
      slice_pushdown = slice_pushdown,
      comm_subplan_elim = FALSE,
      comm_subexpr_elim = FALSE,
      cluster_with_columns = FALSE,
      streaming = FALSE,
      `_eager` = FALSE
    )

    statistics <- translate_statistics(statistics)

    lf$sink_parquet(
      path = path,
      compression = compression,
      compression_level = compression_level,
      statistics = statistics,
      row_group_size = row_group_size,
      data_page_size = data_page_size,
      maintain_order = maintain_order
    )

    invisible(self)
  })
}

#' Evaluate the query in streaming mode and write to an IPC file
#'
#' @inherit lazyframe__sink_parquet description params return
#' @inheritParams rlang::check_dots_empty0
#' @param compression `NULL` or one of:
#' * `"uncompressed"`: same as `NULL`.
#' * `"lz4"`: fast compression/decompression.
#' * `"zstd"`: good compression performance.
#'
#' @rdname IO_sink_ipc
#'
#' @examples
#' # sink table 'mtcars' from mem to ipc
#' tmpf <- tempfile()
#' as_polars_lf(mtcars)$sink_ipc(tmpf)
#'
#' # stream a query end-to-end (not supported yet, https://github.com/pola-rs/polars/issues/1040)
#' # tmpf2 = tempfile()
#' # pl$scan_ipc(tmpf)$select(pl$col("cyl") * 2)$sink_ipc(tmpf2)
#'
#' # load ipc directly into a DataFrame / memory
#' # pl$scan_ipc(tmpf2)$collect()
lazyframe__sink_ipc <- function(
    path,
    ...,
    compression = c("zstd", "lz4", "uncompressed"),
    maintain_order = TRUE,
    type_coercion = TRUE,
    predicate_pushdown = TRUE,
    projection_pushdown = TRUE,
    simplify_expression = TRUE,
    slice_pushdown = TRUE,
    no_optimization = FALSE) {
  wrap({
    check_dots_empty0(...)
    compression <- compression %||% "uncompressed"
    compression <- arg_match0(
      compression,
      values = c("lz4", "uncompressed", "zstd")
    )

    if (isTRUE(no_optimization)) {
      predicate_pushdown <- FALSE
      projection_pushdown <- FALSE
      slice_pushdown <- FALSE
    }

    lf <- self$`_ldf`$optimization_toggle(
      type_coercion = type_coercion,
      predicate_pushdown = predicate_pushdown,
      projection_pushdown = projection_pushdown,
      simplify_expression = simplify_expression,
      slice_pushdown = slice_pushdown,
      comm_subplan_elim = FALSE,
      comm_subexpr_elim = FALSE,
      cluster_with_columns = FALSE,
      streaming = FALSE,
      `_eager` = FALSE
    )

    lf$sink_ipc(
      path = path,
      compression = compression,
      maintain_order = maintain_order
    )

    invisible(self)
  })
}

#' Evaluate the query in streaming mode and write to a CSV file
#'
#' @inherit lazyframe__sink_parquet description params return
#' @inheritParams rlang::check_dots_empty0
#' @param include_bom Logical, whether to include UTF-8 BOM in the CSV output.
#' @param include_header Logical, hether to include header in the CSV output.
#' @param separator Separate CSV fields with this symbol.
#' @param line_terminator String used to end each row.
#' @param quote_char Byte to use as quoting character.
#' @param batch_size Number of rows that will be processed per thread.
#' @param datetime_format A format string, with the specifiers defined by the
#' [chrono](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
#' Rust crate. If no format specified, the default fractional-second precision
#' is inferred from the maximum timeunit found in the frame’s Datetime cols (if
#' any).
#' @param date_format A format string, with the specifiers defined by the
#' [chrono](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
#' Rust crate.
#' @param time_format A format string, with the specifiers defined by the
#' [chrono](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
#' Rust crate.
#' @param float_precision Whether to use scientific form always (`TRUE`), never
#' (`FALSE`), or automatically (`NULL`) for Float32 and Float64 datatypes.
#' @param null_value A string representing null values (defaulting to the empty
#' string).
#' @param quote_style Determines the quoting strategy used. Must be one of:
#' * `"necessary"` (default): This puts quotes around fields only when
#'   necessary. They are necessary when fields contain a quote, delimiter or
#'   record terminator. Quotes are also necessary when writing an empty record
#'   (which is indistinguishable from a record with one empty field). This is
#'   the default.
#' * `"always"`: This puts quotes around every field. Always.
#' * `"never"`: This never puts quotes around fields, even if that results in
#'   invalid CSV data (e.g.: by not quoting strings containing the separator).
#' * `"non_numeric"`: This puts quotes around all fields that are non-numeric.
#'   Namely, when writing a field that does not parse as a valid float or
#'   integer, then quotes will be used even if they aren`t strictly necessary.
#'
#' @rdname IO_sink_csv
#'
#' @examples
#' # sink table 'mtcars' from mem to CSV
#' tmpf <- tempfile()
#' pl$LazyFrame(mtcars)$sink_csv(tmpf)
#'
#' # stream a query end-to-end
#' tmpf2 <- tempfile()
#' pl$scan_csv(tmpf)$select(pl$col("cyl") * 2)$sink_csv(tmpf2)
#'
#' # load parquet directly into a DataFrame / memory
#' pl$scan_csv(tmpf2)$collect()
lazyframe__sink_csv <- function(
    path,
    ...,
    include_bom = FALSE,
    include_header = TRUE,
    separator = ",",
    line_terminator = "\n",
    quote_char = '"',
    batch_size = 1024,
    datetime_format = NULL,
    date_format = NULL,
    time_format = NULL,
    float_precision = NULL,
    null_value = "",
    quote_style = "necessary",
    maintain_order = TRUE,
    type_coercion = TRUE,
    predicate_pushdown = TRUE,
    projection_pushdown = TRUE,
    simplify_expression = TRUE,
    slice_pushdown = TRUE,
    no_optimization = FALSE) {
  wrap({
    check_dots_empty0(...)
    quote_style <- arg_match0(
      quote_style,
      values = c("necessary", "always", "never", "non_numeric")
    )

    if (isTRUE(no_optimization)) {
      predicate_pushdown <- FALSE
      projection_pushdown <- FALSE
      slice_pushdown <- FALSE
    }

    lf <- self$`_ldf`$optimization_toggle(
      type_coercion = type_coercion,
      predicate_pushdown = predicate_pushdown,
      projection_pushdown = projection_pushdown,
      simplify_expression = simplify_expression,
      slice_pushdown = slice_pushdown,
      comm_subplan_elim = FALSE,
      comm_subexpr_elim = FALSE,
      cluster_with_columns = FALSE,
      streaming = FALSE,
      `_eager` = FALSE
    )

    lf$sink_csv(
      path = path,
      include_bom = include_bom,
      include_header = include_header,
      separator = separator,
      line_terminator = line_terminator,
      quote_char = quote_char,
      batch_size = batch_size,
      datetime_format = datetime_format,
      date_format = date_format,
      time_format = time_format,
      float_precision = float_precision,
      null_value = null_value,
      quote_style = quote_style,
      maintain_order = maintain_order
    )

    invisible(self)
  })
}

#' Evaluate the query in streaming mode and write to an NDJSON file
#'
#' @inherit lazyframe__sink_parquet description params return
#' @inheritParams rlang::check_dots_empty0
#'
#' @rdname IO_sink_ndjson
#'
#' @examples
#' # sink table 'mtcars' from mem to NDJSON
#' tmpf <- tempfile(fileext = ".ndjson")
#' pl$LazyFrame(mtcars)$sink_ndjson(tmpf)
#'
#' # load parquet directly into a DataFrame / memory
#' pl$scan_ndjson(tmpf)$collect()
lazyframe__sink_ndjson <- function(
    path,
    ...,
    maintain_order = TRUE,
    type_coercion = TRUE,
    predicate_pushdown = TRUE,
    projection_pushdown = TRUE,
    simplify_expression = TRUE,
    slice_pushdown = TRUE,
    no_optimization = FALSE) {
  wrap({
    check_dots_empty0(...)
    if (isTRUE(no_optimization)) {
      predicate_pushdown <- FALSE
      projection_pushdown <- FALSE
      slice_pushdown <- FALSE
    }

    lf <- self$`_ldf`$optimization_toggle(
      type_coercion = type_coercion,
      predicate_pushdown = predicate_pushdown,
      projection_pushdown = projection_pushdown,
      simplify_expression = simplify_expression,
      slice_pushdown = slice_pushdown,
      comm_subplan_elim = FALSE,
      comm_subexpr_elim = FALSE,
      cluster_with_columns = FALSE,
      streaming = FALSE,
      `_eager` = FALSE
    )

    lf$sink_json(
      path = path,
      maintain_order = maintain_order
    )

    invisible(self)
  })
}

#' Perform joins on nearest keys
#'
#' @description
#' This is similar to a left-join except that we match on nearest key rather
#' than equal keys. Both frames must be sorted by the `asof_join` key.
#'
#' @inheritParams rlang::check_dots_empty0
#' @param other LazyFrame to join with.
#' @inheritParams dataframe__join
#' @param by Join on these columns before performing asof join. Either a vector
#' of column names or a list of expressions and/or strings. Use `left_by` and
#' `right_by` if the column names to match on are different between the two
#' tables.
#' @param by_left,by_right Same as `by` but only for the left or the right
#' table. They must have the same length.
#' @param strategy Strategy for where to find match:
#' * `"backward"` (default): search for the last row in the right table whose
#'   `on` key is less than or equal to the left key.
#' * `"forward"`: search for the first row in the right table whose `on` key is
#'   greater than or equal to the left key.
#' * `"nearest"`: search for the last row in the right table whose value is
#'   nearest to the left key. String keys are not currently supported for a
#'   nearest search.
#' @param tolerance Numeric tolerance. By setting this the join will only be
#' done if the near keys are within this distance. If an asof join is done on
#' columns of dtype "Date", "Datetime", "Duration" or "Time", use the Polars
#' duration string language (see details).
#'
#' @param coalesce Coalescing behavior (merging of `on` / `left_on` /
#' `right_on` columns):
#' * `TRUE`: Always coalesce join columns;
#' * `FALSE`: Never coalesce join columns.
#' Note that joining on any other expressions than `col` will turn off
#' coalescing.
#'
#' @inheritSection polars_duration_string Polars duration string language
#' @examples
#' gdp <- pl$LazyFrame(
#'   date = as.Date(c("2016-1-1", "2017-5-1", "2018-1-1", "2019-1-1", "2020-1-1")),
#'   gdp = c(4164, 4411, 4566, 4696, 4827)
#' )
#'
#' pop <- pl$LazyFrame(
#'   date = as.Date(c("2016-3-1", "2018-8-1", "2019-1-1")),
#'   population = c(82.19, 82.66, 83.12)
#' )
#'
#' # optional make sure tables are already sorted with "on" join-key
#' gdp <- gdp$sort("date")
#' pop <- pop$sort("date")
#'
#'
#' # Note how the dates don’t quite match. If we join them using join_asof and
#' # strategy = 'backward', then each date from population which doesn’t have
#' # an exact match is matched with the closest earlier date from gdp:
#' pop$join_asof(gdp, on = "date", strategy = "backward")$collect()
#'
#' # Note how:
#' # - date 2016-03-01 from population is matched with 2016-01-01 from gdp;
#' # - date 2018-08-01 from population is matched with 2018-01-01 from gdp.
#' # You can verify this by passing coalesce = FALSE:
#' pop$join_asof(
#'   gdp,
#'   on = "date", strategy = "backward", coalesce = FALSE
#' )$collect()
#'
#' # If we instead use strategy = 'forward', then each date from population
#' # which doesn’t have an exact match is matched with the closest later date
#' # from gdp:
#' pop$join_asof(gdp, on = "date", strategy = "forward")$collect()
#'
#' # Note how:
#' # - date 2016-03-01 from population is matched with 2017-01-01 from gdp;
#' # - date 2018-08-01 from population is matched with 2019-01-01 from gdp.
#'
#' # Finally, strategy = 'nearest' gives us a mix of the two results above, as
#' # each date from population which doesn’t have an exact match is matched
#' # with the closest date from gdp, regardless of whether it’s earlier or
#' # later:
#' pop$join_asof(gdp, on = "date", strategy = "nearest")$collect()
#'
#' # Note how:
#' # - date 2016-03-01 from population is matched with 2016-01-01 from gdp;
#' # - date 2018-08-01 from population is matched with 2019-01-01 from gdp.
#'
#' # The `by` argument allows joining on another column first, before the asof
#' # join. In this example we join by country first, then asof join by date, as
#' # above.
#' gdp2 <- pl$LazyFrame(
#'   country = rep(c("Germany", "Netherlands"), each = 5),
#'   date = rep(
#'     as.Date(c("2016-1-1", "2017-1-1", "2018-1-1", "2019-1-1", "2020-1-1")),
#'     2
#'   ),
#'   gdp = c(4164, 4411, 4566, 4696, 4827, 784, 833, 914, 910, 909)
#' )$sort("country", "date")
#' gdp2$collect()
#'
#' pop2 <- pl$LazyFrame(
#'   country = rep(c("Germany", "Netherlands"), each = 3),
#'   date = rep(as.Date(c("2016-3-1", "2018-8-1", "2019-1-1")), 2),
#'   population = c(82.19, 82.66, 83.12, 17.11, 17.32, 17.40)
#' )$sort("country", "date")
#' pop2$collect()
#'
#' pop2$join_asof(
#'   gdp2,
#'   by = "country", on = "date", strategy = "nearest"
#' )$collect()
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
  wrap({
    check_dots_empty0(...)
    strategy <- arg_match0(strategy, values = c("backward", "forward", "nearest"))
    if (!is.null(by)) by_left <- by_right <- by
    if (!is.null(on)) left_on <- right_on <- on
    tolerance_str <- if (is.character(tolerance)) tolerance else NULL
    tolerance_num <- if (!is.character(tolerance)) tolerance else NULL

    self$`_ldf`$join_asof(
      other = other$`_ldf`,
      left_on = as_polars_expr(left_on)$`_rexpr`,
      right_on = as_polars_expr(right_on)$`_rexpr`,
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
  })
}

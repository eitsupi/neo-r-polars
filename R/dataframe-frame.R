# TODO: link to data type docs
#' Polars DataFrame class (`polars_data_frame`)
#'
#' DataFrames are two-dimensional data structure representing data
#' as a table with rows and columns. Polars DataFrames are similar to
#' [R Data Frames][data.frame]. R Data Frame's columns are [R vectors][vector],
#' while Polars DataFrame's columns are [Polars Series][Series].
#'
#' The `pl$DataFrame()` function mimics the constructor of the DataFrame class of Python Polars.
#' This function is basically a shortcut for `list(...) |> as_polars_df()`,
#' so each argument in `...` is converted to a Polars Series by [as_polars_series()]
#' and then passed to [as_polars_df()].
#'
#' @section Active bindings:
#' - `columns`: `$columns` returns a character vector with the names of the columns.
#' - `dtypes`: `$dtypes` returns a nameless list of the data type of each column.
#' - `schema`: `$schema` returns a named list with the column names as names and the data types as values.
#' - `shape`: `$shape` returns a integer vector of length two with the number of rows and columns of the DataFrame.
#' - `height`: `$height` returns a integer with the number of rows of the DataFrame.
#' - `width`: `$width` returns a integer with the number of columns of the DataFrame.
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]>
#' Name-value pairs of objects to be converted to polars [Series]
#' by the [as_polars_series()] function.
#' Each [Series] will be used as a column of the [DataFrame].
#' All values must be the same length.
#' Each name will be used as the column name. If the name is empty,
#' the original name of the [Series] will be used.
#' @return A polars [DataFrame]
#' @examples
#' # Constructing a DataFrame from vectors:
#' pl$DataFrame(a = 1:2, b = 3:4)
#'
#' # Constructing a DataFrame from Series:
#' pl$DataFrame(pl$Series("a", 1:2), pl$Series("b", 3:4))
#'
#' # Constructing a DataFrame from a list:
#' data <- list(a = 1:2, b = 3:4)
#'
#' ## Using the as_polars_df function (recommended)
#' as_polars_df(data)
#'
#' ## Using dynamic dots feature
#' pl$DataFrame(!!!data)
#'
#' # Active bindings:
#' df <- pl$DataFrame(a = 1:3, b = c("foo", "bar", "baz"))
#'
#' df$columns
#' df$dtypes
#' df$schema
#' df$shape
#' df$height
#' df$width
pl__DataFrame <- function(...) {
  list2(...) |>
    as_polars_df()
}

# The env for storing dataframe methods
polars_dataframe__methods <- new.env(parent = emptyenv())

#' @export
wrap.PlRDataFrame <- function(x, ...) {
  self <- new.env(parent = emptyenv())
  self$`_df` <- x

  # TODO: flags
  makeActiveBinding("columns", function() self$`_df`$columns(), self)
  makeActiveBinding("dtypes", function() {
    self$`_df`$dtypes() |>
      lapply(\(x) .savvy_wrap_PlRDataType(x) |> wrap())
  }, self)
  makeActiveBinding("schema", function() structure(self$dtypes, names = self$columns), self)
  makeActiveBinding("shape", function() self$`_df`$shape(), self)
  makeActiveBinding("height", function() self$`_df`$height(), self)
  makeActiveBinding("width", function() self$`_df`$width(), self)

  lapply(names(polars_dataframe__methods), function(name) {
    fn <- polars_dataframe__methods[[name]]
    environment(fn) <- environment()
    assign(name, fn, envir = self)
  })

  class(self) <- c("polars_data_frame", "polars_object")
  self
}

dataframe__set_column_names <- function(names) {
  wrap({
    df <- self$clone()

    df$`_df`$set_column_names(names)
    df
  })
}

# TODO: link to data type docs
#' Convert a DataFrame to a Series of type Struct
#'
#' @param name A character. Name for the struct [Series].
#' @return A [Series] of the struct type
#' @seealso
#' - [as_polars_series()]
#' @examples
#' df <- pl$DataFrame(
#'   a = 1:5,
#'   b = c("one", "two", "three", "four", "five"),
#' )
#' df$to_struct("nums")
dataframe__to_struct <- function(name = "") {
  self$`_df`$to_struct(name) |>
    wrap()
}

#' Convert an existing DataFrame to a LazyFrame
#' @description Start a new lazy query from a DataFrame.
#'
#' @inherit as_polars_lf return
#' @examples
#' pl$DataFrame(iris)$lazy()
dataframe__lazy <- function() {
  self$`_df`$lazy() |>
    wrap()
}

#' Clone a DataFrame
#'
#' This makes a very cheap deep copy/clone of an existing
#' [`DataFrame`][DataFrame_class]. Rarely useful as `DataFrame`s are nearly 100%
#' immutable. Any modification of a `DataFrame` should lead to a clone anyways,
#' but this can be useful when dealing with attributes (see examples).
#'
#' @inherit as_polars_df return
#' @examples
#' df1 <- pl$DataFrame(iris)
#'
#' # Make a function to take a DataFrame, add an attribute, and return a DataFrame
#' give_attr <- function(data) {
#'   attr(data, "created_on") <- "2024-01-29"
#'   data
#' }
#' df2 <- give_attr(df1)
#'
#' # Problem: the original DataFrame also gets the attribute while it shouldn't!
#' attributes(df1)
#'
#' # Use $clone() inside the function to avoid that
#' give_attr <- function(data) {
#'   data <- data$clone()
#'   attr(data, "created_on") <- "2024-01-29"
#'   data
#' }
#' df1 <- pl$DataFrame(iris)
#' df2 <- give_attr(df1)
#'
#' # now, the original DataFrame doesn't get this attribute
#' attributes(df1)
dataframe__clone <- function() {
  self$`_df`$clone() |>
    wrap()
}

# TODO-REWRITE: fix link to Series
#' Get the DataFrame as a List of Series
#'
#' @return A [list] of [Series]
#' @seealso
#' - [`as.list(<polars_data_frame>)`][as.list.polars_data_frame]
#' @examples
#' df <- pl$DataFrame(foo = c(1, 2, 3), bar = c(4, 5, 6))
#' df$get_columns()
#'
#' df <- pl$DataFrame(
#'   a = 1:4,
#'   b = c(0.5, 4, 10, 13),
#'   c = c(TRUE, TRUE, FALSE, TRUE)
#' )
#' df$get_columns()
dataframe__get_columns <- function() {
  self$`_df`$get_columns() |>
    lapply(\(ptr) {
      .savvy_wrap_PlRSeries(ptr) |>
        wrap()
    })
}

#' Group a DataFrame
#' @inheritParams LazyFrame_group_by
#' @inherit LazyFrame_group_by description params
#' @details Within each group, the order of the rows is always preserved,
#' regardless of the `maintain_order` argument.
#' @return [GroupBy][GroupBy_class] (a DataFrame with special groupby methods like `$agg()`)
#' @seealso
#' - [`<DataFrame>$partition_by()`][DataFrame_partition_by]
#' @examples
#' df <- pl$DataFrame(
#'   a = c("a", "b", "a", "b", "c"),
#'   b = c(1, 2, 1, 3, 3),
#'   c = c(5, 4, 3, 2, 1)
#' )
#'
#' df$group_by("a")$agg(pl$col("b")$sum())
#'
#' # Set `maintain_order = TRUE` to ensure the order of the groups is consistent with the input.
#' df$group_by("a", maintain_order = TRUE)$agg(pl$col("c"))
#'
#' # Group by multiple columns by passing a list of column names.
#' df$group_by(c("a", "b"))$agg(pl$max("c"))
#'
#' # Or pass some arguments to group by multiple columns in the same way.
#' # Expressions are also accepted.
#' df$group_by("a", pl$col("b") %/% 2)$agg(
#'   pl$col("c")$mean()
#' )
#'
#' # The columns will be renamed to the argument names.
#' df$group_by(d = "a", e = pl$col("b") %/% 2)$agg(
#'   pl$col("c")$mean()
#' )
dataframe__group_by <- function(..., maintain_order = FALSE) {
  wrap_to_group_by(self, list2(...), maintain_order)
}

#' Select and modify columns of a DataFrame
#' @description Similar to `dplyr::mutate()`. However, it discards unmentioned
#' columns (like `.()` in `data.table`).
#'
#' @param ... Columns to keep. Those can be expressions (e.g `pl$col("a")`),
#' column names  (e.g `"a"`), or list containing expressions or column names
#' (e.g `list(pl$col("a"))`).
#'
#' @inherit as_polars_df return
#' @examples
#' pl$DataFrame(iris)$select(
#'   pl$col("Sepal.Length")$abs()$alias("abs_SL"),
#'   (pl$col("Sepal.Length") + 2)$alias("add_2_SL")
#' )
dataframe__select <- function(...) {
  self$lazy()$select(...)$collect(`_eager` = TRUE) |>
    wrap()
}

#' Modify/append column(s)
#'
#' Add columns or modify existing ones with expressions. This is
#' the equivalent of `dplyr::mutate()` as it keeps unmentioned columns (unlike
#' `$select()`).
#'
#' @param ... Any expressions or string column name, or same wrapped in a list.
#' If first and only element is a list, it is unwrapped as a list of args.
#' @inherit as_polars_df return
#' @examples
#' pl$DataFrame(iris)$with_columns(
#'   pl$col("Sepal.Length")$abs()$alias("abs_SL"),
#'   (pl$col("Sepal.Length") + 2)$alias("add_2_SL")
#' )
#'
#' # same query
#' l_expr <- list(
#'   pl$col("Sepal.Length")$abs()$alias("abs_SL"),
#'   (pl$col("Sepal.Length") + 2)$alias("add_2_SL")
#' )
#' pl$DataFrame(iris)$with_columns(l_expr)
#'
#' pl$DataFrame(iris)$with_columns(
#'   pl$col("Sepal.Length")$abs(), # not named expr will keep name "Sepal.Length"
#'   SW_add_2 = (pl$col("Sepal.Width") + 2)
#' )
dataframe__with_columns <- function(...) {
  self$lazy()$with_columns(...)$collect(`_eager` = TRUE) |>
    wrap()
}

#' Get column by index
#'
#' @description Extract a DataFrame column (by index) as a Polars series. Unlike
#' `get_column()`, this method will not fail but will return a `NULL` if the
#' index doesn't exist in the DataFrame. Keep in mind that Polars is 0-indexed
#' so "0" is the first column.
#'
#' @param idx Index of the column to return as Series. Defaults to 0, which is
#' the first column.
#'
#' @return Series or NULL
#' @examples
#' df <- pl$DataFrame(iris[1:10, ])
#'
#' # default is to extract the first column
#' df$to_series()
#'
#' # Polars is 0-indexed, so we use idx = 1 to extract the *2nd* column
#' df$to_series(idx = 1)
#'
#' # doesn't error if the column isn't there
#' df$to_series(idx = 8)
dataframe__to_series <- function(index = 0) {
  self$`_df`$to_series(index) |>
    wrap()
}

#' Compare two DataFrames
#'
#' @description Check if two DataFrames are equal.
#'
#' @param other DataFrame to compare with.
#' @return A logical value
#' @examples
#' dat1 <- pl$DataFrame(iris)
#' dat2 <- pl$DataFrame(iris)
#' dat3 <- pl$DataFrame(mtcars)
#' dat1$equals(dat2)
#' dat1$equals(dat3)
dataframe__equals <- function(other, ..., null_equal = TRUE) {
  wrap({
    check_dots_empty0(...)
    check_polars_df(other)

    self$`_df`$equals(other$`_df`, null_equal)
  })
}

#' @title Slice
#' @description Get a slice of the DataFrame.
#' @inherit as_polars_df return
#' @param offset Start index, can be a negative value. This is 0-indexed, so
#' `offset = 1` doesn't include the first row.
#' @param length Length of the slice. If `NULL` (default), all rows starting at
#' the offset will be selected.
#' @examples
#' # skip the first 2 rows and take the 4 following rows
#' pl$DataFrame(mtcars)$slice(2, 4)
#'
#' # this is equivalent to:
#' mtcars[3:6, ]
dataframe__slice <- function(offset, length = NULL) {
  wrap({
    check_number_decimal(offset, allow_infinite = FALSE)
    if (isTRUE(length < 0)) {
      length <- self$height - offset + length
    }
    self$`_df`$slice(offset, length)
  })
}

#' @inherit LazyFrame_head title details
#' @param n Number of rows to return. If a negative value is passed,
#' return all rows except the last [`abs(n)`][abs].
#' @return A [DataFrame][DataFrame_class]
#' @examples
#' df <- pl$DataFrame(foo = 1:5, bar = 6:10, ham = letters[1:5])
#'
#' df$head(3)
#'
#' # Pass a negative value to get all rows except the last `abs(n)`.
#' df$head(-3)
dataframe__head <- function(n = 5) {
  wrap({
    if (isTRUE(n < 0)) {
      n <- max(0, self$height + n)
    }
    self$`_df`$head(n)
  })
}

#' @inherit LazyFrame_tail title
#' @param n Number of rows to return. If a negative value is passed,
#' return all rows except the first [`abs(n)`][abs].
#' @inherit DataFrame_head return
#' @examples
#' df <- pl$DataFrame(foo = 1:5, bar = 6:10, ham = letters[1:5])
#'
#' df$tail(3)
#'
#' # Pass a negative value to get all rows except the first `abs(n)`.
#' df$tail(-3)
dataframe__tail <- function(n = 5) {
  wrap({
    if (isTRUE(n < 0)) {
      n <- max(0, self$height + n)
    }
    self$`_df`$tail(n)
  })
}

#' Drop columns of a DataFrame
#'
#' @param ... Characters of column names to drop. Passed to [`pl$col()`][pl_col].
#' @param strict Validate that all column names exist in the schema and throw an
#' exception if a column name does not exist in the schema.
#'
#' @inherit as_polars_df return
#' @examples
#' pl$DataFrame(mtcars)$drop(c("mpg", "hp"))
#'
#' # equivalent
#' pl$DataFrame(mtcars)$drop("mpg", "hp")
dataframe__drop <- function(..., strict = TRUE) {
  self$lazy()$drop(..., strict = strict)$collect(`_eager` = TRUE) |>
    wrap()
}

# TODO: accept formulas for type mapping
#' Cast DataFrame column(s) to the specified dtype
#'
#' @inherit LazyFrame_cast description params
#'
#'
#' @inherit as_polars_df return
#' @examples
#' df <- pl$DataFrame(
#'   foo = 1:3,
#'   bar = c(6, 7, 8),
#'   ham = as.Date(c("2020-01-02", "2020-03-04", "2020-05-06"))
#' )
#'
#' # Cast only some columns
#' df$cast(list(foo = pl$Float32, bar = pl$UInt8))
#'
#' # Cast all columns to the same type
#' df$cast(pl$String)
dataframe__cast <- function(..., strict = TRUE) {
  self$lazy()$cast(..., strict = strict)$collect(`_eager` = TRUE) |>
    wrap()
}

#' Filter rows of a DataFrame
#'
#' @inherit LazyFrame_filter description params details
#'
#' @inherit as_polars_df return
#' @examples
#' df <- pl$DataFrame(iris)
#'
#' df$filter(pl$col("Sepal.Length") > 5)
#'
#' # This is equivalent to
#' # df$filter(pl$col("Sepal.Length") > 5 & pl$col("Petal.Width") < 1)
#' df$filter(pl$col("Sepal.Length") > 5, pl$col("Petal.Width") < 1)
#'
#' # rows where condition is NA are dropped
#' iris2 <- iris
#' iris2[c(1, 3, 5), "Species"] <- NA
#' df <- pl$DataFrame(iris2)
#'
#' df$filter(pl$col("Species") == "setosa")
dataframe__filter <- function(...) {
  self$lazy()$filter(...)$collect(`_eager` = TRUE) |>
    wrap()
}

#' Sort a DataFrame
#' @inherit LazyFrame_sort details description params
#' @inheritParams DataFrame_unique
#' @inherit as_polars_df return
#' @examples
#' df <- mtcars
#' df$mpg[1] <- NA
#' df <- pl$DataFrame(df)
#' df$sort("mpg")
#' df$sort("mpg", nulls_last = TRUE)
#' df$sort("cyl", "mpg")
#' df$sort(c("cyl", "mpg"))
#' df$sort(c("cyl", "mpg"), descending = TRUE)
#' df$sort(c("cyl", "mpg"), descending = c(TRUE, FALSE))
#' df$sort(pl$col("cyl"), pl$col("mpg"))
dataframe__sort <- function(
    ...,
    descending = FALSE,
    nulls_last = FALSE,
    multithreaded = TRUE,
    maintain_order = FALSE) {
  self$lazy()$sort(
    ...,
    descending = descending,
    nulls_last = nulls_last,
    multithreaded = multithreaded,
    maintain_order = maintain_order
  )$collect(`_eager` = TRUE) |>
    wrap()
}


#' Return Polars DataFrame as R data.frame
#'
#' @param ... Any args pased to `as.data.frame()`.
#' @param int64_conversion How should Int64 values be handled when converting a
#' polars object to R?
#'
#' * `"double"` (default) converts the integer values to double.
#' * `"bit64"` uses `bit64::as.integer64()` to do the conversion (requires
#'   the package `bit64` to be attached).
#' * `"string"` converts Int64 values to character.
#'
#' @return An R data.frame
#' @inheritSection DataFrame_class Conversion to R data types considerations
#' @keywords DataFrame
#' @examples
#' df <- pl$DataFrame(iris[1:3, ])
#' df$to_data_frame()
DataFrame_to_data_frame <- function(..., int64_conversion = polars_options()$int64_conversion) {
  # do not unnest structs and mark with I to also preserve categoricals as is
  l <- lapply(
    self$to_list(unnest_structs = FALSE, int64_conversion = int64_conversion),
    function(x) {
      # correctly handle columns with datatype Null
      if (is.null(x)) {
        NA
      } else {
        I(x)
      }
    }
  )

  # similar to as.data.frame, but avoid checks, which would edit structs
  df <- data.frame(seq_along(l[[1L]]), ...)
  for (i in seq_along(l)) df[[i]] <- l[[i]]
  names(df) <- .pr$DataFrame$columns(self)

  # remove AsIs (I) subclass from columns
  df[] <- lapply(df, unAsIs)
  df
}


#' Return Polars DataFrame as a list of vectors
#'
#' @param unnest_structs Logical. If `TRUE` (default), then `$unnest()` is applied
#' on any struct column.
#' @inheritParams DataFrame_to_data_frame
#'
#' @details
#' For simplicity reasons, this implementation relies on unnesting all structs
#' before exporting to R. If `unnest_structs = FALSE`, then `struct` columns
#' will be returned as nested lists, where each row is a list of values. Such a
#' structure is not very typical or efficient in R.
#'
#' @return R list of vectors
#' @inheritSection DataFrame_class Conversion to R data types considerations
#' @seealso
#' - [`<DataFrame>$get_columns()`][DataFrame_get_columns]:
#'   Similar to this method but returns a list of [Series][Series_class] instead of vectors.
#' @examples
#' pl$DataFrame(iris)$to_list()
DataFrame_to_list <- function(unnest_structs = TRUE, ..., int64_conversion = polars_options()$int64_conversion) {
  if (unnest_structs) {
    .pr$DataFrame$to_list(self, int64_conversion) |>
      unwrap("in $to_list():")
  } else {
    .pr$DataFrame$to_list_tag_structs(self, int64_conversion) |>
      unwrap("in $to_list():") |>
      restruct_list()
  }
}

#' Join DataFrames
#'
#' @param other DataFrame to join with.
#' @inherit LazyFrame_join description params
#'
#' @return DataFrame
#' @keywords DataFrame
#' @examples
#' # inner join by default
#' df1 <- pl$DataFrame(list(key = 1:3, payload = c("f", "i", NA)))
#' df2 <- pl$DataFrame(list(key = c(3L, 4L, 5L, NA_integer_)))
#' df1$join(other = df2, on = "key")
#'
#' # cross join
#' df1 <- pl$DataFrame(x = letters[1:3])
#' df2 <- pl$DataFrame(y = 1:4)
#' df1$join(other = df2, how = "cross")
DataFrame_join <- function(
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
  if (!is_polars_df(other)) {
    Err_plain("`other` must be a DataFrame.") |>
      unwrap("in $join():")
  }
  other <- other$lazy()
  .args <- as.list(environment())
  do.call(.pr$DataFrame$lazy(self)$join, .args)$collect()
}

#' Convert DataFrame to a Series of type "struct"
#' @param name Name given to the new Series
#' @return A Series of type "struct"
#' @aliases to_struct
#' @keywords DataFrame
#' @examples
#' # round-trip conversion from DataFrame with two columns
#' df <- pl$DataFrame(a = 1:5, b = c("one", "two", "three", "four", "five"))
#' s <- df$to_struct()
#' s
#'
#' # convert to an R list
#' s$to_r()
#'
#' # Convert back to a DataFrame
#' df_s <- s$to_frame()
#' df_s
DataFrame_to_struct <- function(name = "") {
  unwrap(.pr$DataFrame$to_struct(self, name), "in $to_struct():")
}


#' Unnest the Struct columns of a DataFrame
#'
#' @param ... Names of the struct columns to unnest. This doesn't accept Expr.
#' If nothing is provided, then all columns of datatype [Struct][DataType_Struct]
#' are unnested.
#'
#' @return A DataFrame where some or all columns of datatype Struct are unnested.
#' @examples
#' df <- pl$DataFrame(
#'   a = 1:5,
#'   b = c("one", "two", "three", "four", "five"),
#'   c = 6:10
#' )$
#'   select(
#'   pl$struct("b"),
#'   pl$struct(c("a", "c"))$alias("a_and_c")
#' )
#' df
#'
#' # by default, all struct columns are unnested
#' df$unnest()
#'
#' # we can specify specific columns to unnest
#' df$unnest("a_and_c")
DataFrame_unnest <- function(...) {
  columns <- unpack_list(..., .context = "in $unnest():")
  if (length(columns) == 0) {
    columns <- names(which(dtypes_are_struct(.pr$DataFrame$schema(self))))
  } else {
    columns <- unlist(columns)
  }
  unwrap(.pr$DataFrame$unnest(self, columns), "in $unnest():")
}



#' @title Get the first row of the DataFrame.
#' @keywords DataFrame
#' @return A DataFrame with one row.
#' @examples pl$DataFrame(mtcars)$first()
DataFrame_first <- function() {
  self$lazy()$first()$collect()
}


#' @title Number of chunks of the Series in a DataFrame
#' @description
#' Number of chunks (memory allocations) for all or first Series in a DataFrame.
#' @details
#' A DataFrame is a vector of Series. Each Series in rust-polars is a wrapper
#' around a ChunkedArray, which is like a virtual contiguous vector physically
#' backed by an ordered set of chunks. Each chunk of values has a contiguous
#' memory layout and is an arrow array. Arrow arrays are a fast, thread-safe and
#' cross-platform memory layout.
#'
#' In R, combining with `c()` or `rbind()` requires immediate vector re-allocation
#' to place vector values in contiguous memory. This is slow and memory consuming,
#' and it is why repeatedly appending to a vector in R is discouraged.
#'
#' In polars, when we concatenate or append to Series or DataFrame, the
#' re-allocation can be avoided or delayed by simply appending chunks to each
#' individual Series. However, if chunks become many and small or are misaligned
#' across Series, this can hurt the performance of subsequent operations.
#'
#' Most places in the polars api where chunking could occur, the user have to
#' typically actively opt-out by setting an argument `rechunk = FALSE`.
#'
#' @keywords DataFrame
#' @param strategy Either `"all"` or `"first"`. `"first"` only returns chunks
#' for the first Series.
#' @return A real vector of chunk counts per Series.
#' @seealso [`<DataFrame>$rechunk()`][DataFrame_rechunk]
#' @examples
#' # create DataFrame with misaligned chunks
#' df <- pl$concat(
#'   1:10, # single chunk
#'   pl$concat(1:5, 1:5, rechunk = FALSE, how = "vertical")$rename("b"), # two chunks
#'   how = "horizontal"
#' )
#' df
#' df$n_chunks()
#'
#' # rechunk a chunked DataFrame
#' df$rechunk()$n_chunks()
#'
#' # rechunk is not an in-place operation
#' df$n_chunks()
#'
#' # The following toy example emulates the Series "chunkyness" in R. Here it a
#' # S3-classed list with same type of vectors and where have all relevant S3
#' # generics implemented to make behave as if it was a regular vector.
#' "+.chunked_vector" <- \(x, y) structure(list(unlist(x) + unlist(y)), class = "chunked_vector")
#' print.chunked_vector <- \(x, ...) print(unlist(x), ...)
#' c.chunked_vector <- \(...) {
#'   structure(do.call(c, lapply(list(...), unclass)), class = "chunked_vector")
#' }
#' rechunk <- \(x) structure(unlist(x), class = "chunked_vector")
#' x <- structure(list(1:4, 5L), class = "chunked_vector")
#' x
#' x + 5:1
#' lapply(x, tracemem) # trace chunks to verify no re-allocation
#' z <- c(x, x)
#' z # looks like a plain vector
#' lapply(z, tracemem) # mem allocation  in z are the same from x
#' str(z)
#' z <- rechunk(z)
#' str(z)
DataFrame_n_chunks <- function(strategy = "first") {
  .pr$DataFrame$n_chunks(self, strategy) |>
    unwrap("in n_chunks():")
}


#' @title Rechunk a DataFrame
#' @description Rechunking re-allocates any "chunked" memory allocations to
#' speed-up e.g. vectorized operations.
#' @inherit DataFrame_n_chunks details examples
#'
#' @keywords DataFrame
#' @return A DataFrame
#' @seealso [`<DataFrame>$n_chunks()`][DataFrame_n_chunks]
DataFrame_rechunk <- function() {
  .pr$DataFrame$rechunk(self)
}


#' @title Get the last row of the DataFrame.
#' @keywords DataFrame
#' @return A DataFrame with one row.
#' @examples pl$DataFrame(mtcars)$last()
DataFrame_last <- function() {
  self$lazy()$last()$collect()
}

#' @title Max
#' @description Aggregate the columns in the DataFrame to their maximum value.
#' @keywords DataFrame
#' @return A DataFrame with one row.
#' @examples pl$DataFrame(mtcars)$max()
DataFrame_max <- function() {
  self$lazy()$max()$collect()
}

#' @title Mean
#' @description Aggregate the columns in the DataFrame to their mean value.
#' @keywords DataFrame
#' @return A DataFrame with one row.
#' @examples pl$DataFrame(mtcars)$mean()
DataFrame_mean <- function() {
  self$lazy()$mean()$collect()
}

#' @title Median
#' @description Aggregate the columns in the DataFrame to their median value.
#' @keywords DataFrame
#' @return A DataFrame with one row.
#' @examples pl$DataFrame(mtcars)$median()
DataFrame_median <- function() {
  self$lazy()$median()$collect()
}

#' @title Min
#' @description Aggregate the columns in the DataFrame to their minimum value.
#' @keywords DataFrame
#' @return A DataFrame with one row.
#' @examples pl$DataFrame(mtcars)$min()
DataFrame_min <- function() {
  self$lazy()$min()$collect()
}

#' @title Sum
#' @description Aggregate the columns of this DataFrame to their sum values.
#' @keywords DataFrame
#' @return A DataFrame with one row.
#' @examples pl$DataFrame(mtcars)$sum()
DataFrame_sum <- function() {
  self$lazy()$sum()$collect()
}

#' @title Var
#' @description Aggregate the columns of this DataFrame to their variance values.
#' @keywords DataFrame
#' @param ddof Delta Degrees of Freedom: the divisor used in the calculation is
#' N - ddof, where N represents the number of elements. By default ddof is 1.
#' @return A DataFrame with one row.
#' @examples pl$DataFrame(mtcars)$var()
DataFrame_var <- function(ddof = 1) {
  self$lazy()$var(ddof)$collect()
}

#' @title Std
#' @description Aggregate the columns of this DataFrame to their standard
#' deviation values.
#' @keywords DataFrame
#' @param ddof Delta Degrees of Freedom: the divisor used in the calculation is
#' N - ddof, where N represents the number of elements. By default ddof is 1.
#' @return A DataFrame with one row.
#' @examples pl$DataFrame(mtcars)$std()
DataFrame_std <- function(ddof = 1) {
  self$lazy()$std(ddof)$collect()
}

#' @title Quantile
#' @description Aggregate the columns in the DataFrame to a unique quantile
#' value. Use `$describe()` to specify several quantiles.
#' @keywords DataFrame
#' @param quantile Numeric of length 1 between 0 and 1.
#' @inheritParams Expr_quantile
#' @return DataFrame
#' @examples pl$DataFrame(mtcars)$quantile(.4)
DataFrame_quantile <- function(quantile, interpolation = "nearest") {
  self$lazy()$quantile(quantile, interpolation)$collect()
}

#' @title Reverse
#' @description Reverse the DataFrame (the last row becomes the first one, etc.).
#' @return DataFrame
#' @examples pl$DataFrame(mtcars)$reverse()
DataFrame_reverse <- function() {
  self$lazy()$reverse()$collect()
}

#' @inherit Expr_fill_nan title params
#'
#' @return DataFrame
#' @examples
#' df <- pl$DataFrame(
#'   a = c(1.5, 2, NaN, 4),
#'   b = c(1.5, NaN, NaN, 4)
#' )
#' df$fill_nan(99)
DataFrame_fill_nan <- function(value) {
  self$lazy()$fill_nan(value)$collect()
}

#' @title Fill nulls
#' @description Fill null values (which correspond to `NA` in R) using the
#' specified value or strategy.
#' @keywords DataFrame
#' @param fill_value Value to fill nulls with.
#' @return DataFrame
#' @examples
#' df <- pl$DataFrame(
#'   a = c(1.5, 2, NA, 4),
#'   b = c(1.5, NA, NA, 4)
#' )
#'
#' df$fill_null(99)
#'
#' df$fill_null(pl$col("a")$mean())
DataFrame_fill_null <- function(fill_value) {
  self$lazy()$fill_null(fill_value)$collect()
}

#' @title Slice
#' @description Get a slice of the DataFrame.
#' @return DataFrame
#' @param offset Start index, can be a negative value. This is 0-indexed, so
#' `offset = 1` doesn't include the first row.
#' @param length Length of the slice. If `NULL` (default), all rows starting at
#' the offset will be selected.
#' @examples
#' # skip the first 2 rows and take the 4 following rows
#' pl$DataFrame(mtcars)$slice(2, 4)
#'
#' # this is equivalent to:
#' mtcars[3:6, ]
DataFrame_slice <- function(offset, length = NULL) {
  self$lazy()$slice(offset, length)$collect()
}


#' @title Count null values
#' @description Create a new DataFrame that shows the null (which correspond
#' to `NA` in R) counts per column.
#' @keywords DataFrame
#' @return DataFrame
#' @docType NULL
#' @format NULL
#' @format function
#' @examples
#' x <- mtcars
#' x[1, 2:3] <- NA
#' pl$DataFrame(x)$null_count()
DataFrame_null_count <- use_extendr_wrapper


#' @title Estimated size
#' @description Return an estimation of the total (heap) allocated size of the
#' DataFrame.
#' @keywords DataFrame
#' @return Estimated size in bytes
#' @docType NULL
#' @format NULL
#' @format function
#' @examples
#' pl$DataFrame(mtcars)$estimated_size()
DataFrame_estimated_size <- use_extendr_wrapper



#' Perform joins on nearest keys
#' @inherit LazyFrame_join_asof
#' @inheritSection polars_duration_string  Polars duration string language
#' @param other DataFrame or LazyFrame
#' @return New joined DataFrame
#' @examples
#' # create two DataFrames to join asof
#' gdp <- pl$DataFrame(
#'   date = as.Date(c("2015-1-1", "2016-1-1", "2017-5-1", "2018-1-1", "2019-1-1")),
#'   gdp = c(4321, 4164, 4411, 4566, 4696),
#'   group = c("b", "a", "a", "b", "b")
#' )
#'
#' pop <- pl$DataFrame(
#'   date = as.Date(c("2016-5-12", "2017-5-12", "2018-5-12", "2019-5-12")),
#'   population = c(82.19, 82.66, 83.12, 83.52),
#'   group = c("b", "b", "a", "a")
#' )
#'
#' # optional make sure tables are already sorted with "on" join-key
#' gdp <- gdp$sort("date")
#' pop <- pop$sort("date")
#'
#' # Left-join_asof DataFrame pop with gdp on "date"
#' # Look backward in gdp to find closest matching date
#' pop$join_asof(gdp, on = "date", strategy = "backward")
#'
#' # .... and forward
#' pop$join_asof(gdp, on = "date", strategy = "forward")
#'
#' # join by a group: "only look within within group"
#' pop$join_asof(gdp, on = "date", by = "group", strategy = "backward")
#'
#' # only look 2 weeks and 2 days back
#' pop$join_asof(gdp, on = "date", strategy = "backward", tolerance = "2w2d")
#'
#' # only look 11 days back (numeric tolerance depends on polars type, <date> is in days)
#' pop$join_asof(gdp, on = "date", strategy = "backward", tolerance = 11)
DataFrame_join_asof <- function(
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
  # convert other to LazyFrame, capture any Error as a result, and pass it on

  other_df_result <- pcase(
    inherits(other, "RPolarsDataFrame"), Ok(other$lazy()),
    inherits(other, "RPolarsLazyFrame"), Ok(other),
    or_else = Err(" not a LazyFrame or DataFrame")
  )

  self$lazy()$join_asof(
    other = other_df_result,
    on = on,
    left_on = left_on,
    right_on = right_on,
    by = by,
    by_left = by_left,
    by_right = by_right,
    allow_parallel = allow_parallel,
    force_parallel = force_parallel,
    suffix = suffix,
    strategy = strategy,
    tolerance = tolerance,
    coalesce = coalesce
  )$collect()
}




#' @inherit LazyFrame_unpivot
#' @keywords DataFrame
#'
#' @return A new `DataFrame`
#'
#' @examples
#' df <- pl$DataFrame(
#'   a = c("x", "y", "z"),
#'   b = c(1, 3, 5),
#'   c = c(2, 4, 6),
#'   d = c(7, 8, 9)
#' )
#' df$unpivot(index = "a", on = c("b", "c", "d"))
DataFrame_unpivot <- function(
    on = NULL,
    ...,
    index = NULL,
    variable_name = NULL,
    value_name = NULL) {
  .pr$DataFrame$unpivot(
    self, on %||% character(), index %||% character(),
    value_name, variable_name
  ) |> unwrap("in $unpivot( ): ")
}



#' Pivot data from long to wide
#' @param values Column values to aggregate. Can be multiple columns if the
#' `on` arguments contains multiple columns as well.
#' @param index  One or multiple keys to group by.
#' @param on  Name of the column(s) whose values will be used as the header
#' of the output DataFrame.
#' @param ... Not used.
#' @param aggregate_function One of:
#' - string indicating the expressions to aggregate with, such as 'first',
#'   'sum', 'max', 'min', 'mean', 'median', 'last', 'count'),
#' - an Expr e.g. `pl$element()$sum()`
#' @param maintain_order Sort the grouped keys so that the output order is
#' predictable.
#' @param sort_columns Sort the transposed columns by name. Default is by order
#' of discovery.
#' @param separator Used as separator/delimiter in generated column names.
#'
#' @return DataFrame
#' @keywords DataFrame
#' @examples
#' df <- pl$DataFrame(
#'   foo = c("one", "one", "one", "two", "two", "two"),
#'   bar = c("A", "B", "C", "A", "B", "C"),
#'   baz = c(1, 2, 3, 4, 5, 6)
#' )
#' df
#'
#' df$pivot(
#'   values = "baz", index = "foo", on = "bar"
#' )
#'
#' # Run an expression as aggregation function
#' df <- pl$DataFrame(
#'   col1 = c("a", "a", "a", "b", "b", "b"),
#'   col2 = c("x", "x", "x", "x", "y", "y"),
#'   col3 = c(6, 7, 3, 2, 5, 7)
#' )
#' df
#'
#' df$pivot(
#'   index = "col1",
#'   on = "col2",
#'   values = "col3",
#'   aggregate_function = pl$element()$tanh()$mean()
#' )
DataFrame_pivot <- function(
    on,
    ...,
    index,
    values,
    aggregate_function = NULL,
    maintain_order = TRUE,
    sort_columns = FALSE,
    separator = "_") {
  pcase(
    # if string, call it on Expr-method of pl$element() and capture any Error as Result
    is_string(aggregate_function), result(`$.RPolarsExpr`(pl$element(), aggregate_function)()),

    # Expr or NULL pass as is
    is.null(aggregate_function) || inherits(aggregate_function, "RPolarsExpr"), Ok(aggregate_function),

    # anything else pass err
    or_else = Err(" is neither a string, NULL or an Expr")
  ) |>
    # add param context
    map_err(\(err_msg) paste(
      "param [aggregate_function] being ", str_string(aggregate_function), err_msg
    )) |>
    # run pivot when valid aggregate_expr
    and_then(\(aggregate_expr) .pr$DataFrame$pivot_expr(
      self, on, index, values, maintain_order, sort_columns, aggregate_expr, separator
    )) |>
    # unwrap and add method context name
    unwrap("in $pivot():")
}

#' Rename column names of a DataFrame
#' @inherit pl_DataFrame return
#' @inherit LazyFrame_rename params details
#' @examples
#' df <- pl$DataFrame(
#'   foo = 1:3,
#'   bar = 6:8,
#'   ham = letters[1:3]
#' )
#'
#' df$rename(foo = "apple")
#'
#' df$rename(
#'   \(column_name) paste0("c", substr(column_name, 2, 100))
#' )
DataFrame_rename <- function(...) {
  self$lazy()$rename(...)$collect()
}

#' @title Summary statistics for a DataFrame
#'
#' @description This returns the total number of rows, the number of missing
#' values, the mean, standard deviation, min, max, median and the percentiles
#' specified in the argument `percentiles`.
#'
#' @param percentiles One or more percentiles to include in the summary statistics.
#' All values must be in the range `[0; 1]`.
#' @param interpolation Interpolation method for computing quantiles. One of
#'  `"nearest"`, `"higher"`, `"lower"`, `"midpoint"`, or `"linear"`.
#' @keywords DataFrame
#' @return DataFrame
#' @examples
#' pl$DataFrame(iris)$describe()
#'
#' # string, date, boolean columns are also supported:
#' df <- pl$DataFrame(
#'   int = 1:3,
#'   string = c(letters[1:2], NA),
#'   date = c(as.Date("2024-01-20"), as.Date("2024-01-21"), NA),
#'   cat = factor(c(letters[1:2], NA)),
#'   bool = c(TRUE, FALSE, NA)
#' )
#' df
#'
#' df$describe()
DataFrame_describe <- function(percentiles = c(0.25, 0.75), interpolation = "nearest") {
  uw <- \(res) unwrap(res, "in $describe():")

  if (length(self$columns) == 0) {
    Err_plain("cannot describe a DataFrame without any columns") |>
      uw()
  }

  result(msg = "internal error", {
    # Determine which columns should get std/mean/percentile statistics
    stat_cols <- lapply(self$schema, \(x) {
      is_num <- lapply(pl$numeric_dtypes, \(w) w == x) |>
        unlist() |>
        any()
      if (!is_num) {
        return()
      } else {
        is_num
      }
    }) |>
      unlist() |>
      names()

    # separator used temporarily and used to split the column names later on
    # It's voluntarily weird so that it doesn't match actual column names
    custom_sep <- "??-??"

    # Determine metrics and optional/additional percentiles
    if (!0.5 %in% percentiles) {
      percentiles <- c(percentiles, 0.5)
    }
    metrics <- c("count", "null_count", "mean", "std", "min")
    percentile_exprs <- list()
    for (p in sort(percentiles)) {
      for (c in self$columns) {
        expr <- if (c %in% stat_cols) {
          pl$col(c)$quantile(p, interpolation = interpolation)
        } else {
          pl$lit(NA)
        }
        expr <- expr$alias(paste0(p, custom_sep, c))
        percentile_exprs <- append(percentile_exprs, expr)
      }
      metrics <- append(metrics, paste0(p * 100, "%"))
    }
    metrics <- append(metrics, "max")

    mean_exprs <- lapply(self$columns, function(x) {
      expr <- if (x %in% stat_cols) {
        pl$col(x)$mean()
      } else {
        pl$lit(NA)
      }
      expr$alias(paste0("mean", custom_sep, x))
    })

    std_exprs <- lapply(self$columns, function(x) {
      expr <- if (x %in% stat_cols) {
        pl$col(x)$std()
      } else {
        pl$lit(NA)
      }
      expr$alias(paste0("std", custom_sep, x))
    })

    min_exprs <- lapply(self$columns, function(x) {
      pl$col(x)$min()$alias(paste0("min", custom_sep, x))
    })

    max_exprs <- lapply(self$columns, function(x) {
      pl$col(x)$max()$alias(paste0("max", custom_sep, x))
    })

    # Calculate metrics in parallel
    df_metrics <- self$
      select(
      unlist(
        list(
          pl$all()$count()$name$prefix(paste0("count", custom_sep)),
          pl$all()$null_count()$name$prefix(paste0("null_count", custom_sep)),
          mean_exprs,
          std_exprs,
          min_exprs,
          percentile_exprs,
          max_exprs
        ),
        recursive = FALSE
      )
    )

    df_metrics$
      transpose(include_header = TRUE)$
      with_columns(
      pl$col("column")$str$split_exact(custom_sep, 1)$
        struct$rename_fields(c("statistic", "variable"))$
        alias("fields")
    )$
      unnest("fields")$
      drop("column")$
      pivot(index = "statistic", on = "variable", values = "column_0")$
      with_columns(statistic = pl$lit(metrics))
  }) |>
    uw()
}

#' Show a dense preview of the DataFrame
#'
#' The formatting shows one line per column so that wide DataFrames display
#' cleanly. Each line shows the column name, the data type, and the first few
#' values.
#'
#' @param ... Ignored.
#' @param max_items_per_column Maximum number of items to show per column.
#' @param max_colname_length Maximum length of the displayed column names. Values
#' that exceed this value are truncated with a trailing ellipsis.
#' @param return_as_string Logical (default `FALSE`). If `TRUE`, return the
#' output as a string.
#'
#' @return DataFrame
#' @examples
#' pl$DataFrame(iris)$glimpse()
DataFrame_glimpse <- function(
    ...,
    max_items_per_column = 10,
    max_colname_length = 50,
    return_as_string = FALSE) {
  if (!is_scalar_bool(return_as_string)) {
    Err_plain("`return_as_string` must be `TRUE` or `FALSE`.") |>
      unwrap("in $glimpse() :")
  }

  max_num_value <- min(max_items_per_column, self$height)

  parse_column_ <- \(col_name, dtype) {
    dtype_str <- dtype_str_repr(dtype) |> unwrap_or(paste0("??", str_string(dtype)))
    if (inherits(dtype, "RPolarsDataType")) dtype_str <- paste0(" <", dtype_str, ">")
    val <- self$select(pl$col(col_name)$slice(0, max_num_value))$to_list()[[1]]
    val_str <- paste(val, collapse = ", ")
    if (nchar(col_name) > max_colname_length) {
      col_name <- paste0(substr(col_name, 1, max_colname_length - 3), "...")
    }
    list(
      col_name = col_name,
      dtype_str = dtype_str,
      val_str = val_str
    )
  }

  # construct print, flag any error as internal
  output <- result(
    {
      schema <- self$schema
      data <- lapply(seq_along(schema), \(i) parse_column_(names(schema)[i], schema[[i]]))
      max_col_name <- max(sapply(data, \(x) nchar(x$col_name)))
      max_col_dtyp <- max(sapply(data, \(x) nchar(x$dtype)))
      max_col_vals <- 100 - max_col_name - max_col_dtyp - 3

      sapply(data, \(x) {
        name_filler <- paste(rep(" ", max_col_name - nchar(x$col_name)), collapse = "")
        dtyp_filler <- paste(rep(" ", max_col_dtyp - nchar(x$dtype_str)), collapse = "")
        vals_filler <- paste(rep(" ", max_col_dtyp - nchar(x$dtype_str)), collapse = "")
        paste0(
          "& ", x$col_name, name_filler, x$dtype_str, dtyp_filler, " ",
          substr(x$val_str, 1, max_col_vals), "\n"
        )
      }) |>
        paste0(collapse = "")
    },
    msg = "internal error"
  ) |>
    unwrap("in $glimpse() :")

  if (return_as_string) output else invisible(cat(output))
}


#' @inherit LazyFrame_explode title params
#'
#' @keywords DataFrame
#' @return DataFrame
#' @examples
#' df <- pl$DataFrame(
#'   letters = letters[1:4],
#'   numbers = list(1, c(2, 3), c(4, 5), c(6, 7, 8)),
#'   numbers_2 = list(0, c(1, 2), c(3, 4), c(5, 6, 7)) # same structure as numbers
#' )
#' df
#'
#' # explode a single column, append others
#' df$explode("numbers")
#'
#' # explode two columns of same nesting structure, by names or the common dtype
#' # "List(Float64)"
#' df$explode("numbers", "numbers_2")
#' df$explode(pl$col(pl$List(pl$Float64)))
DataFrame_explode <- function(...) {
  self$lazy()$explode(...)$collect()
}

#' Take a sample of rows from a DataFrame
#'
#' @param n Number of rows to return. Cannot be used with `fraction`.
#' @param ... Ignored.
#' @param fraction Fraction of rows to return. Cannot be used with `n`. Can be
#' larger than 1 if `with_replacement` is `TRUE`.
#' @param with_replacement Allow values to be sampled more than once.
#' @param shuffle If `TRUE`, the order of the sampled rows will be shuffled. If
#' `FALSE` (default), the order of the returned rows will be neither stable nor
#' fully random.
#' @param seed Seed for the random number generator. If set to `NULL` (default),
#' a random seed is generated for each sample operation.
#'
#' @keywords DataFrame
#' @return DataFrame
#' @examples
#' df <- pl$DataFrame(iris)
#' df$sample(n = 20)
#' df$sample(fraction = 0.1)
DataFrame_sample <- function(n = NULL, ..., fraction = NULL, with_replacement = FALSE, shuffle = FALSE, seed = NULL) {
  seed <- seed %||% sample(0:10000, 1)
  pcase(
    !xor(is.null(n), is.null(fraction)), Err_plain("Pass either arg `n` or `fraction`, not both."),
    is.null(fraction), .pr$DataFrame$sample_n(self, n, with_replacement, shuffle, seed),
    is.null(n), .pr$DataFrame$sample_frac(self, fraction, with_replacement, shuffle, seed),
    or_else = Err_plain("internal error")
  ) |>
    unwrap("in $sample():")
}


#' Transpose a DataFrame over the diagonal.
#'
#' @param include_header If `TRUE`, the column names will be added as first column.
#' @param header_name If `include_header` is `TRUE`, this determines the name of the column
#' that will be inserted.
#' @param column_names Character vector indicating the new column names. If `NULL` (default),
#' the columns will be named as "column_1", "column_2", etc. The length of this vector must match
#' the number of rows of the original input.
#'
#' @details
#' This is a very expensive operation.
#'
#' Transpose may be the fastest option to perform non foldable (see `fold()` or `reduce()`)
#' row operations like median.
#'
#' Polars transpose is currently eager only, likely because it is not trivial to deduce the schema.
#'
#' @keywords DataFrame
#' @return DataFrame
#' @examples
#'
#' # simple use-case
#' pl$DataFrame(mtcars)$transpose(include_header = TRUE, column_names = rownames(mtcars))
#'
#' # All rows must have one shared supertype, recast Categorical to String which is a supertype
#' # of f64, and then dataset "Iris" can be transposed
#' pl$DataFrame(iris)$with_columns(pl$col("Species")$cast(pl$String))$transpose()
#'
DataFrame_transpose <- function(
    include_header = FALSE,
    header_name = "column",
    column_names = NULL) {
  keep_names_as <- if (isTRUE(include_header)) header_name else NULL
  .pr$DataFrame$transpose(self, keep_names_as, column_names) |>
    unwrap("in $transpose():")
}

#' Write to comma-separated values (CSV) file
#'
#' @param file File path to which the result should be written.
#' @param ... Ignored.
#' @param include_bom Whether to include UTF-8 BOM (byte order mark) in the CSV
#' output.
#' @param include_header Whether to include header in the CSV output.
#' @param separator Separate CSV fields with this symbol.
#' @param line_terminator String used to end each row.
#' @param quote_char Byte to use as quoting character.
#' @param batch_size Number of rows that will be processed per thread.
#' @param datetime_format A format string, with the specifiers defined by the
#' chrono Rust crate. If no format specified, the default fractional-second
#' precision is inferred from the maximum timeunit found in the frame’s Datetime
#'  cols (if any).
#' @param date_format A format string, with the specifiers defined by the chrono
#' Rust crate.
#' @param time_format A format string, with the specifiers defined by the chrono
#' Rust crate.
#' @param float_precision Number of decimal places to write, applied to both
#' Float32 and Float64 datatypes.
#' @param null_values A string representing null values (defaulting to the empty
#' string).
#' @param quote_style Determines the quoting strategy used.
#' * `"necessary"` (default): This puts quotes around fields only when necessary.
#'   They are necessary when fields contain a quote, delimiter or record
#'   terminator. Quotes are also necessary when writing an empty record (which
#'   is indistinguishable from a record with one empty field). This is the
#'   default.
#' * `"always"`: This puts quotes around every field.
#' * `"non_numeric"`: This puts quotes around all fields that are non-numeric.
#'   Namely, when writing a field that does not parse as a valid float or integer,
#'   then quotes will be used even if they aren`t strictly necessary.
#' * `"never"`: This never puts quotes around fields, even if that results in
#'   invalid CSV data (e.g. by not quoting strings containing the separator).
#'
#' @return Invisibly returns the input DataFrame.
#'
#' @rdname IO_write_csv
#'
#' @examples
#' dat <- pl$DataFrame(mtcars)
#'
#' destination <- tempfile(fileext = ".csv")
#' dat$select(pl$col("drat", "mpg"))$write_csv(destination)
#'
#' pl$read_csv(destination)
DataFrame_write_csv <- function(
    file,
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
    null_values = "",
    quote_style = "necessary") {
  .pr$DataFrame$write_csv(
    self,
    file, include_bom, include_header, separator, line_terminator, quote_char,
    batch_size, datetime_format, date_format, time_format, float_precision,
    null_values, quote_style
  ) |>
    unwrap("in $write_csv():")

  invisible(self)
}


#' Write to Arrow IPC file (a.k.a Feather file)
#'
#' @inherit DataFrame_write_csv params return
#' @inheritParams LazyFrame_sink_ipc
#' @param compat_level Use a specific compatibility level when exporting Polars’
#' internal data structures. This can be:
#' * an integer indicating the compatibility version (currently only 0 for oldest
#'   and 1 for newest);
#' * a logical value with `TRUE` for the newest version and `FALSE` for the oldest
#'   version.
#'
#' @rdname IO_write_ipc
#' @seealso
#' - [`<DataFrame>$to_raw_ipc()`][DataFrame_to_raw_ipc]
#' @examples
#' dat <- pl$DataFrame(mtcars)
#'
#' destination <- tempfile(fileext = ".arrow")
#' dat$write_ipc(destination)
#'
#' if (require("arrow", quietly = TRUE)) {
#'   arrow::read_ipc_file(destination, as_data_frame = FALSE)
#' }
DataFrame_write_ipc <- function(
    file,
    compression = c("uncompressed", "zstd", "lz4"),
    ...,
    compat_level = TRUE) {
  .pr$DataFrame$write_ipc(
    self,
    file,
    compression %||% "uncompressed",
    compat_level
  ) |>
    unwrap("in $write_ipc():")

  invisible(self)
}


#' Write to parquet file
#'
#' @inherit DataFrame_write_csv params return
#' @inheritParams LazyFrame_sink_parquet
#' @param file File path to which the result should be written. This should be
#' a path to a directory if writing a partitioned dataset.
#' @param partition_by Column(s) to partition by. A partitioned dataset will be
#' written if this is specified.
#' @param partition_chunk_size_bytes Approximate size to split DataFrames within
#' a single partition when writing. Note this is calculated using the size of
#' the DataFrame in memory - the size of the output file may differ depending
#' on the file format / compression.
#'
#' @rdname IO_write_parquet
#'
#' @examplesIf requireNamespace("withr", quietly = TRUE)
#' dat <- pl$DataFrame(mtcars)
#'
#' # write data to a single parquet file
#' destination <- withr::local_tempfile(fileext = ".parquet")
#' dat$write_parquet(destination)
#'
#' # write data to folder with a hive-partitioned structure
#' dest_folder <- withr::local_tempdir()
#' dat$write_parquet(dest_folder, partition_by = c("gear", "cyl"))
#' list.files(dest_folder, recursive = TRUE)
DataFrame_write_parquet <- function(
    file,
    ...,
    compression = "zstd",
    compression_level = 3,
    statistics = TRUE,
    row_group_size = NULL,
    data_page_size = NULL,
    partition_by = NULL,
    partition_chunk_size_bytes = 4294967296) {
  statistics <- translate_statistics(statistics) |>
    unwrap("in $write_parquet():")
  .pr$DataFrame$write_parquet(
    self,
    file,
    compression = compression,
    compression_level = compression_level,
    statistics = statistics,
    row_group_size = row_group_size,
    data_page_size = data_page_size,
    partition_by = partition_by,
    partition_chunk_size_bytes = partition_chunk_size_bytes
  ) |>
    unwrap("in $write_parquet():")

  invisible(self)
}

#' Write to JSON file
#'
#' @inherit DataFrame_write_csv params return
#' @param pretty Pretty serialize JSON.
#' @param row_oriented Write to row-oriented JSON. This is slower, but more
#' common.
#'
#' @rdname IO_write_json
#'
#' @examples
#' if (require("jsonlite", quiet = TRUE)) {
#'   dat <- pl$DataFrame(head(mtcars))
#'   destination <- tempfile()
#'
#'   dat$select(pl$col("drat", "mpg"))$write_json(destination)
#'   jsonlite::fromJSON(destination)
#'
#'   dat$select(pl$col("drat", "mpg"))$write_json(destination, row_oriented = TRUE)
#'   jsonlite::fromJSON(destination)
#' }
DataFrame_write_json <- function(
    file,
    ...,
    pretty = FALSE,
    row_oriented = FALSE) {
  .pr$DataFrame$write_json(self, file, pretty, row_oriented) |>
    unwrap("in $write_json():")

  invisible(self)
}

#' Write to NDJSON file
#'
#' @inherit DataFrame_write_csv return
#' @inheritParams DataFrame_write_json
#'
#' @rdname IO_write_ndjson
#'
#' @examples
#' dat <- pl$DataFrame(head(mtcars))
#'
#' destination <- tempfile()
#' dat$select(pl$col("drat", "mpg"))$write_ndjson(destination)
#'
#' pl$read_ndjson(destination)
DataFrame_write_ndjson <- function(file) {
  .pr$DataFrame$write_ndjson(self, file) |>
    unwrap("in $write_ndjson():")

  invisible(self)
}

#' @inherit LazyFrame_rolling title description params details
#' @return A [RollingGroupBy][RollingGroupBy_class] object
#' @seealso
#' - [`<DataFrame>$group_by_dynamic()`][DataFrame_group_by_dynamic]
#' @inheritSection polars_duration_string  Polars duration string language
#' @examples
#' date <- c(
#'   "2020-01-01 13:45:48",
#'   "2020-01-01 16:42:13",
#'   "2020-01-01 16:45:09",
#'   "2020-01-02 18:12:48",
#'   "2020-01-03 19:45:32",
#'   "2020-01-08 23:16:43"
#' )
#' df <- pl$DataFrame(dt = date, a = c(3, 7, 5, 9, 2, 1))$with_columns(
#'   pl$col("dt")$str$strptime(pl$Datetime())$set_sorted()
#' )
#'
#' df$rolling(index_column = "dt", period = "2d")$agg(
#'   sum_a = pl$sum("a"),
#'   min_a = pl$min("a"),
#'   max_a = pl$max("a")
#' )
DataFrame_rolling <- function(
    index_column,
    ...,
    period,
    offset = NULL,
    closed = "right",
    group_by = NULL) {
  period <- parse_as_polars_duration_string(period)
  offset <- parse_as_polars_duration_string(offset) %||% negate_duration_string(period)
  construct_rolling_group_by(self, index_column, period, offset, closed, group_by)
}

#' @inherit LazyFrame_group_by_dynamic title description details params
#' @return A [GroupBy][GroupBy_class] object
#' @seealso
#' - [`<DataFrame>$rolling()`][DataFrame_rolling]
#' @examples
#' df <- pl$DataFrame(
#'   time = pl$datetime_range(
#'     start = strptime("2021-12-16 00:00:00", format = "%Y-%m-%d %H:%M:%S", tz = "UTC"),
#'     end = strptime("2021-12-16 03:00:00", format = "%Y-%m-%d %H:%M:%S", tz = "UTC"),
#'     interval = "30m"
#'   ),
#'   n = 0:6
#' )
#'
#' # get the sum in the following hour relative to the "time" column
#' df$group_by_dynamic("time", every = "1h")$agg(
#'   vals = pl$col("n"),
#'   sum = pl$col("n")$sum()
#' )
#'
#' # using "include_boundaries = TRUE" is helpful to see the period considered
#' df$group_by_dynamic("time", every = "1h", include_boundaries = TRUE)$agg(
#'   vals = pl$col("n")
#' )
#'
#' # in the example above, the values didn't include the one *exactly* 1h after
#' # the start because "closed = 'left'" by default.
#' # Changing it to "right" includes values that are exactly 1h after. Note that
#' # the value at 00:00:00 now becomes included in the interval [23:00:00 - 00:00:00],
#' # even if this interval wasn't there originally
#' df$group_by_dynamic("time", every = "1h", closed = "right")$agg(
#'   vals = pl$col("n")
#' )
#' # To keep both boundaries, we use "closed = 'both'". Some values now belong to
#' # several groups:
#' df$group_by_dynamic("time", every = "1h", closed = "both")$agg(
#'   vals = pl$col("n")
#' )
#'
#' # Dynamic group bys can also be combined with grouping on normal keys
#' df <- df$with_columns(
#'   groups = as_polars_series(c("a", "a", "a", "b", "b", "a", "a"))
#' )
#' df
#'
#' df$group_by_dynamic(
#'   "time",
#'   every = "1h",
#'   closed = "both",
#'   group_by = "groups",
#'   include_boundaries = TRUE
#' )$agg(pl$col("n"))
#'
#' # We can also create a dynamic group by based on an index column
#' df <- pl$LazyFrame(
#'   idx = 0:5,
#'   A = c("A", "A", "B", "B", "B", "C")
#' )$with_columns(pl$col("idx")$set_sorted())
#' df
#'
#' df$group_by_dynamic(
#'   "idx",
#'   every = "2i",
#'   period = "3i",
#'   include_boundaries = TRUE,
#'   closed = "right"
#' )$agg(A_agg_list = pl$col("A"))
DataFrame_group_by_dynamic <- function(
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
  construct_group_by_dynamic(
    self, index_column, every, period, offset, include_boundaries, closed, label,
    group_by, start_by
  )
}


#' Split a DataFrame into multiple DataFrames
#'
#' Similar to [`$group_by()`][DataFrame_group_by].
#' Group by the given columns and return the groups as separate [DataFrames][DataFrame_class].
#' It is useful to use this in combination with functions like [lapply()] or `purrr::map()`.
#' @param ... Characters of column names to group by. Passed to [`pl$col()`][pl_col].
#' @param maintain_order If `TRUE`, ensure that the order of the groups is consistent with the input data.
#' This is slower than a default partition by operation.
#' @param include_key If `TRUE`, include the columns used to partition the DataFrame in the output.
#' @param as_nested_list This affects the format of the output.
#' If `FALSE` (default), the output is a flat [list] of [DataFrames][DataFrame_class].
#' IF `TRUE` and one of the `maintain_order` or `include_key` argument is `TRUE`,
#' then each element of the output has two children: `key` and `data`.
#' See the examples for more details.
#' @return A list of [DataFrames][DataFrame_class]. See the examples for details.
#' @seealso
#' - [`<DataFrame>$group_by()`][DataFrame_group_by]
#' @examples
#' df <- pl$DataFrame(
#'   a = c("a", "b", "a", "b", "c"),
#'   b = c(1, 2, 1, 3, 3),
#'   c = c(5, 4, 3, 2, 1)
#' )
#' df
#'
#' # Pass a single column name to partition by that column.
#' df$partition_by("a")
#'
#' # Partition by multiple columns.
#' df$partition_by("a", "b")
#'
#' # Partition by column data type
#' df$partition_by(pl$String)
#'
#' # If `as_nested_list = TRUE`, the output is a list whose elements have a `key` and a `data` field.
#' # The `key` is a named list of the key values, and the `data` is the DataFrame.
#' df$partition_by("a", "b", as_nested_list = TRUE)
#'
#' # `as_nested_list = TRUE` should be used with `maintain_order = TRUE` or `include_key = TRUE`.
#' tryCatch(
#'   df$partition_by("a", "b", maintain_order = FALSE, include_key = FALSE, as_nested_list = TRUE),
#'   warning = function(w) w
#' )
#'
#' # Example of using with lapply(), and printing the key and the data summary
#' df$partition_by("a", "b", maintain_order = FALSE, as_nested_list = TRUE) |>
#'   lapply(\(x) {
#'     sprintf("\nThe key value of `a` is %s and the key value of `b` is %s\n", x$key$a, x$key$b) |>
#'       cat()
#'     x$data$drop(names(x$key))$describe() |>
#'       print()
#'     invisible(NULL)
#'   }) |>
#'   invisible()
DataFrame_partition_by <- function(
    ...,
    maintain_order = TRUE,
    include_key = TRUE,
    as_nested_list = FALSE) {
  uw <- \(res) unwrap(res, "in $partition_by():")

  by <- result(dots_to_colnames(self, ...)) |>
    uw()

  if (!length(by)) {
    Err_plain("There is no column to partition by.") |>
      uw()
  }

  partitions <- .pr$DataFrame$partition_by(self, by, maintain_order, include_key) |>
    uw()

  if (isTRUE(as_nested_list)) {
    if (include_key) {
      out <- lapply(seq_along(partitions), \(index) {
        data <- partitions[[index]]
        key <- data$select(by)$head(1)$to_list()

        list(key = key, data = data)
      })

      return(out)
    } else if (maintain_order) {
      key_df <- self$select(by)$unique(maintain_order = TRUE)
      out <- lapply(seq_along(partitions), \(index) {
        data <- partitions[[index]]
        key <- key_df$slice(index - 1, 1)$to_list()

        list(key = key, data = data)
      })

      return(out)
    } else {
      warning(
        "cannot use `$partition_by` with ",
        "`maintain_order = FALSE, include_key = FALSE, as_nested_list = TRUE`. ",
        "Fall back to a flat list."
      )
    }
  }

  partitions
}


#' Return the element at the given row/column.
#'
#' If row and column location are not specified, the [DataFrame][DataFrame_class]
#' must have dimensions (1, 1).
#'
#' @param row Optional row index (0-indexed).
#' @param column Optional column index (0-indexed) or name.
#'
#' @return A value of length 1
#'
#' @examples
#' df <- pl$DataFrame(a = c(1, 2, 3), b = c(4, 5, 6))
#'
#' df$select((pl$col("a") * pl$col("b"))$sum())$item()
#'
#' df$item(1, 1)
#'
#' df$item(2, "b")
DataFrame_item <- function(row = NULL, column = NULL) {
  uw <- \(res) unwrap(res, "in $item():")

  row_null <- is.null(row)
  col_null <- is.null(column)

  if (row_null && col_null) {
    if (!identical(self$shape, c(1, 1))) {
      Err_plain(
        "Can only call $item() if the DataFrame is of shape (1, 1) or if explicit row/col values are provided."
      ) |> uw()
    }
    out <- .pr$DataFrame$select_at_idx(self, 0) |>
      uw() |>
      as.vector()
    return(out)
  }

  if ((!row_null && col_null) || (row_null && !col_null)) {
    Err_plain("Cannot call `$item()` with only one of `row` or `column`.") |>
      uw()
  }

  if (is.numeric(column)) {
    column <- self$columns[column + 1]
    if (is.na(column)) {
      Err_plain("`column` is out of bounds.") |>
        uw()
    }
  } else if (is.character(column)) {
    if (!column %in% self$columns) {
      Err_plain("`column` does not exist.") |>
        uw()
    }
  }

  out <- self$get_column(column)[row + 1]$to_r()
  if (length(out) == 0) {
    Err_plain("`row` is out of bounds.") |>
      uw()
  }

  out
}


#' Create an empty or n-row null-filled copy of the DataFrame
#'
#' Returns a n-row null-filled DataFrame with an identical schema. `n` can be
#' greater than the current number of rows in the DataFrame.
#'
#' @param n Number of (null-filled) rows to return in the cleared frame.
#'
#' @return A n-row null-filled DataFrame with an identical schema
#'
#' @examples
#' df <- pl$DataFrame(
#'   a = c(NA, 2, 3, 4),
#'   b = c(0.5, NA, 2.5, 13),
#'   c = c(TRUE, TRUE, FALSE, NA)
#' )
#'
#' df$clear()
#'
#' df$clear(n = 5)
DataFrame_clear <- function(n = 0) {
  if (length(n) > 1 || !is.numeric(n) || n < 0) {
    Err_plain("`n` must be an integer greater or equal to 0.") |>
      unwrap("in $clear():")
  }

  if (n == 0) {
    out <- .pr$DataFrame$clear(self) |>
      unwrap("in $clear():")
  }

  if (n > 0) {
    series <- lapply(seq_along(self$schema), function(x) {
      pl$Series(name = names(self$schema)[x], dtype = self$schema[[x]])$extend_constant(NA, n)
    })
    out <- pl$DataFrame(series)
  }

  out
}


# TODO: we can't use % in the SQL query
# <https://github.com/r-lib/roxygen2/issues/1616>
#' Execute a SQL query against the DataFrame
#'
#' @inherit LazyFrame_sql description details params seealso
#' @inherit pl_DataFrame return
#' @examplesIf polars_info()$features$sql
#' df1 <- pl$DataFrame(
#'   a = 1:3,
#'   b = c("zz", "yy", "xx"),
#'   c = as.Date(c("1999-12-31", "2010-10-10", "2077-08-08"))
#' )
#'
#' # Query the DataFrame using SQL:
#' df1$sql("SELECT c, b FROM self WHERE a > 1")
#'
#' # Join two DataFrames using SQL.
#' df2 <- pl$DataFrame(a = 3:1, d = c(125, -654, 888))
#' df1$sql(
#'   "
#' SELECT self.*, d
#' FROM self
#' INNER JOIN df2 USING (a)
#' WHERE a > 1 AND EXTRACT(year FROM c) < 2050
#' "
#' )
#'
#' # Apply transformations to a DataFrame using SQL, aliasing "self" to "frame".
#' df1$sql(
#'   query = r"(
#' SELECT
#' a,
#' MOD(a, 2) == 0 AS a_is_even,
#' CONCAT_WS(':', b, b) AS b_b,
#' EXTRACT(year FROM c) AS year,
#' 0::float AS 'zero'
#' FROM frame
#' )",
#'   table_name = "frame"
#' )
DataFrame_sql <- function(query, ..., table_name = NULL, envir = parent.frame()) {
  self$lazy()$sql(
    query,
    table_name = table_name,
    envir = envir
  )$collect() |>
    result() |>
    unwrap("in $sql():")
}


#' Take every nth row in the DataFrame
#'
#' @inheritParams LazyFrame_gather_every
#'
#' @return A DataFrame
#'
#' @examples
#' df <- pl$DataFrame(a = 1:4, b = 5:8)
#' df$gather_every(2)
#'
#' df$gather_every(2, offset = 1)
DataFrame_gather_every <- function(n, offset = 0) {
  self$select(pl$col("*")$gather_every(n, offset))
}


#' Cast DataFrame column(s) to the specified dtype
#'
#' @inherit LazyFrame_cast description params
#'
#' @return A DataFrame
#'
#' @examples
#' df <- pl$DataFrame(
#'   foo = 1:3,
#'   bar = c(6, 7, 8),
#'   ham = as.Date(c("2020-01-02", "2020-03-04", "2020-05-06"))
#' )
#'
#' # Cast only some columns
#' df$cast(list(foo = pl$Float32, bar = pl$UInt8))
#'
#' # Cast all columns to the same type
#' df$cast(pl$String)
DataFrame_cast <- function(dtypes, ..., strict = TRUE) {
  self$lazy()$cast(dtypes, strict = strict)$collect()
}


#' Convert variables into dummy/indicator variables
#'
#' @param columns Column name(s) or selector(s) that should be converted to
#' dummy variables. If `NULL` (default), convert all columns.
#' @param ... Ignored.
#' @param separator Separator/delimiter used when generating column names.
#' @param drop_first Remove the first category from the variables being encoded.
#'
#' @return A DataFrame
#'
#' @examples
#' df <- pl$DataFrame(foo = 1:2, bar = 3:4, ham = c("a", "b"))
#'
#' df$to_dummies()
#'
#' df$to_dummies(drop_first = TRUE)
#'
#' df$to_dummies(c("foo", "bar"), separator = "::")
DataFrame_to_dummies <- function(
    columns = NULL,
    ...,
    separator = "_",
    drop_first = FALSE) {
  if (is.null(columns)) {
    columns <- names(self)
  }
  .pr$DataFrame$to_dummies(self, columns = columns, separator = separator, drop_first = drop_first) |>
    unwrap("in $to_dummies():")
}

#' @inherit LazyFrame_join_where title params
#'
#' @description
#' This performs an inner join, so only rows where all predicates are true are
#' included in the result, and a row from either DataFrame may be included
#' multiple times in the result.
#'
#' Note that the row order of the input DataFrames is not preserved.
#'
#' @param other DataFrame to join with.
#'
#' @return A DataFrame
#'
#' @examples
#' east <- pl$DataFrame(
#'   id = c(100, 101, 102),
#'   dur = c(120, 140, 160),
#'   rev = c(12, 14, 16),
#'   cores = c(2, 8, 4)
#' )
#'
#' west <- pl$DataFrame(
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
#' )
DataFrame_join_where <- function(
    other,
    ...,
    suffix = "_right") {
  if (!is_polars_df(other)) {
    Err_plain("`other` must be a DataFrame.") |> unwrap()
  }
  self$lazy()$join_where(other = other$lazy(), ..., suffix = suffix)$collect()
}

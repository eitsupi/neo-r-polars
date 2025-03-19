# The env for storing rolling_group_by methods
polars_sql_context__methods <- new.env(parent = emptyenv())

#' @export
wrap.PlRSQLContext <- function(
  x,
  frames = NULL,
  register_globals = FALSE
) {
  self <- new.env(parent = emptyenv())
  self$`_ctxt` <- x
  self$frames <- frames
  self$register_globals <- register_globals

  class(self) <- c("polars_sql_context", "polars_object")
  self
}

pl__SQLContext <- function(frames = NULL, register_globals = FALSE) {
  PlRSQLContext$new() |>
    wrap()
}

ensure_lazyframe <- function(obj) {
  if (is_polars_df(obj)) {
    obj$lazy()
  } else if (is_polars_lf(obj)) {
    obj
  } else if (is_convertible_to_polars_series(obj)) {
    as_polars_series(obj)$to_frame()$lazy()
  } else {
    abort("Cannot convert object to LazyFrame")
  }
}

sql_context__register <- function(name, frame = NULL) {
  wrap({
    frame <- if (!is.null(frame)) {
      ensure_lazyframe(frame)
    } else {
      pl$LazyFrame()
    }
    self$`_ctxt`$register(name, frame$`_ldf`)
    self
  })
}

#' Parse the given SQL query and execute it against the registered frame data
#'
#' @param query A valid string SQL query.
#'
#' @inherit as_polars_lf return
#' @examples
#' # Declare frame data and register with a SQLContext:
#' df <- pl$DataFrame(
#'   title = c(
#'     "The Godfather",
#'     "The Dark Knight",
#'     "Schindler's List",
#'     "Pulp Fiction",
#'     "The Shawshank Redemption"
#'   ),
#'   release_year = c(1972, 2008, 1993, 1994, 1994),
#'   budget = c(6 * 1e6, 185 * 1e6, 22 * 1e6, 8 * 1e6, 25 * 1e6),
#'   gross = c(134821952, 533316061, 96067179, 107930000, 28341469),
#'   imdb_score = c(9.2, 9, 8.9, 8.9, 9.3)
#' )
#'
# TODO: this should be pl$SQLContext(films = df)
#' ctx <- pl$SQLContext()$register("films", df)
#' ctx$execute(
#'   "
#'      SELECT title, release_year, imdb_score
#'      FROM films
#'      WHERE release_year > 1990
#'      ORDER BY imdb_score DESC
#'      "
#' )$collect()
#'
#' # Execute a GROUP BY query:
#' ctx$execute(
#'   "
#'   SELECT
#'        MAX(release_year / 10) * 10 AS decade,
#'        SUM(gross) AS total_gross,
#'        COUNT(title) AS n_films,
#'   FROM films
#'   GROUP BY (release_year / 10) -- decade
#'   ORDER BY total_gross DESC
#'   "
#' )$collect()
sql_context__execute <- function(query) {
  wrap({
    self$`_ctxt`$execute(query)
  })
}

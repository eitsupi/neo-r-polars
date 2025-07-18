% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/lazyframe-frame.R
\name{lazyframe__profile}
\alias{lazyframe__profile}
\title{Collect and profile a lazy query}
\usage{
lazyframe__profile(
  ...,
  type_coercion = TRUE,
  `_type_check` = TRUE,
  predicate_pushdown = TRUE,
  projection_pushdown = TRUE,
  simplify_expression = TRUE,
  slice_pushdown = TRUE,
  comm_subplan_elim = TRUE,
  comm_subexpr_elim = TRUE,
  cluster_with_columns = TRUE,
  collapse_joins = TRUE,
  no_optimization = FALSE,
  `_check_order` = TRUE,
  show_plot = FALSE,
  truncate_nodes = 0
)
}
\arguments{
\item{...}{These dots are for future extensions and must be empty.}

\item{type_coercion}{A logical, indicates type coercion optimization.}

\item{predicate_pushdown}{A logical, indicates predicate pushdown optimization.}

\item{projection_pushdown}{A logical, indicates projection pushdown optimization.}

\item{simplify_expression}{A logical, indicates simplify expression optimization.}

\item{slice_pushdown}{A logical, indicates slice pushdown optimization.}

\item{comm_subplan_elim}{A logical, indicates trying to cache branching subplans that occur
on self-joins or unions.}

\item{comm_subexpr_elim}{A logical, indicates trying to cache common subexpressions.}

\item{cluster_with_columns}{A logical, indicates to combine sequential independent calls
to with_columns.}

\item{collapse_joins}{Collapse a join and filters into a faster join.}

\item{no_optimization}{A logical. If \code{TRUE}, turn off (certain) optimizations.}

\item{_check_order, _type_check}{For internal use only.}

\item{show_plot}{Show a Gantt chart of the profiling result}

\item{truncate_nodes}{Truncate the label lengths in the Gantt chart to this
number of characters. If \code{0} (default), do not truncate.}
}
\value{
List of two \code{DataFrame}s: one with the collected result, the other
with the timings of each step. If \code{show_plot = TRUE}, then the plot is
also stored in the list.
}
\description{
This will run the query and return a list containing the
materialized DataFrame and a DataFrame that contains profiling information
of each node that is executed.
}
\details{
The units of the timings are microseconds.
}
\examples{
lf <- pl$LazyFrame(
  a = c("a", "b", "a", "b", "b", "c"),
  b = 1:6,
  c = 6:1,
)

lf$group_by("a", .maintain_order = TRUE)$agg(
  pl$all()$sum()
)$sort("a")$profile()
}
\seealso{
\itemize{
\item \code{\link[=lazyframe__collect]{$collect()}} - regular collect.
\item \code{\link[=lazyframe__sink_parquet]{$sink_parquet()}} streams query to a parquet file.
\item \code{\link[=lazyframe__sink_ipc]{$sink_ipc()}} streams query to a arrow file.
}
}

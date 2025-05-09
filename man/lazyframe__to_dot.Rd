% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/lazyframe-frame.R
\name{lazyframe__to_dot}
\alias{lazyframe__to_dot}
\title{Plot the query plan}
\usage{
lazyframe__to_dot(
  ...,
  optimized = TRUE,
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
  streaming = FALSE,
  `_check_order` = TRUE
)
}
\arguments{
\item{...}{Not used..}

\item{optimized}{Optimize the query plan.}

\item{type_coercion}{A logical, indicats type coercion optimization.}

\item{predicate_pushdown}{A logical, indicats predicate pushdown optimization.}

\item{projection_pushdown}{A logical, indicats projection pushdown optimization.}

\item{simplify_expression}{A logical, indicats simplify expression optimization.}

\item{slice_pushdown}{A logical, indicats slice pushdown optimization.}

\item{comm_subplan_elim}{A logical, indicats tring to cache branching subplans that occur
on self-joins or unions.}

\item{comm_subexpr_elim}{A logical, indicats tring to cache common subexpressions.}

\item{cluster_with_columns}{A logical, indicats to combine sequential independent calls
to with_columns.}

\item{collapse_joins}{Collapse a join and filters into a faster join.}

\item{streaming}{\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#deprecated}{\figure{lifecycle-deprecated.svg}{options: alt='[Deprecated]'}}}{\strong{[Deprecated]}}
A logical. If \code{TRUE}, process the query in batches to handle larger-than-memory data.
If \code{FALSE} (default), the entire query is processed in a single batch.
Note that streaming mode is considered unstable.
It may be changed at any point without it being considered a breaking change.}

\item{_check_order, _type_check}{For internal use only.}
}
\value{
A character vector
}
\description{
This only returns the "dot" output that can be passed to other packages, such
as \code{DiagrammeR::grViz()}.
}
\examples{
lf <- pl$LazyFrame(
  a = c("a", "b", "a", "b", "b", "c"),
  b = 1:6,
  c = 6:1
)

query <- lf$group_by("a", .maintain_order = TRUE)$agg(
  pl$all()$sum()
)$sort("a")

query$to_dot() |> cat()

# You could print the graph by using DiagrammeR for example, with
# query$to_dot() |> DiagrammeR::grViz().
}

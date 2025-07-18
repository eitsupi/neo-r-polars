% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/expr-meta.R
\name{expr_meta_tree_format}
\alias{expr_meta_tree_format}
\title{Format the expression as a tree}
\usage{
expr_meta_tree_format(..., as_dot = FALSE, schema = NULL)
}
\arguments{
\item{...}{These dots are for future extensions and must be empty.}

\item{as_dot}{\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#experimental}{\figure{lifecycle-experimental.svg}{options: alt='[Experimental]'}}}{\strong{[Experimental]}}
If \code{TRUE}, show the dot syntax that can be used in other
packages, such as \code{DiagrammeR}.}

\item{schema}{An optional schema. Must be \code{NULL} or a named list of
\link{DataType}.}
}
\value{
A string, either with the tree itself (if \code{as_dot = FALSE}) or with the
corresponding GraphViz code (if \code{as_dot = TRUE}).
}
\description{
Format the expression as a tree
}
\examples{
my_expr <- (pl$col("foo") * pl$col("bar"))$sum()$over(pl$col("ham")) / 2
cat(my_expr$meta$tree_format())

\dontrun{
# This output can be displayed with DiagrammeR for instance
graph <- my_expr$meta$tree_format(as_dot = TRUE)
DiagrammeR::grViz(graph)
}
}

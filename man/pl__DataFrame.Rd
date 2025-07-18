% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dataframe-frame.R
\name{pl__DataFrame}
\alias{pl__DataFrame}
\alias{polars_data_frame}
\alias{DataFrame}
\title{Polars DataFrame class (\code{polars_data_frame})}
\usage{
pl__DataFrame(..., .schema_overrides = NULL, .strict = TRUE)
}
\arguments{
\item{...}{<\code{\link[rlang:dyn-dots]{dynamic-dots}}>
Name-value pairs of objects to be converted to polars \link{Series}
by the \code{\link[=as_polars_series]{as_polars_series()}} function.
Each \link{Series} will be used as a column of the \link{DataFrame}.
All values must be the same length or length 1.
Each name will be used as the column name. If the name is empty,
the original name of the \link{Series} will be used.}

\item{.schema_overrides}{\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#experimental}{\figure{lifecycle-experimental.svg}{options: alt='[Experimental]'}}}{\strong{[Experimental]}}
A list of polars data types or \code{NULL} (default).
Passed to the \code{\link[=dataframe__cast]{$cast()}} method as dynamic-dots.}

\item{.strict}{\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#experimental}{\figure{lifecycle-experimental.svg}{options: alt='[Experimental]'}}}{\strong{[Experimental]}}
A logical value. Passed to the \code{\link[=dataframe__cast]{$cast()}} method's \code{.strict} argument.}
}
\value{
A polars \link{DataFrame}
}
\description{
DataFrames are two-dimensional data structure representing data
as a table with rows and columns. Polars DataFrames are similar to
\link[=data.frame]{R Data Frames}. R Data Frame's columns are \link[=vector]{R vectors},
while Polars DataFrame's columns are \link[=Series]{Polars Series}.
}
\details{
The \code{pl$DataFrame()} function mimics the constructor of the DataFrame class of Python Polars.
This function is basically a shortcut for
\code{as_polars_df(list(...))$cast(!!!.schema_overrides, .strict = .strict)},
so each argument in \code{...} is converted to a Polars Series by \code{\link[=as_polars_series]{as_polars_series()}}
and then passed to \code{\link[=as_polars_df]{as_polars_df()}}.
}
\section{Active bindings}{

\itemize{
\item \code{columns}: \verb{$columns} returns a character vector with the names of the columns.
\item \code{dtypes}: \verb{$dtypes} returns a nameless list of the data type of each column.
\item \code{schema}: \verb{$schema} returns a named list with the column names as names
and the data types as values.
\item \code{shape}: \verb{$shape} returns a integer vector of length two with the number of rows
and columns of the DataFrame.
\item \code{height}: \verb{$height} returns a integer with the number of rows of the DataFrame.
\item \code{width}: \verb{$width} returns a integer with the number of columns of the DataFrame.
\item \code{flags}: \verb{$flags} returns a list with column names as names and a named
logical vector with the flags as values.
}
}

\section{Flags}{


Flags are used internally to avoid doing unnecessary computations, such as
sorting a variable that we know is already sorted. The number of flags
varies depending on the column type: columns of type \code{array} and \code{list}
have the flags \code{SORTED_ASC}, \code{SORTED_DESC}, and \code{FAST_EXPLODE}, while other
column types only have the former two.
\itemize{
\item \code{SORTED_ASC} is set to \code{TRUE} when we sort a column in increasing order, so
that we can use this information later on to avoid re-sorting it.
\item \code{SORTED_DESC} is similar but applies to sort in decreasing order.
}
}

\examples{
# Constructing a DataFrame from vectors:
pl$DataFrame(a = 1:2, b = 3:4)

# Constructing a DataFrame from Series:
pl$DataFrame(pl$Series("a", 1:2), pl$Series("b", 3:4))

# Constructing a DataFrame from a list:
data <- list(a = 1:2, b = 3:4)

## Using the as_polars_df function (recommended)
as_polars_df(data)

## Using dynamic dots feature
pl$DataFrame(!!!data)

# Active bindings:
df <- pl$DataFrame(a = 1:3, b = c("foo", "bar", "baz"))

df$columns
df$dtypes
df$schema
df$shape
df$height
df$width
}

% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dataframe-frame.R
\name{dataframe__transpose}
\alias{dataframe__transpose}
\title{Transpose a DataFrame over the diagonal}
\usage{
dataframe__transpose(
  ...,
  include_header = FALSE,
  header_name = "column",
  column_names = NULL
)
}
\arguments{
\item{...}{These dots are for future extensions and must be empty.}

\item{include_header}{If set, the column names will be added as first column.}

\item{header_name}{If \code{include_header} is set, this determines the name of
the column that will be inserted.}

\item{column_names}{Optional string naming an existing column, or a function
that takes an integer vector representing the position of value (non-header)
columns and returns a character vector of same length. Column position is
0-indexed.}
}
\value{
A polars \link{DataFrame}
}
\description{
Transpose a DataFrame over the diagonal
}
\details{
This is a very expensive operation. Perhaps you can do it differently.
}
\examples{
df <- pl$DataFrame(a = c(1, 2, 3), b = c(4, 5, 6))
df$transpose(include_header = TRUE)

# Replace the auto-generated column names with a list
df$transpose(include_header = FALSE, column_names = c("x", "y", "z"))

# Include the header as a separate column
df$transpose(
  include_header = TRUE, header_name = "foo", column_names = c("x", "y", "z")
)

# Use a function to produce the new column names
name_generator <- function(x) {
  paste0("my_column_", x)
}
df$transpose(include_header = FALSE, column_names = name_generator)

# Use an existing column as the new column names
df <- pl$DataFrame(id = c("i", "j", "k"), a = c(1, 2, 3), b = c(4, 5, 6))
df$transpose(column_names = "id")
df$transpose(include_header = TRUE, header_name = "new_id", column_names = "id")
}

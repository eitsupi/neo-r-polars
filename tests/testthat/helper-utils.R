# Workaround for R < 4.4
# https://github.com/eitsupi/neo-r-polars/pull/240
if (!exists("%||%", envir = baseenv())) {
  `%||%` <- rlang::`%||%`
}

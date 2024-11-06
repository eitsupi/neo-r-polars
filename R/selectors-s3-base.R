#' @export
`-.polars_selector` <- function(e1, e2) {
  check_polars_selector(e2)
  e1$sub(e2)
}

#' @export
`!.polars_selector` <- function(e) {
  e$invert()
}

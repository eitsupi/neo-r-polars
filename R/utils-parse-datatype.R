#' Parse dynamic dots into a list of datatypes
#' @noRd
parse_into_list_of_datatypes <- function(...) {
  dots <- list2(...)
  out <- lapply(dots, \(x) {
    if (!isTRUE(is_polars_dtype(x))) {
      abort(
        sprintf("Dynamic dots `...` must be polars data types, got %s", toString(class(x))),
        call = parent.frame(3L)
      )
    }
    x$`_dt`
  })

  # In pl$Struct(), we want to use the name given in pl$Field() as the name of
  # the corresponding list element
  field_idx <- c()
  field_nms <- c()
  for (i in seq_along(dots)) {
    if (!inherits(dots[[i]], "polars_dtype_field")) {
      next
    }
    field_idx <- c(field_idx, i)
    field_nms <- c(field_nms, dots[[i]]$`_nm`)
  }

  if (length(field_idx) == 0) {
    return(out)
  }

  names(out)[field_idx] <- field_nms
  out
}

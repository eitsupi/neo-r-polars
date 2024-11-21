Sys.setenv(POLARS_IN_LAZY_TEST = FALSE)

to_duplicate <- test_path("test-frame.R")

for (i in to_duplicate) {
  tmp <- readLines(i)
  out <- gsub("pl\\$DataFrame", "pl\\$LazyFrame", tmp)
  out <- gsub("as_polars_df\\(", "as_polars_lf(", out)
  out <- gsub("expect_equal\\(", "expect_equal_lazy(", out)
  out <- gsub("expect_error\\(", "expect_error_lazy(", out)
  out <- gsub("expect_snapshot", "expect_snapshot_lazy", out)
  out <- paste0(
    "#############################################\n",
    "### [GENERATED AUTOMATICALLY] Update ", i, " instead.\n",
    "#############################################\n\n",
    "Sys.setenv(POLARS_IN_LAZY_TEST = TRUE)\n\n",
    paste(out, collapse = "\n"),
    "\n\nSys.setenv(POLARS_IN_LAZY_TEST = FALSE)"
  )
  cat(out, file = gsub("\\.R$", "-lazy\\.R", i))
}

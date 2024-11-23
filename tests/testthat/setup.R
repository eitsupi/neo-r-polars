Sys.setenv(POLARS_IN_LAZY_TEST = FALSE)

orig <- test_path("test-dataframe-frame.R")
dest <- test_path("test-lazyframe-frame.R")

tmp <- readLines(orig)
out <- gsub("pl\\$DataFrame", "pl\\$LazyFrame", tmp)
out <- gsub("as_polars_df\\(", "as_polars_lf(", out)
out <- gsub("expect_equal\\(", "expect_equal_lazy(", out)
out <- gsub("expect_error\\(", "expect_error_lazy(", out)
out <- gsub("expect_snapshot", "expect_snapshot_lazy", out)
out <- paste0(
  "#############################################\n",
  "### [GENERATED AUTOMATICALLY] Update ", orig, " instead.\n",
  "#############################################\n\n",
  "withr::with_envvar(
    list(POLARS_IN_LAZY_TEST = TRUE),
    {\n",
  paste(out, collapse = "\n"),
  "\n}
  )"
)
cat(out, file = dest)

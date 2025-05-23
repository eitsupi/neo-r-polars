### To be run after altdoc::render_docs()

list_man_html <- list.files("docs/man", pattern = "\\.html$", full.names = TRUE, recursive = TRUE)

### Make the "Usage" section prettier (if there is one):
### DataFrame_describe(...)   ->   <DataFrame>$describe()

classes <- c(
  "series",
  "dataframe",
  "lazyframe",
  "groupby",
  "lazygroupby",
  "sql_context",
  # "S3",
  "expr",
  "pl"
)

to_modify <- grep(
  paste0("/", paste(classes, collapse = "|")),
  list_man_html,
  value = TRUE
)

for (i in to_modify) {
  which_class <- if (grepl("man/expr__", i)) {
    "expr"
  } else if (grepl("man/expr_arr_", i)) {
    "expr_arr"
  } else if (grepl("man/expr_bin_", i)) {
    "expr_bin"
  } else if (grepl("man/expr_cat_", i)) {
    "expr_cat"
  } else if (grepl("man/expr_dt_", i)) {
    "expr_dt"
  } else if (grepl("man/expr_list_", i)) {
    "expr_list"
  } else if (grepl("man/expr_meta_", i)) {
    "expr_meta"
  } else if (grepl("man/expr_name_", i)) {
    "expr_name"
  } else if (grepl("man/expr_str_", i)) {
    "expr_str"
  } else if (grepl("man/expr_struct_", i)) {
    "expr_struct"
  } else if (grepl("man/expr__", i)) {
    "expr"
  } else if (grepl("man/dataframe__", i)) {
    "dataframe"
  } else if (grepl("man/lazyframe__", i)) {
    "lazyframe"
  } else if (grepl("man/groupby__", i)) {
    "groupby"
  } else if (grepl("man/lazygroupby__", i)) {
    "lazygroupby"
  } else if (grepl("man/series__", i)) {
    "series"
  } else if (grepl("man/pl__", i)) {
    "pl"
  } else if (grepl("man/sql_context", i)) {
    "sql_context"
  } else {
    "foobar"
  }

  orig <- readLines(i, warn = FALSE)

  if (!any(grepl("<h2 id=\"usage\">Usage</h2>", orig))) {
    next
  }

  # prefix with pl$ for read/scan
  if (which_class == "pl") {
    new <- gsub(
      "<code class='language-R'>pl__",
      "<code class='language-R'>pl$",
      orig
    )
  } else if (which_class == "expr") {
    new <- gsub(
      "<code class='language-R'>expr__",
      "<code class='language-R'>&lt;Expr&gt;$",
      orig
    )
  } else if (
    which_class %in%
      c(
        "expr_arr",
        "expr_bin",
        "expr_cat",
        "expr_dt",
        "expr_list",
        "expr_meta",
        "expr_name",
        "expr_str",
        "expr_struct"
      )
  ) {
    subns <- gsub("expr_", "", which_class)

    new <- gsub(
      paste0("<code class='language-R'>", which_class, "_"),
      paste0("<code class='language-R'>&lt;Expr&gt;$", subns, "$"),
      orig
    )
  } else if (
    which_class %in% c("dataframe", "lazyframe", "groupby", "lazygroupby", "sql_context")
  ) {
    replacement <- switch(
      which_class,
      dataframe = "DataFrame",
      lazyframe = "LazyFrame",
      groupby = "GroupBy",
      lazygroupby = "LazyGroupBy",
      sql_context = "SQLContext",
      "unreachable"
    )
    new <- gsub(
      paste0("<code class='language-R'>", which_class, "__"),
      paste0(
        "<code class='language-R'>&lt;",
        replacement,
        "&gt;$"
      ),
      orig
    )
  } else {
    new <- gsub(
      paste0("<code class='language-R'>", which_class, "_"),
      paste0("<code class='language-R'>&lt;", which_class, "&gt;$"),
      orig
    )
  }

  # fix escaping of left-angle brackets (not needed for right-angle brackets)
  new <- gsub("\\\\&lt;", "&lt;", new)

  writeLines(new, i)
}

###### Custom script:
######
###### - get the Rd files, convert them to markdown in "docs/docs/reference" and
######   put them in "mkdocs.yaml"
###### - run the examples in the "Reference" files
library(yaml)
library(here)
library(pkgload)

pkgload::load_all(".")

yml <- read_yaml("altdoc/mkdocs_static.yml")

is_internal <- function(file) {
  y <- capture.output(tools::Rd2latex(file))
  z <- grepl("\\\\keyword\\{", y)
  if (!any(z)) {
    return(FALSE)
  }
  reg <- regmatches(y, gregexpr("\\{\\K[^{}]+(?=\\})", y, perl = TRUE))
  test <- vapply(
    seq_along(y),
    function(foo) {
      z[foo] && ("internal" %in% reg[[foo]] || "docs" %in% reg[[foo]])
    },
    FUN.VALUE = logical(1L)
  )
  any(test)
}


##############
## Make docs hierarchy ##
##############

other <- list.files("man", pattern = "\\.Rd")
other <- Filter(\(x) !is_internal(paste0("man/", x)), other)
other <- sub("Rd$", "md", other)
out <- list()
# order determines order in sidebar
classes <- c(
  "pl_",
  "series_",
  "dataframe_",
  "lazyframe_",
  # "GroupBy",
  # "LazyGroupBy",
  # "RollingGroupBy",
  # "DynamicGroupBy",
  "expr_list",
  "expr_bin",
  "expr_cat",
  "expr_dt",
  "expr_meta",
  "expr_name",
  "expr_str",
  "expr_struct",
  "expr_arr",
  "expr_"
  # "IO",
  # "RThreadHandle",
  # "SQLContext",
  # "S3"
)
for (cl in classes) {
  files <- grep(paste0("^", cl, "_"), other, value = TRUE)
  fn_name <- sub("\\.md", "", sub("[^_]*_", "", files))
  fn_name <- sub("^_", "", fn_name)

  tmp <- sprintf("%s: man/%s", fn_name, files)
  cl_label <- ifelse(cl == "pl_", "Polars", cl)
  cl_label <- ifelse(cl == "IO", "Input/Output", cl_label)
  cl_label <- ifelse(cl == "S3", "S3 Methods", cl_label)
  cl_label <- ifelse(cl == "lazyframe_", "LazyFrame", cl_label)
  cl_label <- ifelse(cl == "dataframe_", "DataFrame", cl_label)
  cl_label <- ifelse(cl == "series_", "Series", cl_label)
  cl_label <- gsub("_$", "", cl_label)
  out <- append(out, setNames(list(tmp), cl_label))
  other <- setdiff(other, files)
}
# expr: nested
nam <- c(
  "expr" = "All others",
  "expr_arr" = "Array",
  "expr_list" = "List",
  "expr_bin" = "Binary",
  "expr_cat" = "Categorical",
  "expr_dt" = "DateTime",
  "expr_meta" = "Meta",
  "expr_name" = "Name",
  "expr_str" = "String",
  "expr_struct" = "Struct"
)

tmp <- lapply(names(nam), \(n) setNames(list(out[[n]]), nam[n]))
out <- out[!names(out) %in% names(nam)]
out[["Expressions"]] <- tmp
# other
tmp <- sprintf("%s: man/%s", sub("\\.md$", "", other), other)
hierarchy <- append(out, setNames(list(tmp), "Other"))


##############
## Convert to yaml format ##
##############

hierarchy <- append(list("Reference" = "reference_home.md"), hierarchy)

# Insert the links in the settings
yml$nav[[3]]$Reference = hierarchy


# Customize the search
plugins <- yml$plugins
replacement <- list(
  separator = paste0("[\\s\\-\\$]+|(", paste(classes, collapse = "_|"), "_)")
)
if (is.character(plugins)) {
  plugins <- setNames(as.list(plugins), plugins)
  plugins[["search"]] = replacement
} else if (is.list(plugins)) {
  for (i in seq_along(plugins)) {
    if (plugins[[i]] == "search") {
      plugins[[i]] = list(search = replacement)
    }
  }
}
yml$plugins = plugins


# These two elements should be lists in the yaml format, not single elements,
# otherwise mkdocs breaks
for (i in c("extra_css", "plugins")) {
  if (!is.null(yml[[i]]) && !is.list(length(yml[[i]]))) {
    yml[[i]] = as.list(yml[[i]])
  }
}

# Write the settings to the `altdoc/` directory
out <- as.yaml(yml, indent.mapping.sequence = TRUE)
out <- gsub("- '", "- ", out)
out <- gsub("\\.md'", "\\.md", out)
cat(out, file = "altdoc/mkdocs.yml")

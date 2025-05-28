
<!-- README.md is generated from README.Rmd. Please edit that file -->

# polars

<!-- TODO: add link to discord -->

<!-- badges: start -->

[![R-multiverse
status](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fcommunity.r-multiverse.org%2Fapi%2Fpackages%2Fpolars&query=%24.Version&label=r-multiverse)](https://community.r-multiverse.org/polars)
[![R-universe status
badge](https://rpolars.r-universe.dev/badges/polars)](https://rpolars.r-universe.dev)
[![CRAN
status](https://www.r-pkg.org/badges/version/polars)](https://CRAN.R-project.org/package=polars)
[![Docs dev
version](https://img.shields.io/badge/docs-dev-blue.svg)](https://pola-rs.github.io/r-polars)
<!-- badges: end -->

## Overview

Polars is a DataFrame interface on top of an OLAP Query Engine
implemented in Rust using [Apache Arrow Columnar
Format](https://arrow.apache.org/docs/format/Columnar.html) as the
memory model.

- Lazy \| eager execution
- Multi-threaded
- SIMD
- Query optimization
- Powerful expression API
- Hybrid Streaming (larger-than-RAM datasets)
- Rust \| Python \| NodeJS \| R \| …

This `{polars}` R package provides the R bindings for Polars. It can be
used to convert R DataFrames to Polars DataFrames and vice versa, as
well as to integrate with other common R packages.

To learn more, read the [online
documentation](https://pola-rs.github.io/r-polars/) for this R package,
and the [user guide](https://docs.pola.rs/) for Python / Rust Polars.

## Installation

The recommended way to install this package is from the R-multiverse
community repository:

``` r
Sys.setenv(NOT_CRAN = "true")
install.packages("polars", repos = "https://community.r-multiverse.org")
```

More recent (unstable) version may be installed from the rpolars
R-universe repository:

``` r
Sys.setenv(NOT_CRAN = "true")
install.packages('polars', repos = c("https://rpolars.r-universe.dev", "https://cloud.r-project.org"))
```

<!-- TODO: link to the installation vignette -->

## Usage

To avoid conflicts with other packages and base R function names,
`{polars}`’s top level functions are hosted in the `pl` environment, and
accessible via the `pl$` prefix. And, most of the methods for `{polars}`
objects should be called with the `$` operator. This means that `polars`
queries written in Python and in R are very similar.

For example, writing the [example from the user guide of
Python/Rust](https://docs.pola.rs/#example) in R:

``` r
library(neopolars) # TODO: rename to polars

# Prepare a CSV file
csv_file <- tempfile(fileext = ".csv")
write.csv(iris, csv_file, row.names = FALSE)

# Create a query plan (LazyFrame) with filtering and group aggregation
q <- pl$scan_csv(csv_file)$filter(pl$col("Sepal.Length") > 5)$group_by(
  "Species",
  .maintain_order = TRUE
)$agg(pl$all()$sum())

# Execute the query plan and collect the result as a Polars DataFrame
q$collect()
#> shape: (3, 5)
#> ┌────────────┬──────────────┬─────────────┬──────────────┬─────────────┐
#> │ Species    ┆ Sepal.Length ┆ Sepal.Width ┆ Petal.Length ┆ Petal.Width │
#> │ ---        ┆ ---          ┆ ---         ┆ ---          ┆ ---         │
#> │ str        ┆ f64          ┆ f64         ┆ f64          ┆ f64         │
#> ╞════════════╪══════════════╪═════════════╪══════════════╪═════════════╡
#> │ setosa     ┆ 116.9        ┆ 81.7        ┆ 33.2         ┆ 6.1         │
#> │ versicolor ┆ 281.9        ┆ 131.8       ┆ 202.9        ┆ 63.3        │
#> │ virginica  ┆ 324.5        ┆ 146.2       ┆ 273.1        ┆ 99.6        │
#> └────────────┴──────────────┴─────────────┴──────────────┴─────────────┘
```

The [Get Started
vignette](https://pola-rs.github.io/r-polars/vignettes/polars.html)
(`vignette("polars")`) provides a more detailed introduction.

## Extensions

While one can use polars as-is, other packages build on it to provide
different syntaxes:

- [polarssql](https://rpolars.github.io/r-polarssql/) provides a polars
  backend for [DBI](https://dbi.r-dbi.org/) and
  [dbplyr](https://dbplyr.tidyverse.org/).
- [tidypolars](https://tidypolars.etiennebacher.com/) allows one to use
  the [tidyverse](https://www.tidyverse.org/) syntax while using the
  power of polars.

<!-- TODO: add governance section or something else -->

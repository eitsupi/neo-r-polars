use crate::{
    prelude::*, PlRDataFrame, PlRDataType, PlRExpr, PlRLazyFrame, PlRLazyGroupBy, RPolarsErr,
};
use polars::io::RowIndex;
use savvy::{savvy, ListSexp, LogicalSexp, NumericScalar, OwnedStringSexp, Result, Sexp};

#[savvy]
impl PlRLazyFrame {
    fn describe_plan(&self) -> Result<Sexp> {
        let string = self.ldf.describe_plan().map_err(RPolarsErr::from)?;
        let sexp = OwnedStringSexp::try_from_scalar(string)?;
        Ok(sexp.into())
    }

    fn describe_optimized_plan(&self) -> Result<Sexp> {
        let string = self
            .ldf
            .describe_optimized_plan()
            .map_err(RPolarsErr::from)?;
        let sexp = OwnedStringSexp::try_from_scalar(string)?;
        Ok(sexp.into())
    }

    fn describe_plan_tree(&self) -> Result<Sexp> {
        let string = self.ldf.describe_plan_tree().map_err(RPolarsErr::from)?;
        let sexp = OwnedStringSexp::try_from_scalar(string)?;
        Ok(sexp.into())
    }

    fn describe_optimized_plan_tree(&self) -> Result<Sexp> {
        let string = self
            .ldf
            .describe_optimized_plan_tree()
            .map_err(RPolarsErr::from)?;
        let sexp = OwnedStringSexp::try_from_scalar(string)?;
        Ok(sexp.into())
    }

    fn optimization_toggle(
        &self,
        type_coercion: bool,
        predicate_pushdown: bool,
        projection_pushdown: bool,
        simplify_expression: bool,
        slice_pushdown: bool,
        comm_subplan_elim: bool,
        comm_subexpr_elim: bool,
        cluster_with_columns: bool,
        streaming: bool,
        _eager: bool,
    ) -> Result<Self> {
        let ldf = self
            .ldf
            .clone()
            .with_type_coercion(type_coercion)
            .with_predicate_pushdown(predicate_pushdown)
            .with_simplify_expr(simplify_expression)
            .with_slice_pushdown(slice_pushdown)
            .with_comm_subplan_elim(comm_subplan_elim)
            .with_comm_subexpr_elim(comm_subexpr_elim)
            .with_cluster_with_columns(cluster_with_columns)
            .with_streaming(streaming)
            ._with_eager(_eager)
            .with_projection_pushdown(projection_pushdown);

        Ok(ldf.into())
    }

    fn filter(&mut self, predicate: &PlRExpr) -> Result<Self> {
        let ldf = self.ldf.clone();
        Ok(ldf.filter(predicate.inner.clone()).into())
    }

    fn select(&mut self, exprs: ListSexp) -> Result<Self> {
        let ldf = self.ldf.clone();
        let exprs = <Wrap<Vec<Expr>>>::from(exprs).0;
        Ok(ldf.select(exprs).into())
    }

    fn group_by(&mut self, by: ListSexp, maintain_order: bool) -> Result<PlRLazyGroupBy> {
        let ldf = self.ldf.clone();
        let by = <Wrap<Vec<Expr>>>::from(by).0;
        let lazy_gb = if maintain_order {
            ldf.group_by_stable(by)
        } else {
            ldf.group_by(by)
        };

        Ok(lazy_gb.into())
    }

    fn collect(&self) -> Result<PlRDataFrame> {
        use crate::{
            r_threads::{concurrent_handler, ThreadCom},
            r_udf::{RUdfReturn, RUdfSignature, CONFIG},
        };
        fn serve_r(
            udf_sig: RUdfSignature,
        ) -> std::result::Result<RUdfReturn, Box<dyn std::error::Error>> {
            udf_sig.eval()
        }

        let ldf = self.ldf.clone();
        let df = if ThreadCom::try_from_global(&CONFIG).is_ok() {
            ldf.collect().map_err(RPolarsErr::from)?
        } else {
            concurrent_handler(
                // closure 1: spawned by main thread
                // tc is a ThreadCom which any child thread can use to submit R jobs to main thread
                move |tc| {
                    // get return value
                    let retval = ldf.collect();

                    // drop the last two ThreadCom clones, signals to main/R-serving thread to shut down.
                    ThreadCom::kill_global(&CONFIG);
                    drop(tc);

                    retval
                },
                // closure 2: how to serve polars worker R job request in main thread
                serve_r,
                // CONFIG is "global variable" where any new thread can request a clone of ThreadCom to establish contact with main thread
                &CONFIG,
            )
            .map_err(|e| e.to_string())?
            .map_err(RPolarsErr::from)?
        };

        Ok(df.into())
    }

    fn slice(&self, offset: NumericScalar, len: Option<NumericScalar>) -> Result<Self> {
        let ldf = self.ldf.clone();
        let offset = <Wrap<i64>>::try_from(offset)?.0;
        let len = len
            .map(|l| <Wrap<u32>>::try_from(l))
            .transpose()?
            .map(|l| l.0);
        Ok(ldf.slice(offset, len.unwrap_or(u32::MAX)).into())
    }

    fn tail(&self, n: NumericScalar) -> Result<Self> {
        let ldf = self.ldf.clone();
        let n = <Wrap<u32>>::try_from(n)?.0;
        Ok(ldf.tail(n).into())
    }

    fn drop(&self, columns: ListSexp, strict: bool) -> Result<Self> {
        let ldf = self.ldf.clone();
        let columns = <Wrap<Vec<Expr>>>::from(columns).0;
        if strict {
            Ok(ldf.drop(columns).into())
        } else {
            Ok(ldf.drop_no_validate(columns).into())
        }
    }

    fn cast(&self, dtypes: ListSexp, strict: bool) -> Result<Self> {
        let dtypes = <Wrap<Vec<Field>>>::try_from(dtypes)?.0;
        let mut cast_map = PlHashMap::with_capacity(dtypes.len());
        cast_map.extend(dtypes.iter().map(|f| (f.name.as_ref(), f.dtype.clone())));
        Ok(self.ldf.clone().cast(cast_map, strict).into())
    }

    fn cast_all(&self, dtype: &PlRDataType, strict: bool) -> Result<Self> {
        Ok(self.ldf.clone().cast_all(dtype.dt.clone(), strict).into())
    }

    fn sort_by_exprs(
        &self,
        by: ListSexp,
        descending: LogicalSexp,
        nulls_last: LogicalSexp,
        maintain_order: bool,
        multithreaded: bool,
    ) -> Result<Self> {
        let ldf = self.ldf.clone();
        let by = <Wrap<Vec<Expr>>>::from(by).0;
        Ok(ldf
            .sort_by_exprs(
                by,
                SortMultipleOptions {
                    descending: descending.to_vec(),
                    nulls_last: nulls_last.to_vec(),
                    maintain_order,
                    multithreaded,
                },
            )
            .into())
    }

    fn with_columns(&mut self, exprs: ListSexp) -> Result<Self> {
        let ldf = self.ldf.clone();
        let exprs = <Wrap<Vec<Expr>>>::from(exprs).0;
        Ok(ldf.with_columns(exprs).into())
    }

    fn new_from_csv(
        path: &str,
        separator: &str,
        has_header: bool,
        ignore_errors: bool,
        skip_rows: NumericScalar,
        cache: bool,
        missing_utf8_is_empty_string: bool,
        low_memory: bool,
        rechunk: bool,
        skip_rows_after_header: NumericScalar,
        encoding: &str,
        try_parse_dates: bool,
        eol_char: &str,
        raise_if_empty: bool,
        truncate_ragged_lines: bool,
        decimal_comma: bool,
        glob: bool,
        retries: NumericScalar,
        comment_prefix: Option<&str>,
        quote_char: Option<&str>,
        // null_values: Option<Wrap<NullValues>>,
        infer_schema_length: Option<NumericScalar>,
        // with_schema_modify: Option<PyObject>,
        row_index_name: Option<&str>,
        row_index_offset: Option<NumericScalar>,
        n_rows: Option<NumericScalar>,
        // overwrite_dtype: Option<Vec<(&str, Wrap<DataType>)>>,
        // schema: Option<Wrap<Schema>>,
        // cloud_options: Option<Vec<(String, String)>>,
        // credential_provider: Option<PyObject>,
        file_cache_ttl: Option<NumericScalar>,
        include_file_paths: Option<&str>,
    ) -> Result<PlRLazyFrame> {
        use cloud::credential_provider::PlCredentialProvider;

        let path = std::path::PathBuf::from(path);
        let encoding = <Wrap<CsvEncoding>>::try_from(encoding)?.0;
        let skip_rows = <Wrap<usize>>::try_from(skip_rows)?.0;
        let skip_rows_after_header = <Wrap<usize>>::try_from(skip_rows_after_header)?.0;
        let infer_schema_length: Option<usize> = match infer_schema_length {
            Some(x) => Some(<Wrap<usize>>::try_from(x)?.0),
            None => None,
        };
        let row_index_offset: Option<u32> = match row_index_offset {
            Some(x) => Some(<Wrap<u32>>::try_from(x)?.0),
            None => None,
        };
        let n_rows: Option<usize> = match n_rows {
            Some(x) => Some(<Wrap<usize>>::try_from(x)?.0),
            None => None,
        };
        let retries = <Wrap<usize>>::try_from(retries)?.0;
        let file_cache_ttl: Option<u64> = match file_cache_ttl {
            Some(x) => Some(<Wrap<u64>>::try_from(x)?.0),
            None => None,
        };

        // let null_values = null_values.map(|w| w.0);
        let quote_char = quote_char
            .map(|s| {
                s.as_bytes()
                    .first()
                    .ok_or_else(|| polars_err!(InvalidOperation: "`quote_char` cannot be empty"))
            })
            .transpose()
            .map_err(RPolarsErr::from)?
            .copied();
        let separator = separator
            .as_bytes()
            .first()
            .ok_or_else(|| polars_err!(InvalidOperation: "`separator` cannot be empty"))
            .copied()
            .map_err(RPolarsErr::from)?;
        let eol_char = eol_char
            .as_bytes()
            .first()
            .ok_or_else(|| polars_err!(InvalidOperation: "`eol_char` cannot be empty"))
            .copied()
            .map_err(RPolarsErr::from)?;

        let row_index: Option<RowIndex> = match row_index_name {
            Some(x) => Some(RowIndex {
                name: x.into(),
                // TODO: remove unwrap()
                offset: row_index_offset.unwrap(),
            }),
            None => None,
        };

        // let overwrite_dtype = overwrite_dtype.map(|overwrite_dtype| {
        //     overwrite_dtype
        //         .into_iter()
        //         .map(|(name, dtype)| Field::new((&*name).into(), dtype.0))
        //         .collect::<Schema>()
        // });

        let sources = path;
        // let first_path = sources;
        // let sources = sources.0;
        // let (first_path, sources) = match source {
        //     None => (sources.first_path().map(|p| p.to_path_buf()), sources),
        //     Some(source) => pyobject_to_first_path_and_scan_sources(source)?,
        // };

        // let mut r = LazyCsvReader::new_with_sources(sources);
        let mut r = LazyCsvReader::new(sources);

        // if let Some(first_path) = first_path {
        //     let first_path_url = first_path.to_string_lossy();

        //     let mut cloud_options =
        //         parse_cloud_options(&first_path_url, cloud_options.unwrap_or_default())?;
        //     if let Some(file_cache_ttl) = file_cache_ttl {
        //         cloud_options.file_cache_ttl = file_cache_ttl;
        //     }
        //     cloud_options = cloud_options
        //         .with_max_retries(retries)
        //         .with_credential_provider(
        //             credential_provider.map(PlCredentialProvider::from_python_func_object),
        //         );
        //     r = r.with_cloud_options(Some(cloud_options));
        // }

        let mut r = r
            .with_infer_schema_length(infer_schema_length)
            .with_separator(separator)
            .with_has_header(has_header)
            .with_ignore_errors(ignore_errors)
            .with_skip_rows(skip_rows)
            .with_n_rows(n_rows)
            .with_cache(cache)
            // .with_dtype_overwrite(overwrite_dtype.map(Arc::new))
            // .with_schema(schema.map(|schema| Arc::new(schema.0)))
            .with_low_memory(low_memory)
            .with_comment_prefix(comment_prefix.map(|x| x.into()))
            .with_quote_char(quote_char)
            .with_eol_char(eol_char)
            .with_rechunk(rechunk)
            .with_skip_rows_after_header(skip_rows_after_header)
            .with_encoding(encoding)
            .with_row_index(row_index)
            .with_try_parse_dates(try_parse_dates)
            // .with_null_values(null_values)
            .with_missing_is_null(!missing_utf8_is_empty_string)
            .with_truncate_ragged_lines(truncate_ragged_lines)
            .with_decimal_comma(decimal_comma)
            .with_glob(glob)
            .with_raise_if_empty(raise_if_empty)
            .with_include_file_paths(include_file_paths.map(|x| x.into()));

        // if let Some(lambda) = with_schema_modify {
        //     let f = |schema: Schema| {
        //         let iter = schema.iter_names().map(|s| s.as_str());
        //         Python::with_gil(|py| {
        //             let names = PyList::new_bound(py, iter);

        //             let out = lambda.call1(py, (names,)).expect("python function failed");
        //             let new_names = out
        //                 .extract::<Vec<String>>(py)
        //                 .expect("python function should return List[str]");
        //             polars_ensure!(new_names.len() == schema.len(),
        //                 ShapeMismatch: "The length of the new names list should be equal to or less than the original column length",
        //             );
        //             Ok(schema
        //                 .iter_values()
        //                 .zip(new_names)
        //                 .map(|(dtype, name)| Field::new(name.into(), dtype.clone()))
        //                 .collect())
        //         })
        //     };
        //     r = r.with_schema_modify(f).map_err(RPolarsErr::from)?
        // }

        Ok(r.finish().map_err(RPolarsErr::from)?.into())
    }
}

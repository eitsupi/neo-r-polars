use crate::{
    prelude::*, PlRDataFrame, PlRDataType, PlRExpr, PlRLazyFrame, PlRLazyGroupBy, RPolarsErr,
};
use polars::io::{HiveOptions, RowIndex};
use savvy::{
    savvy, ListSexp, LogicalSexp, NumericScalar, OwnedListSexp, OwnedStringSexp, Result, Sexp,
    StringSexp,
};

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

    fn collect_schema(&mut self) -> Result<Sexp> {
        let schema = self.ldf.collect_schema().map_err(RPolarsErr::from)?;
        let mut out = OwnedListSexp::new(schema.len(), true)?;
        for (i, (name, dtype)) in schema.iter().enumerate() {
            let value: Sexp = PlRDataType::from(dtype.clone()).try_into()?;
            let _ = out.set_name_and_value(i, name.as_str(), value);
        }
        Ok(out.into())
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
                    limit: None,
                },
            )
            .into())
    }

    fn with_columns(&mut self, exprs: ListSexp) -> Result<Self> {
        let ldf = self.ldf.clone();
        let exprs = <Wrap<Vec<Expr>>>::from(exprs).0;
        Ok(ldf.with_columns(exprs).into())
    }

    fn new_from_ipc(
        path: StringSexp,
        cache: bool,
        rechunk: bool,
        try_parse_hive_dates: bool,
        retries: NumericScalar,
        n_rows: Option<NumericScalar>,
        row_index_name: Option<&str>,
        row_index_offset: Option<NumericScalar>,
        cloud_options: Option<StringSexp>,
        // credential_provider: Option<PyObject>,
        hive_partitioning: Option<bool>,
        hive_schema: Option<ListSexp>,
        file_cache_ttl: Option<NumericScalar>,
        include_file_paths: Option<&str>,
    ) -> Result<Self> {
        use std::path::PathBuf;

        let path = path
            .to_vec()
            .iter()
            .map(std::path::PathBuf::from)
            .collect::<Vec<PathBuf>>();
        let row_index_offset: Option<u32> = match row_index_offset {
            Some(x) => Some(<Wrap<u32>>::try_from(x)?.0),
            None => None,
        };
        let hive_schema: Option<Wrap<Schema>> = match hive_schema {
            Some(x) => Some(<Wrap<Schema>>::try_from(x)?),
            None => None,
        };
        let n_rows: Option<usize> = match n_rows {
            Some(x) => Some(<Wrap<usize>>::try_from(x)?.0),
            None => None,
        };
        let row_index: Option<RowIndex> = match row_index_name {
            Some(x) => Some(RowIndex {
                name: x.into(),
                // TODO: remove unwrap()
                offset: row_index_offset.unwrap(),
            }),
            None => None,
        };
        let retries = <Wrap<usize>>::try_from(retries)?.0;
        let file_cache_ttl: Option<u64> = match file_cache_ttl {
            Some(x) => Some(<Wrap<u64>>::try_from(x)?.0),
            None => None,
        };

        let hive_options = HiveOptions {
            enabled: hive_partitioning,
            hive_start_idx: 0,
            schema: hive_schema.map(|x| Arc::new(x.0)),
            try_parse_dates: try_parse_hive_dates,
        };

        let cloud_options: Option<Vec<(String, String)>> = match cloud_options {
            Some(x) => {
                let values = x.to_vec();
                let names = x.get_names().unwrap();
                let vec = names
                    .into_iter()
                    .zip(values.into_iter())
                    .map(|(xi, yi)| (xi.to_string(), yi.to_string()))
                    .collect::<Vec<(String, String)>>();
                Some(vec)
            }
            None => None,
        };

        let mut args = ScanArgsIpc {
            n_rows,
            cache,
            rechunk,
            row_index,
            cloud_options: None,
            hive_options,
            include_file_paths: include_file_paths.map(|x| x.into()),
        };

        let sources = path;
        let first_path: Option<PathBuf> = sources.get(0).unwrap().clone().into();
        // let sources = sources.0;
        // let (first_path, sources) = match source {
        //     None => (sources.first_path().map(|p| p.to_path_buf()), sources),
        //     Some(source) => pyobject_to_first_path_and_scan_sources(source)?,
        // };

        if let Some(first_path) = first_path {
            let first_path_url = first_path.to_string_lossy();

            let mut cloud_options =
                parse_cloud_options(&first_path_url, cloud_options.unwrap_or_default())?;
            if let Some(file_cache_ttl) = file_cache_ttl {
                cloud_options.file_cache_ttl = file_cache_ttl;
            }
            args.cloud_options = Some(
                cloud_options.with_max_retries(retries), // .with_credential_provider(
                                                         //     credential_provider.map(PlCredentialProvider::from_python_func_object),
                                                         // ),
            );
        }

        let lf = LazyFrame::scan_ipc_files(sources.into(), args).map_err(RPolarsErr::from)?;
        Ok(lf.into())
    }
}

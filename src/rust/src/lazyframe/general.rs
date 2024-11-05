use super::*;
use crate::{PlRDataFrame, PlRDataType, PlRExpr, PlRLazyFrame, PlRLazyGroupBy, RPolarsErr};
use savvy::{
    savvy, ListSexp, LogicalSexp, NumericScalar, OwnedStringSexp, Result, Sexp, StringSexp,
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
    ) -> Result<PlRLazyFrame> {
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

    fn filter(&mut self, predicate: &PlRExpr) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        Ok(ldf.filter(predicate.inner.clone()).into())
    }

    fn select(&mut self, exprs: ListSexp) -> Result<PlRLazyFrame> {
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

    fn slice(&self, offset: NumericScalar, len: Option<NumericScalar>) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let offset = <Wrap<i64>>::try_from(offset)?.0;
        let len = len
            .map(|l| <Wrap<u32>>::try_from(l))
            .transpose()?
            .map(|l| l.0);
        Ok(ldf.slice(offset, len.unwrap_or(u32::MAX)).into())
    }

    fn tail(&self, n: NumericScalar) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let n = <Wrap<u32>>::try_from(n)?.0;
        Ok(ldf.tail(n).into())
    }

    fn drop(&self, columns: ListSexp, strict: bool) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let columns = <Wrap<Vec<Expr>>>::from(columns).0;
        if strict {
            Ok(ldf.drop(columns).into())
        } else {
            Ok(ldf.drop_no_validate(columns).into())
        }
    }

    fn cast(&self, dtypes: ListSexp, strict: bool) -> Result<PlRLazyFrame> {
        let dtypes = <Wrap<Vec<Field>>>::try_from(dtypes)?.0;
        let mut cast_map = PlHashMap::with_capacity(dtypes.len());
        cast_map.extend(dtypes.iter().map(|f| (f.name.as_ref(), f.dtype.clone())));
        Ok(self.ldf.clone().cast(cast_map, strict).into())
    }

    fn cast_all(&self, dtype: &PlRDataType, strict: bool) -> Result<PlRLazyFrame> {
        Ok(self.ldf.clone().cast_all(dtype.dt.clone(), strict).into())
    }

    fn sort_by_exprs(
        &self,
        by: ListSexp,
        descending: LogicalSexp,
        nulls_last: LogicalSexp,
        maintain_order: bool,
        multithreaded: bool,
    ) -> Result<PlRLazyFrame> {
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

    fn with_columns(&mut self, exprs: ListSexp) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let exprs = <Wrap<Vec<Expr>>>::from(exprs).0;
        Ok(ldf.with_columns(exprs).into())
    }

    fn to_dot(&self, optimized: bool) -> Result<String> {
        let result = self.ldf.to_dot(optimized).map_err(RPolarsErr::from)?;
        Ok(result)
    }

    fn sort(
        &self,
        by_column: &str,
        descending: bool,
        nulls_last: bool,
        maintain_order: bool,
        multithreaded: bool,
    ) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        Ok(ldf
            .sort(
                [by_column],
                SortMultipleOptions {
                    descending: vec![descending],
                    nulls_last: vec![nulls_last],
                    multithreaded,
                    maintain_order,
                },
            )
            .into())
    }

    fn top_k(&self, k: NumericScalar, by: ListSexp, reverse: LogicalSexp) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let k = <Wrap<u32>>::try_from(k)?.0;
        let exprs = <Wrap<Vec<Expr>>>::from(by).0;
        let reverse = reverse.to_vec();
        Ok(ldf
            .top_k(
                k,
                exprs,
                SortMultipleOptions::new().with_order_descending_multi(reverse),
            )
            .into())
    }

    fn bottom_k(
        &self,
        k: NumericScalar,
        by: ListSexp,
        reverse: LogicalSexp,
    ) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let k = <Wrap<u32>>::try_from(k)?.0;
        let exprs = <Wrap<Vec<Expr>>>::from(by).0;
        let reverse = reverse.to_vec();
        Ok(ldf
            .bottom_k(
                k,
                exprs,
                SortMultipleOptions::new().with_order_descending_multi(reverse),
            )
            .into())
    }

    fn cache(&self) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        Ok(ldf.cache().into())
    }

    // fn profile(&self, py: Python) -> Result<(PlRDataFrame, PlRDataFrame)> {
    //     // if we don't allow threads and we have udfs trying to acquire the gil from different
    //     // threads we deadlock.
    //     let (df, time_df) = py.allow_threads(|| {
    //         let ldf = self.ldf.clone();
    //         ldf.profile().map_err(RPolarsErr::from)
    //     })?;
    //     Ok((df.into(), time_df.into()))
    // }

    //     fn collect_with_callback(&self, lambda: PyObject) {
    //         let ldf = self.ldf.clone();

    //         polars_core::POOL.spawn(move || {
    //             let result = ldf
    //                 .collect()
    //                 .map(PlRDataFrame::new)
    //                 .map_err(RPolarsErr::from);

    //             Python::with_gil(|py| match result {
    //                 Ok(df) => {
    //                     lambda.call1(py, (df,)).map_err(|err| err.restore(py)).ok();
    //                 }
    //                 Err(err) => {
    //                     lambda
    //                         .call1(py, (PyErr::from(err).to_object(py),))
    //                         .map_err(|err| err.restore(py))
    //                         .ok();
    //                 }
    //             });
    //         });
    //     }

    // fn sink_parquet(
    //     &self,
    //     py: Python,
    //     path: PathBuf,
    //     compression: &str,
    //     compression_level: Option<i32>,
    //     statistics: Wrap<StatisticsOptions>,
    //     row_group_size: Option<usize>,
    //     data_page_size: Option<usize>,
    //     maintain_order: bool,
    // ) -> Result<()> {
    //     let compression = parse_parquet_compression(compression, compression_level)?;

    //     let options = ParquetWriteOptions {
    //         compression,
    //         statistics: statistics.0,
    //         row_group_size,
    //         data_page_size,
    //         maintain_order,
    //     };

    //     // if we don't allow threads and we have udfs trying to acquire the gil from different
    //     // threads we deadlock.
    //     py.allow_threads(|| {
    //         let ldf = self.ldf.clone();
    //         ldf.sink_parquet(path, options).map_err(RPolarsErr::from)
    //     })?;
    //     Ok(())
    // }

    // fn sink_ipc(
    //     &self,
    //     py: Python,
    //     path: PathBuf,
    //     compression: Option<Wrap<IpcCompression>>,
    //     maintain_order: bool,
    // ) -> Result<()> {
    //     let options = IpcWriterOptions {
    //         compression: compression.map(|c| c.0),
    //         maintain_order,
    //     };

    //     // if we don't allow threads and we have udfs trying to acquire the gil from different
    //     // threads we deadlock.
    //     py.allow_threads(|| {
    //         let ldf = self.ldf.clone();
    //         ldf.sink_ipc(path, options).map_err(RPolarsErr::from)
    //     })?;
    //     Ok(())
    // }

    // fn sink_csv(
    //     &self,
    //     py: Python,
    //     path: PathBuf,
    //     include_bom: bool,
    //     include_header: bool,
    //     separator: u8,
    //     line_terminator: String,
    //     quote_char: u8,
    //     batch_size: NonZeroUsize,
    //     datetime_format: Option<String>,
    //     date_format: Option<String>,
    //     time_format: Option<String>,
    //     float_scientific: Option<bool>,
    //     float_precision: Option<usize>,
    //     null_value: Option<String>,
    //     quote_style: Option<Wrap<QuoteStyle>>,
    //     maintain_order: bool,
    // ) -> Result<()> {
    //     let quote_style = quote_style.map_or(QuoteStyle::default(), |wrap| wrap.0);
    //     let null_value = null_value.unwrap_or(SerializeOptions::default().null);

    //     let serialize_options = SerializeOptions {
    //         date_format,
    //         time_format,
    //         datetime_format,
    //         float_scientific,
    //         float_precision,
    //         separator,
    //         quote_char,
    //         null: null_value,
    //         line_terminator,
    //         quote_style,
    //     };

    //     let options = CsvWriterOptions {
    //         include_bom,
    //         include_header,
    //         maintain_order,
    //         batch_size,
    //         serialize_options,
    //     };

    //     // if we don't allow threads and we have udfs trying to acquire the gil from different
    //     // threads we deadlock.
    //     py.allow_threads(|| {
    //         let ldf = self.ldf.clone();
    //         ldf.sink_csv(path, options).map_err(RPolarsErr::from)
    //     })?;
    //     Ok(())
    // }

    //     fn sink_json(&self, py: Python, path: PathBuf, maintain_order: bool) -> Result<()> {
    //         let options = JsonWriterOptions { maintain_order };

    //         // if we don't allow threads and we have udfs trying to acquire the gil from different
    //         // threads we deadlock.
    //         py.allow_threads(|| {
    //             let ldf = self.ldf.clone();
    //             ldf.sink_json(path, options).map_err(RPolarsErr::from)
    //         })?;
    //         Ok(())
    //     }

    // fn fetch(&self, py: Python, n_rows: NumericScalar) -> Result<PlRDataFrame> {
    //     let ldf = self.ldf.clone();
    //     let n_rows = <Wrap<usize>>::try_from(n_rows)?.0;
    //     let df = py.allow_threads(|| ldf.fetch(n_rows).map_err(RPolarsErr::from))?;
    //     Ok(df.into())
    // }

    fn select_seq(&mut self, exprs: ListSexp) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let exprs = <Wrap<Vec<Expr>>>::from(exprs).0;
        Ok(ldf.select_seq(exprs).into())
    }

    fn rolling(
        &mut self,
        index_column: &PlRExpr,
        period: &str,
        offset: &str,
        closed: &str,
        by: ListSexp,
    ) -> Result<PlRLazyGroupBy> {
        let closed_window = <Wrap<ClosedWindow>>::try_from(closed)?.0;
        let ldf = self.ldf.clone();
        let by = <Wrap<Vec<Expr>>>::from(by).0;
        let lazy_gb = ldf.rolling(
            index_column.inner.clone(),
            by,
            RollingGroupOptions {
                index_column: "".into(),
                period: Duration::try_parse(period).map_err(RPolarsErr::from)?,
                offset: Duration::try_parse(offset).map_err(RPolarsErr::from)?,
                closed_window,
            },
        );

        Ok(PlRLazyGroupBy { lgb: Some(lazy_gb) })
    }

    fn group_by_dynamic(
        &mut self,
        index_column: &PlRExpr,
        every: &str,
        period: &str,
        offset: &str,
        label: &str,
        include_boundaries: bool,
        closed: &str,
        group_by: ListSexp,
        start_by: &str,
    ) -> Result<PlRLazyGroupBy> {
        let closed_window = <Wrap<ClosedWindow>>::try_from(closed)?.0;
        let group_by = <Wrap<Vec<Expr>>>::from(group_by).0;
        let ldf = self.ldf.clone();
        let label = <Wrap<Label>>::try_from(label)?.0;
        let start_by = <Wrap<StartBy>>::try_from(start_by)?.0;
        let lazy_gb = ldf.group_by_dynamic(
            index_column.inner.clone(),
            group_by,
            DynamicGroupOptions {
                every: Duration::try_parse(every).map_err(RPolarsErr::from)?,
                period: Duration::try_parse(period).map_err(RPolarsErr::from)?,
                offset: Duration::try_parse(offset).map_err(RPolarsErr::from)?,
                label,
                include_boundaries,
                closed_window,
                start_by,
                ..Default::default()
            },
        );

        Ok(PlRLazyGroupBy { lgb: Some(lazy_gb) })
    }

    fn with_context(&self, contexts: ListSexp) -> Result<PlRLazyFrame> {
        let contexts = <Wrap<Vec<LazyFrame>>>::try_from(contexts)?.0;
        Ok(self.ldf.clone().with_context(contexts).into())
    }

    // fn join_asof(
    //     &self,
    //     other: &PlRLazyFrame,
    //     left_on: &PlRExpr,
    //     right_on: &PlRExpr,
    //     left_by: Option<Vec<&str>>,
    //     right_by: Option<Vec<&str>>,
    //     allow_parallel: bool,
    //     force_parallel: bool,
    //     suffix: String,
    //     strategy: Wrap<AsofStrategy>,
    //     tolerance: Option<Wrap<AnyValue<'_>>>,
    //     tolerance_str: Option<String>,
    //     coalesce: bool,
    // ) -> Result<PlRLazyFrame> {
    //     let coalesce = if coalesce {
    //         JoinCoalesce::CoalesceColumns
    //     } else {
    //         JoinCoalesce::KeepColumns
    //     };
    //     let ldf = self.ldf.clone();
    //     let other = other.ldf;
    //     let left_on = left_on.inner;
    //     let right_on = right_on.inner;
    //     Ok(ldf
    //         .join_builder()
    //         .with(other)
    //         .left_on([left_on])
    //         .right_on([right_on])
    //         .allow_parallel(allow_parallel)
    //         .force_parallel(force_parallel)
    //         .coalesce(coalesce)
    //         .how(JoinType::AsOf(AsOfOptions {
    //             strategy: strategy.0,
    //             left_by: left_by.map(strings_to_pl_smallstr),
    //             right_by: right_by.map(strings_to_pl_smallstr),
    //             tolerance: tolerance.map(|t| t.0.into_static()),
    //             tolerance_str: tolerance_str.map(|s| s.into()),
    //         }))
    //         .suffix(suffix)
    //         .finish()
    //         .into())
    // }

    fn join(
        &self,
        other: &PlRLazyFrame,
        left_on: ListSexp,
        right_on: ListSexp,
        allow_parallel: bool,
        force_parallel: bool,
        join_nulls: bool,
        how: &str,
        suffix: &str,
        validate: &str,
        coalesce: Option<bool>,
    ) -> Result<PlRLazyFrame> {
        let coalesce = match coalesce {
            None => JoinCoalesce::JoinSpecific,
            Some(true) => JoinCoalesce::CoalesceColumns,
            Some(false) => JoinCoalesce::KeepColumns,
        };
        let ldf = self.ldf.clone();
        let other = other.ldf.clone();
        let left_on = <Wrap<Vec<Expr>>>::from(left_on).0;
        let right_on = <Wrap<Vec<Expr>>>::from(right_on).0;
        let how = <Wrap<JoinType>>::try_from(how)?.0;
        let validate = <Wrap<JoinValidation>>::try_from(validate)?.0;
        Ok(ldf
            .join_builder()
            .with(other)
            .left_on(left_on)
            .right_on(right_on)
            .allow_parallel(allow_parallel)
            .force_parallel(force_parallel)
            .join_nulls(join_nulls)
            .how(how)
            .coalesce(coalesce)
            .validate(validate)
            .suffix(suffix)
            .finish()
            .into())
    }

    fn join_where(
        &self,
        other: &PlRLazyFrame,
        predicates: ListSexp,
        suffix: &str,
    ) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let other = other.ldf.clone();

        let predicates = <Wrap<Vec<Expr>>>::from(predicates).0;

        Ok(ldf
            .join_builder()
            .with(other)
            .suffix(suffix)
            .join_where(predicates)
            .into())
    }

    fn with_columns_seq(&mut self, exprs: ListSexp) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let exprs = <Wrap<Vec<Expr>>>::from(exprs).0;
        Ok(ldf.with_columns_seq(exprs).into())
    }

    fn rename(
        &mut self,
        existing: StringSexp,
        new: StringSexp,
        strict: bool,
    ) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        Ok(ldf.rename(existing.to_vec(), new.to_vec(), strict).into())
    }

    fn reverse(&self) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        Ok(ldf.reverse().into())
    }

    fn shift(&self, n: &PlRExpr, fill_value: Option<&PlRExpr>) -> Result<PlRLazyFrame> {
        let lf = self.ldf.clone();
        let out = match fill_value {
            Some(v) => lf.shift_and_fill(n.inner.clone(), v.inner.clone()),
            None => lf.shift(n.inner.clone()),
        };
        Ok(out.into())
    }

    fn fill_nan(&self, fill_value: &PlRExpr) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        Ok(ldf.fill_nan(fill_value.inner.clone()).into())
    }

    fn min(&self) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.min();
        Ok(out.into())
    }

    fn max(&self) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.max();
        Ok(out.into())
    }

    fn sum(&self) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.sum();
        Ok(out.into())
    }

    fn mean(&self) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.mean();
        Ok(out.into())
    }

    fn std(&self, ddof: u8) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.std(ddof);
        Ok(out.into())
    }

    fn var(&self, ddof: u8) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.var(ddof);
        Ok(out.into())
    }

    fn median(&self) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.median();
        Ok(out.into())
    }

    fn quantile(&self, quantile: &PlRExpr, interpolation: &str) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let interpolation = <Wrap<QuantileMethod>>::try_from(interpolation)?.0;
        let out = ldf.quantile(quantile.inner.clone(), interpolation);
        Ok(out.into())
    }

    fn explode(&self, column: ListSexp) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let column = <Wrap<Vec<Expr>>>::from(column).0;
        Ok(ldf.explode(column).into())
    }

    fn null_count(&self) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        Ok(ldf.null_count().into())
    }

    fn unique(
        &self,
        maintain_order: bool,
        keep: &str,
        subset: Option<ListSexp>,
    ) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let keep = <Wrap<UniqueKeepStrategy>>::try_from(keep)?.0;
        let subset = subset.map(|e| <Wrap<Vec<Expr>>>::from(e).0);
        let out = match maintain_order {
            true => ldf.unique_stable_generic(subset, keep),
            false => ldf.unique_generic(subset, keep),
        };
        Ok(out.into())
    }

    fn drop_nulls(&self, subset: Option<ListSexp>) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let subset = subset.map(|e| <Wrap<Vec<Expr>>>::from(e).0);
        Ok(ldf.drop_nulls(subset).into())
    }

    fn unpivot(
        &self,
        on: ListSexp,
        index: ListSexp,
        value_name: Option<&str>,
        variable_name: Option<&str>,
    ) -> Result<PlRLazyFrame> {
        let on = <Wrap<Vec<Expr>>>::from(on).0;
        let index = <Wrap<Vec<Expr>>>::from(index).0;
        let args = UnpivotArgsDSL {
            on: on.into_iter().map(|e| e.into()).collect(),
            index: index.into_iter().map(|e| e.into()).collect(),
            value_name: value_name.map(|s| s.into()),
            variable_name: variable_name.map(|s| s.into()),
        };

        let ldf = self.ldf.clone();
        Ok(ldf.unpivot(args).into())
    }

    fn with_row_index(&self, name: &str, offset: Option<NumericScalar>) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        let offset: Option<u32> = match offset {
            Some(x) => Some(<Wrap<u32>>::try_from(x)?.0),
            None => None,
        };
        Ok(ldf.with_row_index(name, offset).into())
    }

    // fn map_batches(
    //     &self,
    //     lambda: PyObject,
    //     predicate_pushdown: bool,
    //     projection_pushdown: bool,
    //     slice_pushdown: bool,
    //     streamable: bool,
    //     schema: Option<Wrap<Schema>>,
    //     validate_output: bool,
    // ) -> Result<PlRLazyFrame> {
    //     let mut opt = OptFlags::default();
    //     opt.set(OptFlags::PREDICATE_PUSHDOWN, predicate_pushdown);
    //     opt.set(OptFlags::PROJECTION_PUSHDOWN, projection_pushdown);
    //     opt.set(OptFlags::SLICE_PUSHDOWN, slice_pushdown);
    //     opt.set(OptFlags::STREAMING, streamable);

    //     self.ldf
    //         .clone()
    //         .map_python(
    //             lambda.into(),
    //             opt,
    //             schema.map(|s| Arc::new(s.0)),
    //             validate_output,
    //         )
    //         .into()
    // }

    fn clone(&self) -> Result<PlRLazyFrame> {
        Ok(self.ldf.clone().into())
    }

    // fn collect_schema(&mut self, py: Python) -> Result<ListSexp> {
    //     let schema = py
    //         .allow_threads(|| self.ldf.collect_schema())
    //         .map_err(RPolarsErr::from)?;

    //     let schema_dict = PyDict::new_bound(py);
    //     schema.iter_fields().for_each(|fld| {
    //         schema_dict
    //             .set_item(fld.name().as_str(), Wrap(fld.dtype().clone()))
    //             .unwrap()
    //     });
    //     Ok(schema_dict.to_object(py))
    // }

    fn unnest(&self, columns: ListSexp) -> Result<PlRLazyFrame> {
        let columns = <Wrap<Vec<Expr>>>::from(columns).0;
        Ok(self.ldf.clone().unnest(columns).into())
    }

    fn count(&self) -> Result<PlRLazyFrame> {
        let ldf = self.ldf.clone();
        Ok(ldf.count().into())
    }

    fn merge_sorted(&self, other: &PlRLazyFrame, key: &str) -> Result<PlRLazyFrame> {
        let out = self
            .ldf
            .clone()
            .merge_sorted(other.ldf.clone(), key)
            .map_err(RPolarsErr::from)?;
        Ok(out.into())
    }
}

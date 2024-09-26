use crate::{prelude::*, PlRExpr};
use polars_utils::format_pl_smallstr;
use savvy::{savvy, FunctionArgs, FunctionSexp, Result, Sexp, TypedSexp};

#[savvy]
impl PlRExpr {
    fn name_keep(&self) -> Result<Self> {
        Ok(self.inner.clone().name().keep().into())
    }

    fn name_map(&self, lambda: FunctionSexp) -> Result<Self> {
        use crate::r_udf::RUdf;

        let lambda = RUdf::new(lambda);
        let expr = self.inner.clone().name().map(move |name| {
            let mut args = FunctionArgs::new();
            args.add("name", name.as_str()).unwrap();
            let out: Sexp = (*lambda.function)
                .lock()
                .unwrap()
                .0
                .call(args)
                .map_err(|e| PolarsError::ComputeError(format!("R function failed: {}", e).into()))?
                .into();
            match out.into_typed() {
                TypedSexp::String(s) => {
                    let s = s.to_vec().into_iter().nth(0).ok_or_else(|| {
                        PolarsError::ComputeError("R function must return character".into())
                    })?;
                    Ok(format_pl_smallstr!("{}", s))
                }
                _ => Err(PolarsError::ComputeError(
                    "R function must return character".into(),
                )),
            }
        });
        Ok(expr.into())
    }

    fn name_prefix(&self, prefix: &str) -> Result<Self> {
        Ok(self.inner.clone().name().prefix(prefix).into())
    }

    fn name_suffix(&self, suffix: &str) -> Result<Self> {
        Ok(self.inner.clone().name().suffix(suffix).into())
    }

    fn name_to_lowercase(&self) -> Result<Self> {
        Ok(self.inner.clone().name().to_lowercase().into())
    }

    fn name_to_uppercase(&self) -> Result<Self> {
        Ok(self.inner.clone().name().to_uppercase().into())
    }

    fn name_prefix_fields(&self, prefix: &str) -> Result<Self> {
        Ok(self.inner.clone().name().prefix_fields(prefix).into())
    }

    fn name_suffix_fields(&self, suffix: &str) -> Result<Self> {
        Ok(self.inner.clone().name().suffix_fields(suffix).into())
    }
}

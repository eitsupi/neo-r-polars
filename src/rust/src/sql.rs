use crate::{PlRLazyFrame, RPolarsErr};
use polars::sql::SQLContext;
use savvy::{savvy, EnvironmentSexp, Result};

#[savvy]
#[repr(transparent)]
#[derive(Clone)]
pub struct PlRSQLContext {
    pub context: SQLContext,
}

impl From<SQLContext> for PlRSQLContext {
    fn from(context: SQLContext) -> Self {
        PlRSQLContext { context }
    }
}

impl TryFrom<EnvironmentSexp> for &PlRSQLContext {
    type Error = String;

    fn try_from(env: EnvironmentSexp) -> std::result::Result<Self, String> {
        let ptr = env
            .get(".ptr")
            .expect("Failed to get `.ptr` from the object")
            .ok_or("The object is not a valid polars SQL context")?;
        <&PlRSQLContext>::try_from(ptr).map_err(|e| e.to_string())
    }
}

#[savvy]
#[allow(
    clippy::wrong_self_convention,
    clippy::should_implement_trait,
    clippy::len_without_is_empty
)]
impl PlRSQLContext {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Result<PlRSQLContext> {
        Ok(PlRSQLContext {
            context: SQLContext::new(),
        })
    }

    pub fn execute(&mut self, query: &str) -> Result<PlRLazyFrame> {
        Ok(self
            .context
            .execute(query)
            .map_err(RPolarsErr::from)?
            .into())
    }

    pub fn get_tables(&self) -> Result<Vec<String>> {
        Ok(self.context.get_tables())
    }

    pub fn register(&mut self, name: &str, lf: PlRLazyFrame) -> Result<()> {
        self.context.register(name, lf.ldf.clone());
        Ok(())
    }

    pub fn unregister(&mut self, name: &str) -> Result<()> {
        self.context.unregister(name);
        Ok(())
    }
}

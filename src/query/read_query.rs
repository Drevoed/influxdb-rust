//! Read Query Builder returned by InfluxDbQuery::raw_read_query
//!
//! Can only be instantiated by using InfluxDbQuery::raw_read_query

use crate::error::InfluxDbError;
use crate::query::{InfluxDbQuery, ValidQuery};

pub struct InfluxDbReadQuery {
    queries: Vec<String>,
}

impl InfluxDbReadQuery {
    pub(crate) fn build(&self) -> Result<ValidQuery, InfluxDbError> {
        Ok(ValidQuery(self.queries.join(";")))
    }
    /// Creates a new [`InfluxDbReadQuery`]
    pub fn new<S>(query: S) -> Self
    where
        S: Into<String>,
    {
        InfluxDbReadQuery {
            queries: vec![query.into()],
        }
    }

    /// Adds a query to the [`InfluxDbReadQuery`]
    pub fn add<S>(mut self, query: S) -> Self
    where
        S: Into<String>,
    {
        self.queries.push(query.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::query::{create_raw_read_query, InfluxDbQuery, QueryType};

    #[test]
    fn test_read_builder_single_query() {
        let query = create_raw_read_query("SELECT * FROM aachen").build();

        assert_eq!(query.unwrap(), "SELECT * FROM aachen");
    }

    #[test]
    fn test_read_builder_multi_query() {
        let query = create_raw_read_query("SELECT * FROM aachen")
            .add("SELECT * FROM cologne")
            .build();

        assert_eq!(query.unwrap(), "SELECT * FROM aachen;SELECT * FROM cologne");
    }

    #[test]
    fn test_correct_query_type() {
        let query = create_raw_read_query("SELECT * FROM aachen");

        assert_eq!(query.get_type(), QueryType::ReadQuery);
    }
}

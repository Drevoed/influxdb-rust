//! Used to create queries of type [`InfluxDbReadQuery`](crate::query::read_query::InfluxDbReadQuery) or
//! [`InfluxDbWriteQuery`](crate::query::write_query::InfluxDbWriteQuery) which can be executed in InfluxDB
//!
//! # Examples
//!
//! ```rust
//! use influxdb::query::{InfluxDbQuery, Timestamp, create_write_query, create_raw_read_query};
//!
//! let write_query = create_write_query(Timestamp::NOW, "measurement")
//!     .add_field("field1", 5)
//!     .add_tag("author", "Gero")
//!     .build();
//!
//! assert!(write_query.is_ok());
//!
//! let read_query = create_raw_read_query("SELECT * FROM weather")
//!     .build();
//!
//! assert!(read_query.is_ok());
//! ```

pub mod read_query;
pub mod write_query;

use std::fmt;

use crate::error::InfluxDbError;
use crate::query::read_query::InfluxDbReadQuery;
use crate::query::write_query::InfluxDbWriteQuery;

/// Returns a [`InfluxDbWriteQuery`](crate::query::write_query::InfluxDbWriteQuery) builder.
///
/// # Examples
///
/// ```rust
/// use influxdb::query::{create_write_query, Timestamp};
///
/// let _ = create_write_query(Timestamp::NOW, "measurement"); // Is of type [`InfluxDbWriteQuery`](crate::query::write_query::InfluxDbWriteQuery)
/// ```
pub fn create_write_query<S>(timestamp: Timestamp, measurement: S) -> InfluxDbWriteQuery
where
    S: Into<String>,
{
    InfluxDbWriteQuery::new(timestamp, measurement)
}

/// Returns a [`InfluxDbReadQuery`](crate::query::read_query::InfluxDbReadQuery) builder.
///
/// # Examples
///
/// ```rust
/// use influxdb::query::create_raw_read_query;
///
/// let _ = create_raw_read_query("SELECT * FROM weather"); // Is of type [`InfluxDbReadQuery`](crate::query::read_query::InfluxDbReadQuery)
/// ```
pub fn create_raw_read_query<S>(read_query: S) -> InfluxDbReadQuery
where
    S: Into<String>,
{
    InfluxDbReadQuery::new(read_query)
}

#[derive(PartialEq)]
pub enum Timestamp {
    NOW,
    NANOSECONDS(usize),
    MICROSECONDS(usize),
    MILLISECONDS(usize),
    SECONDS(usize),
    MINUTES(usize),
    HOURS(usize),
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Timestamp::*;
        match self {
            NOW => write!(f, ""),
            NANOSECONDS(ts) | MICROSECONDS(ts) | MILLISECONDS(ts) | SECONDS(ts) | MINUTES(ts)
            | HOURS(ts) => write!(f, "{}", ts),
        }
    }
}

/// Internal Enum used to decide if a `POST` or `GET` request should be sent to InfluxDB. See [InfluxDB Docs](https://docs.influxdata.com/influxdb/v1.7/tools/api/#query-http-endpoint).
pub enum InfluxDbQuery {
    Write(InfluxDbWriteQuery),
    Read(InfluxDbReadQuery),
}

impl InfluxDbQuery {
    pub fn build(&self) -> Result<ValidQuery, InfluxDbError> {
        use InfluxDbQuery::*;

        match self {
            Write(write_query) => write_query.build(),
            Read(read_query) => read_query.build(),
        }
    }
}

#[derive(Debug)]
#[doc(hidden)]
pub struct ValidQuery(String);
impl ValidQuery {
    pub fn get(self) -> String {
        self.0
    }
}
impl<T> From<T> for ValidQuery
where
    T: Into<String>,
{
    fn from(string: T) -> Self {
        Self(string.into())
    }
}
impl PartialEq<String> for ValidQuery {
    fn eq(&self, other: &String) -> bool {
        &self.0 == other
    }
}
impl PartialEq<&str> for ValidQuery {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

#[cfg(test)]
mod tests {
    use crate::query::{Timestamp, ValidQuery};

    #[test]
    fn test_equality_str() {
        assert_eq!(ValidQuery::from("hello"), "hello");
    }

    #[test]
    fn test_equality_string() {
        assert_eq!(ValidQuery::from("hello"), String::from("hello"));
    }

    #[test]
    fn test_format_for_timestamp_now() {
        assert!(format!("{}", Timestamp::NOW) == String::from(""));
    }

    #[test]
    fn test_format_for_timestamp_else() {
        assert!(format!("{}", Timestamp::NANOSECONDS(100)) == String::from("100"));
    }
}

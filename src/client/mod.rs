//! Client which can read and write data from InfluxDB.
//!
//! # Arguments
//!
//!  * `url`: The URL where InfluxDB is running (ex. `http://localhost:8086`).
//!  * `database`: The Database against which queries and writes will be run.
//!
//! # Examples
//!
//! ```rust
//! use influxdb::client::InfluxDbClient;
//!
//! let client = InfluxDbClient::new("http://localhost:8086", "test");
//!
//! assert_eq!(client.database_name(), "test");
//! ```

use futures::prelude::*;
use reqwest::Client;
use reqwest::{StatusCode, Url};

use crate::error::InfluxDbError;
use crate::query::{InfluxDbQuery};

#[derive(Clone, Debug)]
/// Internal Authentication representation
pub(crate) struct InfluxDbAuthentication {
    pub username: String,
    pub password: String,
}

#[derive(Clone, Debug)]
/// Internal Representation of a Client
pub struct InfluxDbClient {
    url: String,
    database: String,
    auth: Option<InfluxDbAuthentication>,
    pub(crate) inner_client: Client
}

impl Into<Vec<(String, String)>> for InfluxDbClient {
    fn into(self) -> Vec<(String, String)> {
        let mut vec: Vec<(String, String)> = Vec::new();
        vec.push(("db".to_string(), self.database));
        if let Some(auth) = self.auth {
            vec.push(("u".to_string(), auth.username));
            vec.push(("p".to_string(), auth.password));
        }
        vec
    }
}

impl<'a> Into<Vec<(String, String)>> for &'a InfluxDbClient {
    fn into(self) -> Vec<(String, String)> {
        let mut vec: Vec<(String, String)> = Vec::new();
        vec.push(("db".to_string(), self.database.to_owned()));
        if let Some(auth) = &self.auth {
            vec.push(("u".to_string(), auth.username.to_owned()));
            vec.push(("p".to_string(), auth.password.to_owned()));
        }
        vec
    }
}

impl InfluxDbClient {
    /// Instantiates a new [`InfluxDbClient`](crate::client::InfluxDbClient)
    ///
    /// # Arguments
    ///
    ///  * `url`: The URL where InfluxDB is running (ex. `http://localhost:8086`).
    ///  * `database`: The Database against which queries and writes will be run.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use influxdb::client::InfluxDbClient;
    ///
    /// let _client = InfluxDbClient::new("http://localhost:8086", "test");
    /// ```
    pub fn new<S1, S2>(url: S1, database: S2) -> Self
    where
        S1: ToString,
        S2: ToString,
    {
        InfluxDbClient {
            url: url.to_string(),
            database: database.to_string(),
            auth: None,
            inner_client: Client::new()
        }
    }

    /// Add authentication/authorization information to [`InfluxDbClient`](crate::client::InfluxDbClient)
    ///
    /// # Arguments
    ///
    /// * username: The Username for InfluxDB.
    /// * password: THe Password for the user.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use influxdb::client::InfluxDbClient;
    ///
    /// let _client = InfluxDbClient::new("http://localhost:9086", "test").with_auth("admin", "password");
    /// ```
    pub fn with_auth<'a, S1, S2>(mut self, username: S1, password: S2) -> Self
    where
        S1: ToString,
        S2: ToString,
    {
        self.auth = Some(InfluxDbAuthentication {
            username: username.to_string(),
            password: password.to_string(),
        });
        self
    }

    /// Returns the name of the database the client is using
    pub fn database_name(&self) -> &str {
        &self.database
    }

    /// Returns the URL of the InfluxDB installation the client is using
    pub fn database_url(&self) -> &str {
        &self.url
    }

    /// Pings the InfluxDB Server
    ///
    /// Returns a tuple of build type and version number
    pub async fn ping(&self) -> Result<(String, String), InfluxDbError> {
        let res = self.inner_client
            .get(format!("{}/ping", self.url).as_str())
            .send()
            .await
            .map_err(|err| InfluxDbError::ProtocolError {
                error: format!("{}", err)
            })?;
        let version = res
            .headers()
            .get("X-Influxdb-Version")
            .unwrap()
            .to_str()
            .unwrap();
        let build = res
            .headers()
            .get("X-Influxdb-Build")
            .unwrap()
            .to_str()
            .unwrap();
        Ok((String::from(build), String::from(version)))
    }

    /// Sends a [`InfluxDbReadQuery`](crate::query::read_query::InfluxDbReadQuery) or [`InfluxDbWriteQuery`](crate::query::write_query::InfluxDbWriteQuery) to the InfluxDB Server.
    ///
    /// A version capable of parsing the returned string is available under the [serde_integration](crate::integrations::serde_integration)
    ///
    /// # Arguments
    ///
    ///  * `q`: Query of type [`InfluxDbReadQuery`](crate::query::read_query::InfluxDbReadQuery) or [`InfluxDbWriteQuery`](crate::query::write_query::InfluxDbWriteQuery)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use influxdb::client::InfluxDbClient;
    /// use influxdb::query::{InfluxDbQuery, Timestamp};
    ///
    /// let client = InfluxDbClient::new("http://localhost:8086", "test");
    /// let mut runtime = tokio::runtime::Runtime::new().unwrap();
    /// runtime.block_on(async {
    ///     client.query(&InfluxDbQuery::write_query(Timestamp::NOW, "weather")
    ///         .add_field("temperature", 82)
    ///     )
    /// });
    /// ```
    /// # Errors
    ///
    /// If the function can not finish the query,
    /// a [`InfluxDbError`] variant will be returned.
    ///
    /// [`InfluxDbError`]: enum.InfluxDbError.html
    pub async fn query(&self, q: &InfluxDbQuery) -> Result<String, InfluxDbError>
    {
        let basic_parameters: Vec<(String, String)> = self.into();

        let query = match q.build() {
            Err(err) => {
                let error = InfluxDbError::InvalidQueryError {
                    error: format!("{}", err),
                };
                return Err(error);
            }
            Ok(query) => query,
        };

        let client = match q {
            InfluxDbQuery::Read(_) => {
                let read_query = query.get();
                let mut url = match Url::parse_with_params(
                    &format!("{url}/query", url = self.database_url()),
                    basic_parameters
                ) {
                    Ok(url) => url,
                    Err(err) => {
                        let error = InfluxDbError::UrlConstructionError {error: format!("{}", err)};
                        return Err(error)
                    }
                };
                url.query_pairs_mut().append_pair("q", &read_query);

                if read_query.contains("SELECT") || read_query.contains("SHOW") {
                    self.inner_client.get(url)
                } else {
                    self.inner_client.post(url)
                }
            },
            InfluxDbQuery::Write(write_query) => {
                let mut url = match Url::parse_with_params(
                    &format!("{url}/write", url = self.database_url()),
                    basic_parameters
                ) {
                    Ok(url) => url,
                    Err(err) => {
                        let error = InfluxDbError::InvalidQueryError{error: format!("{}", err)};
                        return Err(error)
                    }
                };
                url.query_pairs_mut().append_pair("precision", &write_query.get_precision());

                self.inner_client.post(url).body(query.get())
            }
        };
        let res = client.send().await.map_err(|err| InfluxDbError::ConnectionError {error: err})?;
        match res.status() {
            StatusCode::UNAUTHORIZED => return Err(InfluxDbError::AuthorizationError),
            StatusCode::FORBIDDEN => return Err(InfluxDbError::AuthenticationError),
            _ => {}
        };
        let bytes = res.bytes().await.map_err(|err| InfluxDbError::ConnectionError {error: err})?;
        if let Ok(utf8) = std::str::from_utf8(&bytes) {
            let s = utf8.to_owned();

            if s.contains("\"error\"") {
                return Err(InfluxDbError::DatabaseError {
                    error: format!("influxdb error: \"{}\"", s),
                });
            }
            Ok(s)
        } else {
            Err(InfluxDbError::DeserializationError {
                error: format!("response could not be converted to UTF-8 encoded string")
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::client::InfluxDbClient;

    #[test]
    fn test_fn_database() {
        let client = InfluxDbClient::new("http://localhost:8068", "database");
        assert_eq!("database", client.database_name());
    }

    #[test]
    fn test_with_auth() {
        let client = InfluxDbClient::new("http://localhost:8068", "database");
        assert_eq!(client.url, "http://localhost:8068");
        assert_eq!(client.database, "database");
        assert!(client.auth.is_none());
        let with_auth = client.with_auth("username", "password");
        assert!(with_auth.auth.is_some());
        let auth = with_auth.auth.unwrap();
        assert_eq!(&auth.username, "username");
        assert_eq!(&auth.password, "password");
    }

    #[test]
    fn test_into_impl() {
        let client = InfluxDbClient::new("http://localhost:8068", "database");
        assert!(client.auth.is_none());
        let basic_parameters: Vec<(String, String)> = client.into();
        assert_eq!(
            vec![("db".to_string(), "database".to_string())],
            basic_parameters
        );

        let with_auth = InfluxDbClient::new("http://localhost:8068", "database")
            .with_auth("username", "password");
        let basic_parameters_with_auth: Vec<(String, String)> = with_auth.into();
        assert_eq!(
            vec![
                ("db".to_string(), "database".to_string()),
                ("u".to_string(), "username".to_string()),
                ("p".to_string(), "password".to_string())
            ],
            basic_parameters_with_auth
        );

        let client = InfluxDbClient::new("http://localhost:8068", "database");
        assert!(client.auth.is_none());
        let basic_parameters: Vec<(String, String)> = (&client).into();
        assert_eq!(
            vec![("db".to_string(), "database".to_string())],
            basic_parameters
        );

        let with_auth = InfluxDbClient::new("http://localhost:8068", "database")
            .with_auth("username", "password");
        let basic_parameters_with_auth: Vec<(String, String)> = (&with_auth).into();
        assert_eq!(
            vec![
                ("db".to_string(), "database".to_string()),
                ("u".to_string(), "username".to_string()),
                ("p".to_string(), "password".to_string())
            ],
            basic_parameters_with_auth
        );
    }
}

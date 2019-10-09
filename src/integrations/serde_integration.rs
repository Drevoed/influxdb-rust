//! Serde Integration for InfluxDB. Provides deserialization of query returns.
//!
//! When querying multiple series in the same query (e.g. with a regex query), it might be desirable to flat map
//! the resulting series into a single `Vec` like so. The example assumes, that there are weather readings in multiple
//! series named `weather_<city_name>` (e.g. `weather_berlin`, or `weather_london`). Since we're using a Regex query,
//! we don't actually know which series will be returned. To assign the city name to the series, we can use the series
//! `name`, InfluxDB provides alongside query results.
//!
//! ```rust,no_run
//! use futures::prelude::*;
//! use influxdb::client::InfluxDbClient;
//! use influxdb::query::create_raw_read_query;
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct WeatherWithoutCityName {
//!     temperature: i32,
//! }
//!
//! #[derive(Deserialize)]
//! struct Weather {
//!     city_name: String,
//!     weather: WeatherWithoutCityName,
//! }
//!
//! let mut rt = tokio::runtime::current_thread::Runtime::new().unwrap();
//! let client = InfluxDbClient::new("http://localhost:8086", "test");
//! let query = create_raw_read_query(
//!     "SELECT temperature FROM /weather_[a-z]*$/ WHERE time > now() - 1m ORDER BY DESC",
//! );
//! let _result = rt
//!     .block_on(client.json_query(query))
//!     .map(|mut db_result| db_result.deserialize_next::<WeatherWithoutCityName>())
//!     .map(|it| {
//!         it.map(|series_vec| {
//!             series_vec
//!                 .series
//!                 .into_iter()
//!                 .map(|mut city_series| {
//!                     let city_name =
//!                         city_series.name.split("_").collect::<Vec<&str>>().remove(2);
//!                     Weather {
//!                         weather: city_series.values.remove(0),
//!                         city_name: city_name.to_string(),
//!                     }
//!                 })
//!                 .collect::<Vec<Weather>>()
//!         })
//!     });
//! ```

use crate::client::InfluxDbClient;

use serde::de::DeserializeOwned;

use futures::{Future};
use reqwest::{StatusCode, Url};

use serde::Deserialize;
use serde_json;

use crate::error::InfluxDbError;

use crate::query::read_query::InfluxDbReadQuery;
use crate::query::InfluxDbQuery;


#[derive(Deserialize)]
#[doc(hidden)]
struct _DatabaseError {
    error: String,
}

#[derive(Deserialize, Debug)]
#[doc(hidden)]
pub struct DatabaseQueryResult {
    pub results: Vec<serde_json::Value>,
}

impl DatabaseQueryResult {
    pub async fn deserialize_next<T: DeserializeOwned>(
        &mut self,
    ) -> Result<InfluxDbReturn<T>, InfluxDbError>
    {
        match serde_json::from_value::<InfluxDbReturn<T>>(self.results.remove(0)) {
            Ok(item) => Ok(item),
            Err(err) => Err(InfluxDbError::DeserializationError {
                error: format!("could not deserialize: {}", err),
            }),
        }
    }
}

#[derive(Deserialize, Debug)]
#[doc(hidden)]
pub struct InfluxDbReturn<T> {
    pub series: Vec<InfluxDbSeries<T>>,
}

#[derive(Deserialize, Debug)]
/// Represents a returned series from InfluxDB
pub struct InfluxDbSeries<T> {
    pub name: String,
    pub values: Vec<T>,
}

impl InfluxDbClient {
    pub async fn json_query(
        &self,
        q: InfluxDbReadQuery,
    ) -> Result<DatabaseQueryResult, InfluxDbError> {
        let query = q.build().unwrap();
        let basic_parameters: Vec<(String, String)> = self.into();
        let client = {
            let read_query = query.get();

            let mut url = match Url::parse_with_params(
                &format!("{url}/query", url = self.database_url()),
                basic_parameters,
            ) {
                Ok(url) => url,
                Err(err) => {
                    let error = InfluxDbError::UrlConstructionError {
                        error: format!("{}", err),
                    };
                    return Err(error);
                }
            };
            url.query_pairs_mut().append_pair("q", &read_query.clone());

            if read_query.contains("SELECT") || read_query.contains("SHOW") {
                self.inner_client.get(url)
            } else {
                let error = InfluxDbError::InvalidQueryError {
                    error: String::from(
                        "Only SELECT and SHOW queries supported with JSON deserialization",
                    ),
                };
                return Err(error);
            }
        };

        let res = client.send().await.map_err(|err| InfluxDbError::ConnectionError {error: err})?;
        match res.status() {
            StatusCode::UNAUTHORIZED => {
                return Err(InfluxDbError::AuthorizationError)
            }
            StatusCode::FORBIDDEN => {
                return Err(InfluxDbError::AuthenticationError)
            }
            _ => {}
        }

        let bytes = res.bytes().await.map_err(|err| InfluxDbError::ConnectionError {error: err})?;

        if let Ok(error) = serde_json::from_slice::<_DatabaseError>(&bytes) {
            return Err(InfluxDbError::DatabaseError {error: error.error})
        } else {
            let deserialized = serde_json::from_slice::<DatabaseQueryResult>(&bytes)
                .map_err(|e| InfluxDbError::DeserializationError {error: format!("serde error: {}", e)})?;
            Ok(deserialized)
        }
    }
}

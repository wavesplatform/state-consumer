use crate::{config::PostgresConfig, error::AppError};
use anyhow::{Error, Result};
use diesel::{Connection, PgConnection};

// todo max connections
pub fn new(config: &PostgresConfig) -> Result<PgConnection> {
    let db_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        config.user, config.password, config.host, config.port, config.database
    );

    PgConnection::establish(&db_url).map_err(|err| Error::new(AppError::ConnectionError(err)))
}

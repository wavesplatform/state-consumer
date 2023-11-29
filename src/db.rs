use crate::config::PostgresConfig;

use diesel::{pg::PgConnection, r2d2::ConnectionManager};
use r2d2::Pool;
use r2d2::PooledConnection;
use std::time::Duration;

pub type PgPool = Pool<ConnectionManager<PgConnection>>;
pub type PooledPgConnection = PooledConnection<ConnectionManager<PgConnection>>;

pub fn pool(config: &PostgresConfig) -> anyhow::Result<PgPool> {
    let manager = ConnectionManager::<PgConnection>::new(config.database_url());
    Ok(Pool::builder()
        .max_size(config.poolsize)
        .idle_timeout(Some(Duration::from_secs(300)))
        .build(manager)?)
}

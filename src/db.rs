use crate::config::PostgresConfig;

use diesel::{pg::PgConnection, r2d2::ConnectionManager};
use r2d2::Pool;
use r2d2::PooledConnection;
use std::time::Duration;

pub type PgPool = Pool<ConnectionManager<PgConnection>>;
pub type PooledPgConnection = PooledConnection<ConnectionManager<PgConnection>>;

pub fn pool(config: &PostgresConfig) -> anyhow::Result<PgPool> {
    let db_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        config.user, config.password, config.host, config.port, config.database
    );
    let manager = ConnectionManager::<PgConnection>::new(db_url);
    Ok(Pool::builder()
        .max_size(config.poolsize)
        .idle_timeout(Some(Duration::from_secs(300)))
        .build(manager)?)
}

use crate::{config::PostgresConfig, error::Error};
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool};

pub type PgPool = Pool<ConnectionManager<PgConnection>>;

// todo max connections
pub fn pool(config: &PostgresConfig) -> Result<PgPool, Error> {
    let db_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        config.user, config.password, config.host, config.port, config.database
    );

    let manager = ConnectionManager::<PgConnection>::new(db_url);
    Ok(Pool::builder().build(manager)?)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config::tests::POSTGRES_LOCAL;
    use once_cell::sync::Lazy;

    pub static PG_POOL_LOCAL: Lazy<PgPool> = Lazy::new(|| pool(&POSTGRES_LOCAL).unwrap());
}

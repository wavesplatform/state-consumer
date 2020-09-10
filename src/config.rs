use crate::{data_entries, error::Error};
use serde::Deserialize;

fn default_port() -> u16 {
    8080
}

fn default_pgport() -> u16 {
    5432
}

fn default_pgpool() -> u8 {
    4
}

fn default_blocks_per_request() -> usize {
    256
}

fn default_starting_height() -> u32 {
    0
}

#[derive(Deserialize, Debug, Clone)]
struct ConfigFlat {
    #[serde(default = "default_port")]
    pub port: u16,

    // service's postgres
    pub pghost: String,
    #[serde(default = "default_pgport")]
    pub pgport: u16,
    pub pgdatabase: String,
    pub pguser: String,
    pub pgpassword: String,
    #[serde(default = "default_pgpool")]
    pub pgpool: u8,

    pub blockchain_updates_url: String,
    #[serde(default = "default_blocks_per_request")]
    pub blocks_per_request: usize,
    #[serde(default = "default_starting_height")]
    pub starting_height: u32,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub data_entries: data_entries::Config,
    pub postgres: PostgresConfig,
}

#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub pool_size: u8,
}

pub fn load() -> Result<Config, Error> {
    let config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        port: config_flat.port,
        data_entries: data_entries::Config {
            blockchain_updates_url: config_flat.blockchain_updates_url,
            blocks_per_request: config_flat.blocks_per_request,
            starting_height: config_flat.starting_height,
        },
        postgres: PostgresConfig {
            host: config_flat.pghost,
            port: config_flat.pgport,
            database: config_flat.pgdatabase,
            user: config_flat.pguser,
            password: config_flat.pgpassword,
            pool_size: config_flat.pgpool,
        },
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::PostgresConfig;
    use crate::data_entries;
    use once_cell::sync::Lazy;

    pub static DATA_ENTRIES_STAGENET: Lazy<data_entries::Config> =
        Lazy::new(|| data_entries::Config {
            blockchain_updates_url: "https://blockchain-updates-stagenet.waves.exchange".to_owned(),
            blocks_per_request: 256,
            starting_height: 0,
        });

    pub static POSTGRES_LOCAL: Lazy<PostgresConfig> = Lazy::new(|| PostgresConfig {
        host: "localhost".to_owned(),
        port: 5432,
        database: "marketmaking".to_owned(),
        password: "postgres".to_owned(),
        user: "postgres".to_owned(),
        pool_size: 2,
    });
}

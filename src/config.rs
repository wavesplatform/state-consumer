use crate::{data_entries, error::Error};
use serde::Deserialize;

fn default_port() -> u16 {
    8080
}

fn default_pgport() -> u16 {
    5432
}

fn default_updates_per_request() -> usize {
    256
}

fn default_max_wait_time_in_secs() -> u64 {
    5
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

    pub blockchain_updates_url: String,
    #[serde(default = "default_updates_per_request")]
    pub updates_per_request: usize,
    #[serde(default = "default_max_wait_time_in_secs")]
    pub max_wait_time_in_secs: u64,
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
}

pub fn load() -> Result<Config, Error> {
    let config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        port: config_flat.port,
        data_entries: data_entries::Config {
            blockchain_updates_url: config_flat.blockchain_updates_url,
            updates_per_request: config_flat.updates_per_request,
            max_wait_time_in_secs: config_flat.max_wait_time_in_secs,
            starting_height: config_flat.starting_height,
        },
        postgres: PostgresConfig {
            host: config_flat.pghost,
            port: config_flat.pgport,
            database: config_flat.pgdatabase,
            user: config_flat.pguser,
            password: config_flat.pgpassword,
        },
    })
}

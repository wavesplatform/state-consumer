use crate::data_entries;
use anyhow::Result;
use serde::Deserialize;

fn default_port() -> u16 {
    8080
}

fn default_metrics_port() -> u16 {
    9090
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

fn default_start_rollback_depth() -> u32 {
    1
}

fn default_pgpoolsize() -> u32 {
    2
}

#[derive(Deserialize, Debug, Clone)]
struct ConfigFlat {
    #[serde(default = "default_port")]
    port: u16,
    #[serde(default = "default_metrics_port")]
    metrics_port: u16,

    // service's postgres
    pghost: String,
    #[serde(default = "default_pgport")]
    pgport: u16,
    pgdatabase: String,
    pguser: String,
    pgpassword: String,
    #[serde(default = "default_pgpoolsize")]
    pgpoolsize: u32,

    blockchain_updates_url: String,
    #[serde(default = "default_updates_per_request")]
    updates_per_request: usize,
    #[serde(default = "default_max_wait_time_in_secs")]
    max_wait_time_in_secs: u64,

    #[serde(default = "default_start_rollback_depth")]
    start_rollback_depth: u32,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub metrics_port: u16,
    pub data_entries: data_entries::Config,
    pub start_rollback_depth: u32,
    pub postgres: PostgresConfig,
}

#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub poolsize: u32
}

impl PostgresConfig {
    pub fn database_url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.database
        )
    }
}

pub fn load() -> Result<Config> {
    let config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        port: config_flat.port,
        metrics_port: config_flat.metrics_port,
        start_rollback_depth: config_flat.start_rollback_depth,
        data_entries: data_entries::Config {
            blockchain_updates_url: config_flat.blockchain_updates_url,
            updates_per_request: config_flat.updates_per_request,
            max_wait_time_in_secs: config_flat.max_wait_time_in_secs,
        },
        postgres: PostgresConfig {
            host: config_flat.pghost,
            port: config_flat.pgport,
            database: config_flat.pgdatabase,
            user: config_flat.pguser,
            password: config_flat.pgpassword,
            poolsize: config_flat.pgpoolsize
        },
    })
}

#[macro_use]
extern crate diesel;

pub mod config;
pub mod data_entries;
pub mod db;
pub mod error;
pub mod readiness;
pub mod schema;

use anyhow::Result;
use data_entries::{repo::DataEntriesRepoImpl, updates::DataEntriesSourceImpl};
use std::sync::Arc;
use tokio::select;
use wavesexchange_log::{error, info};
use wavesexchange_warp::MetricsWarpBuilder;
use std::time::Duration;

const MAX_BLOCK_AGE: Duration = Duration::from_secs(600);

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SyncMode {
    Historical,
    Realtime,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::load()?;

    let conn = db::pool(&config.postgres)?;
    let data_entries_repo = Arc::new(DataEntriesRepoImpl::new(conn));

    let updates_repo =
        DataEntriesSourceImpl::new(&config.data_entries.blockchain_updates_url).await?;

    info!("Starting state-consumer");

    let (sync_mode_tx, sync_mode_rx) = tokio::sync::mpsc::unbounded_channel();

    let readiness_channel = readiness::channel(
        data_entries_repo.clone(),
        sync_mode_rx,
        MAX_BLOCK_AGE,
    );

    let consumer = data_entries::daemon::start(
        updates_repo,
        data_entries_repo.clone(),
        config.data_entries.updates_per_request,
        config.data_entries.max_wait_time_in_secs,
        config.start_rollback_depth,
        sync_mode_tx,
    );

    let metrics = MetricsWarpBuilder::new()
        .with_metrics_port(config.metrics_port)
        .with_readiness_channel(readiness_channel)
        .run_async();

    select! {
        Err(err) = consumer => {
            error!("{}", err);
            panic!("{}", err);
        },
        _ = metrics => {
            error!("Metrics stopped");
        }
    };
    Ok(())
}

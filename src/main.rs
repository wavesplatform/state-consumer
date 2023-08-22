#[macro_use]
extern crate diesel;

pub mod config;
pub mod data_entries;
pub mod db;
pub mod error;
pub mod schema;

use anyhow::Result;
use data_entries::{repo::DataEntriesRepoImpl, updates::DataEntriesSourceImpl};
use std::sync::Arc;
use tokio::select;
use wavesexchange_log::{error, info};
use wavesexchange_warp::MetricsWarpBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::load()?;

    let conn = db::new(&config.postgres)?;

    let data_entries_repo = Arc::new(DataEntriesRepoImpl::new(conn));

    let updates_repo =
        DataEntriesSourceImpl::new(&config.data_entries.blockchain_updates_url).await?;

    info!("Starting state-consumer");

    let consumer = data_entries::daemon::start(
        updates_repo,
        data_entries_repo.clone(),
        config.data_entries.updates_per_request,
        config.data_entries.max_wait_time_in_secs,
        config.start_rollback_depth,
    );

    let api = data_entries::api::start(data_entries_repo.clone(), config.port);

    let metrics = MetricsWarpBuilder::new()
        .with_metrics_port(config.metrics_port)
        .run_async();

    select! {
        Err(err) = consumer => {
            error!("{}", err);
            panic!("{}", err);
        },
        Err(err) = api => {
            error!("{}", err);
            panic!("{}", err);
        },
        _ = metrics => {
            error!("Metrics stopped");
        }
    };
    Ok(())
}

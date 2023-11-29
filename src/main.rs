#[macro_use]
extern crate diesel;

pub mod config;
pub mod data_entries;
pub mod db;
pub mod error;
pub mod schema;

use anyhow::Result;
use data_entries::{repo::PgDataEntriesRepo, updates::DataEntriesSourceImpl};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use wavesexchange_liveness::channel;
use wavesexchange_log::{error, info};
use wavesexchange_warp::MetricsWarpBuilder;

const POLL_INTERVAL_SECS: u64 = 60;
const MAX_BLOCK_AGE: Duration = Duration::from_secs(300);

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::load()?;

    let pool = db::pool(&config.postgres)?;
    let data_entries_repo = Arc::new(PgDataEntriesRepo::new(pool));

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

    let db_url = config.postgres.database_url();
    let readiness_channel = channel(db_url, POLL_INTERVAL_SECS, MAX_BLOCK_AGE);

    let metrics = tokio::spawn(async move {
        MetricsWarpBuilder::new()
            .with_metrics_port(config.metrics_port)
            .with_readiness_channel(readiness_channel)
            .run_async()
            .await
    });

    select! {
        Err(err) = consumer => {
            error!("{}", err);
            panic!("{}", err);
        },
        result = metrics => {
            if let Err(err) = result {
                error!("Metrics failed: {:?}", err);
            } else {
                error!("Metrics stopped");
            }
        }
    };
    Ok(())
}

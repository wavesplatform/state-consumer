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
use wavesexchange_log::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::load()?;

    let conn = db::new(&config.postgres)?;

    let data_entries_repo = Arc::new(DataEntriesRepoImpl::new(conn));

    let updates_repo =
        DataEntriesSourceImpl::new(&config.data_entries.blockchain_updates_url).await?;

    info!("Starting state-consumer");

    if let Err(err) = data_entries::daemon::start(
        updates_repo,
        data_entries_repo,
        config.data_entries.updates_per_request,
        config.data_entries.max_wait_time_in_secs,
    )
    .await
    {
        error!("{}", err);
        panic!("{}", err);
    }
    Ok(())
}

#[macro_use]
extern crate diesel;

pub mod config;
pub mod data_entries;
pub mod db;
pub mod error;
pub mod log;
pub mod schema;

use data_entries::{repo::DataEntriesRepoImpl, updates::DataEntriesSourceImpl};
use log::APP_LOG;
use slog::info;
use tokio::try_join;

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    let config = config::load()?;

    let service_pg_pool = db::pool(&config.postgres)?;

    let data_entries_repo = DataEntriesRepoImpl::new(service_pg_pool.clone());

    let updates_repo =
        DataEntriesSourceImpl::new(&config.data_entries.blockchain_updates_url).await?;

    let data_entries_daemon_join_handle = {
        info!(APP_LOG, "Starting data_entries daemon");
        let starting_height = config.data_entries.starting_height.clone();
        let blocks_per_request = config.data_entries.blocks_per_request.clone();
        let data_entries_repo = data_entries_repo.clone();
        tokio::spawn(async move {
            data_entries::daemon::start(
                updates_repo,
                data_entries_repo,
                starting_height,
                blocks_per_request,
            )
            .await;
        })
    };

    if let Err(err) = try_join!(data_entries_daemon_join_handle) {
        panic!(err);
    }

    Ok(())
}

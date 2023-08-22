use super::repo::DataEntriesRepoImpl;
use crate::data_entries::DataEntriesRepo;
use std::sync::Arc;
use warp::Filter;

pub async fn start(repo: Arc<DataEntriesRepoImpl>, port: u16) -> Result<(), anyhow::Error> {
    let with_repo = warp::any().map(move || repo.clone());

    let routes = warp::path("last_block_timestamp")
        .and(warp::get())
        .and(with_repo)
        .and_then(|repo: Arc<DataEntriesRepoImpl>| async move {
            match repo.get_last_block_timestamp() {
                Ok(Some(timestamp)) => Ok(warp::reply::json(&timestamp)),
                Ok(None) => Err(warp::reject::not_found()),
                Err(_) => Err(warp::reject::not_found()),
            }
        });

    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
    Ok(())
}

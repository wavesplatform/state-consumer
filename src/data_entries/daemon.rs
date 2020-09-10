use super::{DataEntriesRepo, DataEntriesSource, DeletableDataEntry};
use crate::log::APP_LOG;
use slog::info;
use std::time::Duration;

/*
Algorithm:
1. Get last height from db
2. Request a batch of T block updates from last_height
3. Transform updates into rows for insertion
4. Insert updates
5. Upadte last handled height in db
5. If less than T updates have been received, sleep for some time before continuing
*/

pub async fn start<T: DataEntriesSource + Send + Sync, U: DataEntriesRepo + Send + Sync>(
    updates_src: T,
    mut dbw: U,
    min_height: u32,
    blocks_per_request: usize,
) {
    loop {
        let last_handled_height = dbw.get_last_handled_height().unwrap();

        let from_height = if last_handled_height < min_height {
            min_height
        } else {
            last_handled_height + 1
        };
        let to_height = from_height + (blocks_per_request as u32) - 1;

        info!(
            APP_LOG,
            "updating data entries from {} to {}", from_height, to_height
        );

        let updates = updates_src
            .fetch_updates(from_height, to_height)
            .await
            .unwrap_or_else(|_| (from_height as i32, Vec::new(), Vec::new()));

        if updates.1.len() > 0 {
            info!(APP_LOG, "insert updates {}", updates.1.len());
            dbw.insert_entries(&updates.1).unwrap();
        }

        if updates.2.len() > 0 {
            let to_delete: Vec<DeletableDataEntry> = updates
                .1
                .clone()
                .into_iter()
                .map(|dde| DeletableDataEntry {
                    address: dde.address,
                    key: dde.key,
                })
                .collect();
            dbw.delete_entries(&to_delete).unwrap();
        }

        let last_updated_height = updates.0;

        dbw.set_last_handled_height(last_updated_height as u32)
            .unwrap();

        let inserted = updates.1.len();
        let deleted = updates.2.len();


        info!(APP_LOG, "last updated height: {}", last_updated_height);
        info!(APP_LOG, "inserted {} data entries", inserted);
        info!(APP_LOG, "deleted {} data entries", deleted);

        // if inserted + deleted == 0 {
        //     tokio::time::delay_for(Duration::from_secs(5)).await;
        // }
    }
}

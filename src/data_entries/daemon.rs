use super::{
    repo::DataEntriesRepoImpl, BlockMicroblock, BlockMicroblockAppend, BlockchainUpdate,
    DataEntriesRepo, DataEntriesSource, DataEntry, DataEntryUpdate, InsertableDataEntry,
    FRAGMENT_SEPARATOR, INTEGER_SEPARATOR, RE, STRING_SEPARATOR,
};
use crate::error::Error;
use crate::log::APP_LOG;
use itertools::Itertools;
use slog::{error, info};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/*
Algorithm:
1. Get last height from db
2. Request a batch of T block updates from last_height
3. Transform updates into rows for insertion
4. Insert updates
5. Upadte last handled height in db
5. If less than T updates have been received, sleep for some time before continuing
*/

enum UpdatesItem {
    Blocks(Vec<BlockMicroblockAppend>),
    Microblock(BlockMicroblockAppend),
    Rollback(String),
}

struct BlockUidWithDataEntry {
    block_uid: i64,
    data_entry: DataEntry,
}

pub async fn start<T: DataEntriesSource + Send + Sync, U: DataEntriesRepo>(
    updates_src: T,
    dbw: Arc<U>,
    min_height: u32,
    blocks_per_request: usize,
) -> Result<(), Error> {
    let mut result = Ok(());

    while let Ok(_) = result {
        let last_handled_height = dbw.get_last_handled_height()?;

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

        let mut start = Instant::now();
        let updates_with_height = updates_src.fetch_updates(from_height, to_height).await?;
        info!(
            APP_LOG,
            "updates were received in {} secs",
            start.elapsed().as_secs()
        );
        start = Instant::now();

        let last_updated_height = updates_with_height.height;

        result = dbw.transaction(|conn| {
            let dbw = Arc::new(Mutex::new(DataEntriesRepoImpl::new(conn.clone())));

            updates_with_height
                .updates
                .into_iter()
                .fold::<&mut Vec<UpdatesItem>, _>(&mut vec![], |acc, cur| match cur {
                    BlockchainUpdate::Block(b) => {
                        let len = acc.len();
                        if acc.len() > 0 {
                            match acc.iter_mut().nth(len as usize - 1).unwrap() {
                                UpdatesItem::Blocks(v) => {
                                    v.push(b);
                                    acc
                                }
                                UpdatesItem::Microblock(_) | UpdatesItem::Rollback(_) => {
                                    acc.push(UpdatesItem::Blocks(vec![b]));
                                    acc
                                }
                            }
                        } else {
                            acc.push(UpdatesItem::Blocks(vec![b]));
                            acc
                        }
                    }
                    BlockchainUpdate::Microblock(mba) => {
                        acc.push(UpdatesItem::Microblock(mba));
                        acc
                    }
                    BlockchainUpdate::Rollback(sig) => {
                        acc.push(UpdatesItem::Rollback(sig));
                        acc
                    }
                })
                .into_iter()
                // rewrite to handle error on each update processing
                .fold(Ok(()), |_, update_item| match update_item {
                    UpdatesItem::Blocks(bs) => {
                        squash_microblocks(dbw.clone())?;
                        append_blocks_or_microblocks(dbw.clone(), bs.as_ref())
                    }
                    UpdatesItem::Microblock(mba) => {
                        squash_microblocks(dbw.clone())?;
                        append_blocks_or_microblocks(dbw.clone(), &vec![mba.to_owned()])
                    }
                    UpdatesItem::Rollback(sig) => {
                        let block_uid = dbw.try_lock().unwrap().get_block_uid(&sig)?;
                        dbw.lock().unwrap().rollback_blocks_microblocks(&block_uid)
                    }
                })?;

            info!(
                APP_LOG,
                "updates were processed in {} secs",
                start.elapsed().as_secs()
            );

            dbw.lock()
                .unwrap()
                .set_last_handled_height(last_updated_height)?;

            info!(APP_LOG, "last updated height: {}", last_updated_height);

            if to_height > last_updated_height {
                std::thread::sleep(Duration::from_secs(5));
            }

            Ok(())
        });
    }

    error!(APP_LOG, "{:?}", result);
    result
}

fn extract_string_fragment(fragments: &Vec<&str>, position: usize) -> Option<String> {
    fragments.get(position).map_or(None, |fr| {
        if fr.starts_with(STRING_SEPARATOR) {
            Some(fr.trim_start_matches(STRING_SEPARATOR).to_owned())
        } else {
            None
        }
    })
}

fn extract_integer_fragment(fragments: &Vec<&str>, position: usize) -> Option<i32> {
    fragments.get(position).map_or(None, |fr| {
        if fr.starts_with(INTEGER_SEPARATOR) {
            fr.trim_start_matches(INTEGER_SEPARATOR).parse().ok()
        } else {
            None
        }
    })
}

fn append_blocks_or_microblocks<U: DataEntriesRepo>(
    dbw: Arc<Mutex<U>>,
    appends: &Vec<BlockMicroblockAppend>,
) -> Result<(), Error> {
    let mut dbw = dbw.try_lock().unwrap();

    let block_uids = dbw.insert_blocks_or_microblocks(
        &appends
            .into_iter()
            .map(|append| BlockMicroblock {
                id: append.id.clone(),
                height: append.height as i32,
                time_stamp: append.time_stamp,
            })
            .collect_vec(),
    )?;

    append_data_entries(
        dbw,
        block_uids
            .iter()
            .zip(appends)
            .filter(|(_, append)| append.data_entries.len() > 0)
            .flat_map(|(block_uid, append)| {
                append
                    .data_entries
                    .clone()
                    .into_iter()
                    .map(|de| BlockUidWithDataEntry {
                        block_uid: block_uid.to_owned(),
                        data_entry: de,
                    })
                    .collect_vec()
            })
            .collect_vec(),
    )
}

fn append_data_entries<U: DataEntriesRepo>(
    mut dbw: MutexGuard<U>,
    updates: Vec<BlockUidWithDataEntry>,
) -> Result<(), Error> {
    let last_uid = dbw.get_last_update_uid()?;

    let updates_with_uids_superseded_by = updates
        .into_iter()
        .enumerate()
        .map(
            |(
                idx,
                BlockUidWithDataEntry {
                    block_uid,
                    data_entry,
                },
            )| {
                let key = &data_entry.key;
                let frs: Vec<&str> = key
                    .split(FRAGMENT_SEPARATOR)
                    .into_iter()
                    // unwrap will be guarantly safe cause it was preliminarily filtered by suitability
                    .filter_map(|fr| {
                        RE.captures(fr)
                            .map(|caps| caps.get(1).map(|m| m.as_str()))?
                    })
                    .collect();

                InsertableDataEntry {
                    block_uid: block_uid,
                    transaction_id: data_entry.transaction_id.clone(),
                    uid: idx as i64,
                    superseded_by: -1,
                    address: data_entry.address.clone(),
                    key: data_entry.key.clone(),
                    value_binary: data_entry.value_binary.clone(),
                    value_bool: data_entry.value_bool,
                    value_integer: data_entry.value_integer,
                    value_string: data_entry.value_string.clone(),
                    fragment_0_integer: extract_integer_fragment(&frs, 0),
                    fragment_0_string: extract_string_fragment(&frs, 0),
                    fragment_1_integer: extract_integer_fragment(&frs, 1),
                    fragment_1_string: extract_string_fragment(&frs, 1),
                    fragment_2_integer: extract_integer_fragment(&frs, 2),
                    fragment_2_string: extract_string_fragment(&frs, 2),
                    fragment_3_integer: extract_integer_fragment(&frs, 3),
                    fragment_3_string: extract_string_fragment(&frs, 3),
                    fragment_4_integer: extract_integer_fragment(&frs, 4),
                    fragment_4_string: extract_string_fragment(&frs, 4),
                    fragment_5_integer: extract_integer_fragment(&frs, 5),
                    fragment_5_string: extract_string_fragment(&frs, 5),
                    fragment_6_integer: extract_integer_fragment(&frs, 6),
                    fragment_6_string: extract_string_fragment(&frs, 6),
                    fragment_7_integer: extract_integer_fragment(&frs, 7),
                    fragment_7_string: extract_string_fragment(&frs, 7),
                    fragment_8_integer: extract_integer_fragment(&frs, 8),
                    fragment_8_string: extract_string_fragment(&frs, 8),
                    fragment_9_integer: extract_integer_fragment(&frs, 9),
                    fragment_9_string: extract_string_fragment(&frs, 9),
                    fragment_10_integer: extract_integer_fragment(&frs, 10),
                    fragment_10_string: extract_string_fragment(&frs, 10),
                }
            },
        )
        .group_by(|item| {
            (
                item.address.clone(),
                item.key.clone(),
                item.transaction_id.clone(),
            )
        })
        .into_iter()
        .map(|(_, updates)| {
            let mut updates = updates
                .into_iter()
                .sorted_by_key(|item| item.uid)
                .collect::<Vec<InsertableDataEntry>>();

            let mut last_uid = std::i64::MAX - 1;
            updates
                .as_mut_slice()
                .iter_mut()
                .rev()
                .map(|cur| {
                    cur.superseded_by = last_uid;
                    last_uid = cur.uid;
                    cur.to_owned()
                })
                .sorted_by_key(|item| item.uid)
                .collect()
        })
        .collect::<Vec<Vec<InsertableDataEntry>>>();

    // First uid for each asset in a new batch. This value closes superseded_by of previous updates.
    let first_uids: Vec<DataEntryUpdate> = updates_with_uids_superseded_by
        .iter()
        .map(|it| {
            let first = it.iter().next().clone().unwrap();
            DataEntryUpdate {
                address: first.address.clone(),
                key: first.key.clone(),
                uid: first.uid,
            }
        })
        .collect();

    dbw.close_superseded_by(&first_uids)?;

    dbw.insert_data_entries(
        &updates_with_uids_superseded_by
            .clone()
            .into_iter()
            .flatten()
            .sorted_by_key(|de| de.uid)
            .collect_vec(),
    )?;

    dbw.set_last_update_uid(last_uid + updates_with_uids_superseded_by.len() as i64 + 1)
}

fn squash_microblocks<U: DataEntriesRepo>(dbw: Arc<Mutex<U>>) -> Result<(), Error> {
    let total_block_id = dbw.try_lock().unwrap().get_total_block_id()?;

    match total_block_id {
        Some(total_block_id) => {
            let key_block_uid = dbw.try_lock().unwrap().get_key_block_uid()?;

            match key_block_uid {
                Some(key_block_uid) => {
                    dbw.lock()
                        .unwrap()
                        .update_data_entries_block_references(&key_block_uid)?;

                    dbw.try_lock().unwrap().delete_microblocks()?;

                    dbw.try_lock()
                        .unwrap()
                        .change_block_id(&key_block_uid, &total_block_id)?;
                }
                None => (),
            }
        }
        None => (),
    }

    Ok(())
}

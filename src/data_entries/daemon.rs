use super::{
    repo::DataEntriesRepoImpl, BlockMicroblock, BlockMicroblockAppend, BlockchainUpdate,
    DataEntriesRepo, DataEntriesSource, DataEntry, DataEntryUpdate, InsertableDataEntry,
    FRAGMENT_SEPARATOR, INTEGER_DESCRIPTOR, STRING_DESCRIPTOR,
};
use crate::error::Error;
use crate::log::APP_LOG;
use itertools::Itertools;
use slog::info;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

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
    dbw: Arc<Mutex<U>>,
    min_height: u32,
    updates_per_request: usize,
    max_wait_time_in_secs: u64,
) -> Result<(), Error> {
    loop {
        let last_handled_height = dbw.try_lock().unwrap().get_last_handled_height()? as u32;

        let from_height = if last_handled_height < min_height {
            min_height
        } else {
            last_handled_height
        };

        info!(
            APP_LOG,
            "Fetching data entries updates from height {}", from_height
        );
        let max_duration = Duration::from_secs(max_wait_time_in_secs);
        let mut start = Instant::now();
        let updates_with_height = updates_src
            .fetch_updates(from_height, updates_per_request, max_duration)
            .await?;

        dbw.try_lock().unwrap().transaction(|conn| {
            let dbw = Arc::new(Mutex::new(DataEntriesRepoImpl::new(conn.clone())));
            dbw.try_lock().unwrap().delete_last_block()?;

            start = Instant::now();

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
                .try_fold((), |_, update_item| match update_item {
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
                "Updates were processed in {} secs",
                start.elapsed().as_secs()
            );

            info!(
                APP_LOG,
                "Last updated height {}", updates_with_height.last_height
            );

            std::thread::sleep(Duration::from_secs(1));

            Ok(())
        })?;
    }
}

fn extract_string_fragment(values: &Vec<(String, String)>, position: usize) -> Option<String> {
    values.get(position).map_or(None, |(t, v)| {
        if t == STRING_DESCRIPTOR {
            Some(v.to_owned())
        } else {
            None
        }
    })
}

fn extract_integer_fragment(values: &Vec<(String, String)>, position: usize) -> Option<i32> {
    values.get(position).map_or(None, |(t, v)| {
        if t == INTEGER_DESCRIPTOR {
            v.parse().ok()
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

    let data_entries = block_uids
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
        .collect_vec();

    if data_entries.len() > 0 {
        append_data_entries(dbw, data_entries)
    } else {
        Ok(())
    }
}

fn append_data_entries<U: DataEntriesRepo>(
    mut dbw: MutexGuard<U>,
    updates: Vec<BlockUidWithDataEntry>,
) -> Result<(), Error> {
    let next_uid = dbw.get_next_update_uid()?;
    let updates_count = updates.len() as i64;

    let entries = updates.into_iter().enumerate().map(
        |(
            idx,
            BlockUidWithDataEntry {
                block_uid,
                data_entry,
            },
        )| {
            let key = &data_entry.key;
            let mut frs = key.split(FRAGMENT_SEPARATOR).into_iter();

            let types = frs
                .next()
                .map(|ts| ts.split("").into_iter().collect_vec())
                .unwrap_or(vec![]);
            let values = frs
                .enumerate()
                .filter_map(|(idx, fragment)| {
                    types
                        .clone()
                        .into_iter()
                        .nth(idx)
                        .map(|t| (t.to_owned(), fragment.to_owned()))
                })
                .collect();

            InsertableDataEntry {
                block_uid: block_uid,
                transaction_id: data_entry.transaction_id.clone(),
                uid: next_uid + idx as i64,
                superseded_by: -1,
                address: data_entry.address.clone(),
                key: data_entry.key.clone(),
                value_binary: data_entry.value_binary.clone(),
                value_bool: data_entry.value_bool,
                value_integer: data_entry.value_integer,
                value_string: data_entry.value_string.clone(),
                fragment_0_integer: extract_integer_fragment(&values, 0),
                fragment_0_string: extract_string_fragment(&values, 0),
                fragment_1_integer: extract_integer_fragment(&values, 1),
                fragment_1_string: extract_string_fragment(&values, 1),
                fragment_2_integer: extract_integer_fragment(&values, 2),
                fragment_2_string: extract_string_fragment(&values, 2),
                fragment_3_integer: extract_integer_fragment(&values, 3),
                fragment_3_string: extract_string_fragment(&values, 3),
                fragment_4_integer: extract_integer_fragment(&values, 4),
                fragment_4_string: extract_string_fragment(&values, 4),
                fragment_5_integer: extract_integer_fragment(&values, 5),
                fragment_5_string: extract_string_fragment(&values, 5),
                fragment_6_integer: extract_integer_fragment(&values, 6),
                fragment_6_string: extract_string_fragment(&values, 6),
                fragment_7_integer: extract_integer_fragment(&values, 7),
                fragment_7_string: extract_string_fragment(&values, 7),
                fragment_8_integer: extract_integer_fragment(&values, 8),
                fragment_8_string: extract_string_fragment(&values, 8),
                fragment_9_integer: extract_integer_fragment(&values, 9),
                fragment_9_string: extract_string_fragment(&values, 9),
                fragment_10_integer: extract_integer_fragment(&values, 10),
                fragment_10_string: extract_string_fragment(&values, 10),
            }
        },
    );

    let mut grouped_updates: HashMap<InsertableDataEntry, Vec<InsertableDataEntry>> =
        HashMap::new();

    entries.for_each(|item| {
        let group = grouped_updates.entry(item.clone()).or_insert(vec![]);
        group.push(item);
    });

    let grouped_updates = grouped_updates.into_iter().collect_vec();

    let grouped_updates_with_uids_superseded_by = grouped_updates
        .into_iter()
        .map(|(key, group)| {
            let mut updates = group
                .into_iter()
                .sorted_by_key(|item| item.uid)
                .collect::<Vec<InsertableDataEntry>>();

            let mut last_uid = std::i64::MAX - 1;
            (
                key,
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
                    .collect(),
            )
        })
        .collect::<Vec<(InsertableDataEntry, Vec<InsertableDataEntry>)>>();

    // First uid for each asset in a new batch. This value closes superseded_by of previous updates.
    let first_uids: Vec<DataEntryUpdate> = grouped_updates_with_uids_superseded_by
        .iter()
        .map(|(_, group)| {
            let first = group.iter().next().unwrap().clone();
            DataEntryUpdate {
                address: first.address,
                key: first.key,
                superseded_by: first.uid,
            }
        })
        .collect();

    dbw.close_superseded_by(&first_uids)?;

    let updates_with_uids_superseded_by = &grouped_updates_with_uids_superseded_by
        .clone()
        .into_iter()
        .flat_map(|(_, v)| v)
        .sorted_by_key(|de| de.uid)
        .collect_vec();

    dbw.insert_data_entries(updates_with_uids_superseded_by)?;

    dbw.set_next_update_uid(next_uid + updates_count)
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

use super::{
    BlockMicroblock, BlockMicroblockAppend, BlockchainUpdate, DataEntriesRepo, DataEntriesSource,
    DataEntry, DataEntryUpdate, DeletedDataEntry, InsertableDataEntry, FRAGMENT_SEPARATOR,
    INTEGER_DESCRIPTOR, STRING_DESCRIPTOR,
};
use crate::error::AppError;
use crate::log::APP_LOG;
use anyhow::{Error, Result};
use itertools::Itertools;
use slog::info;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

enum UpdatesItem {
    Blocks(Vec<BlockMicroblockAppend>),
    Microblock(BlockMicroblockAppend),
    Rollback(String),
}

#[derive(Debug)]
struct BlockUidWithDataEntry {
    block_uid: i64,
    data_entry: DataEntry,
}

pub async fn start<T: DataEntriesSource + Send + Sync + 'static, U: DataEntriesRepo>(
    updates_src: T,
    dbw: Arc<U>,
    updates_per_request: usize,
    max_wait_time_in_secs: u64,
    addresses_white_list: Vec<String>,
) -> Result<()> {
    let starting_from_height = match dbw.get_prev_handled_height()? {
        Some(prev_handled_height) => {
            rollback(dbw.clone(), prev_handled_height.uid)?;
            prev_handled_height.height as u32 + 1
        }
        None => 1u32,
    };

    info!(
        APP_LOG,
        "Fetching data entries updates from height {}.", starting_from_height
    );
    let max_duration = Duration::from_secs(max_wait_time_in_secs);

    let mut rx = updates_src
        .stream(starting_from_height, updates_per_request, max_duration)
        .await?;

    loop {
        let mut start = Instant::now();

        let updates_with_height =
            rx.recv()
                .await
                .ok_or(Error::new(AppError::StreamReceiveEmpty(
                    "There aren't any blockchain updates.".to_string(),
                )))?;

        info!(
            APP_LOG,
            "{} updates were received in {:?}",
            updates_with_height.updates.len(),
            start.elapsed()
        );

        start = Instant::now();

        dbw.transaction(|| {
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
                        append_blocks_or_microblocks(
                            dbw.clone(),
                            bs.as_ref(),
                            &addresses_white_list,
                        )
                    }
                    UpdatesItem::Microblock(mba) => append_blocks_or_microblocks(
                        dbw.clone(),
                        &vec![mba.to_owned()],
                        &addresses_white_list,
                    ),
                    UpdatesItem::Rollback(sig) => {
                        let block_uid = dbw.clone().get_block_uid(&sig)?;
                        rollback(dbw.clone(), block_uid)
                    }
                })?;

            info!(
                APP_LOG,
                "Updates were processed in {:?}. Last updated height is {}.",
                start.elapsed(),
                updates_with_height.last_height
            );

            Ok(())
        })?;
    }
}

fn extract_string_fragment(values: &Vec<(&str, &str)>, position: usize) -> Option<String> {
    values.get(position).map_or(None, |(t, v)| {
        if *t == STRING_DESCRIPTOR {
            Some(v.to_string())
        } else {
            None
        }
    })
}

fn extract_integer_fragment(values: &Vec<(&str, &str)>, position: usize) -> Option<i32> {
    values.get(position).map_or(None, |(t, v)| {
        if *t == INTEGER_DESCRIPTOR {
            v.parse().ok()
        } else {
            None
        }
    })
}

fn rollback<U: DataEntriesRepo>(dbw: Arc<U>, block_uid: i64) -> Result<()> {
    let deletes = dbw.rollback_data_entries(&block_uid)?;

    let mut grouped_deletes: HashMap<DeletedDataEntry, Vec<DeletedDataEntry>> = HashMap::new();

    deletes.into_iter().for_each(|item| {
        let group = grouped_deletes.entry(item.clone()).or_insert(vec![]);
        group.push(item);
    });

    let lowest_deleted_uids: Vec<i64> = grouped_deletes
        .into_iter()
        .filter_map(|(_, group)| group.into_iter().min_by_key(|i| i.uid).map(|i| i.uid))
        .collect();

    dbw.reopen_superseded_by(&lowest_deleted_uids)?;

    dbw.rollback_blocks_microblocks(&block_uid)
}

fn append_blocks_or_microblocks<U: DataEntriesRepo>(
    dbw: Arc<U>,
    appends: &Vec<BlockMicroblockAppend>,
    addresses_white_list: &Vec<String>,
) -> Result<()> {
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
                .filter(|de| addresses_white_list.contains(&de.address))
                .map(|de| BlockUidWithDataEntry {
                    block_uid: block_uid.to_owned(),
                    data_entry: de,
                })
                .collect_vec()
        })
        .collect_vec();

    if data_entries.len() > 0 {
        append_data_entries(dbw.clone(), data_entries)
    } else {
        Ok(())
    }
}

fn append_data_entries<U: DataEntriesRepo>(
    dbw: Arc<U>,
    updates: Vec<BlockUidWithDataEntry>,
) -> Result<()> {
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
                .map(|fragment| {
                    fragment
                        .split("%")
                        .into_iter()
                        .skip(1) // first item is empty
                        .collect()
                })
                .unwrap_or(vec![]);

            let values = types.into_iter().zip(frs).collect();

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

fn squash_microblocks<U: DataEntriesRepo>(dbw: Arc<U>) -> Result<()> {
    let total_block_id = dbw.get_total_block_id()?;

    match total_block_id {
        Some(total_block_id) => {
            let key_block_uid = dbw.get_key_block_uid()?;

            dbw.update_data_entries_block_references(&key_block_uid)?;

            dbw.delete_microblocks()?;

            dbw.change_block_id(&key_block_uid, &total_block_id)?;
        }
        None => (),
    }

    Ok(())
}

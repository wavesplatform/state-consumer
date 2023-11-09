pub use super::{DataEntriesRepo, DataEntriesRepoOperations};
use super::{
    BlockMicroblock, DataEntryUpdate, DeletedDataEntry, InsertableDataEntry,
    InsertedDataEntry, LastBlockTimestamp, PrevHandledHeight,
};
use crate::error::AppError;
use crate::schema::blocks_microblocks;
use crate::schema::blocks_microblocks::dsl::*;
use crate::schema::data_entries;
use crate::schema::data_entries_history_keys;
use crate::schema::data_entries_uid_seq;
use crate::schema::data_entries_uid_seq::dsl::*;
use crate::db::{PgPool, PooledPgConnection};
use anyhow::{Error, Result};
use diesel::prelude::*;
use diesel::sql_types::{Array, BigInt, VarChar};

const MAX_UID: i64 = std::i64::MAX - 1;



pub struct PgDataEntriesRepo {
    pool: PgPool,
}

impl PgDataEntriesRepo {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn get_conn(&self) -> Result<PooledPgConnection> {
        Ok(self.pool.get()?)
    }
}

impl DataEntriesRepo for PgDataEntriesRepo {
    type Operations = PooledPgConnection;

    fn execute<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(PooledPgConnection) -> Result<R>,
    {
        tokio::task::block_in_place(move || {
            let conn = self.get_conn()?;
            f(conn)
        })
    }

    fn transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&PooledPgConnection) -> Result<R>,
    {
        tokio::task::block_in_place(move || {
            let conn = self.get_conn()?;
            conn.transaction(|| f(&conn))
        })
    }
}

impl DataEntriesRepoOperations for PooledPgConnection {
    fn get_handled_height(&self, depth: u32) -> Result<Option<PrevHandledHeight>> {
        let sql_height = format!("(select max(height) - {} from blocks_microblocks)", depth);

        blocks_microblocks
            .select((blocks_microblocks::uid, blocks_microblocks::height))
            .filter(
                blocks_microblocks::height
                    .eq(diesel::expression::sql_literal::sql(sql_height.as_str())),
            )
            .order(blocks_microblocks::uid.asc())
            .first(self)
            .optional()
            .map_err(|err| Error::new(AppError::DbError(err)))
    }

    fn get_last_block_timestamp(&self) -> Result<LastBlockTimestamp> {
        blocks_microblocks
            .select(blocks_microblocks::time_stamp)
            .order(blocks_microblocks::uid.desc())
            .filter(blocks_microblocks::time_stamp.is_not_null())
            .first::<Option<i64>>(self)
            .map(|opt_ts| LastBlockTimestamp { time_stamp: opt_ts })
            .map_err(|err| Error::new(AppError::DbError(err)))
    }

    fn get_block_uid(&self, block_id: &str) -> Result<i64> {
        blocks_microblocks
            .select(blocks_microblocks::uid)
            .filter(blocks_microblocks::id.eq(block_id))
            .get_result(self)
            .map_err(|err| {
                Error::new(AppError::DbError(err))
                    .context(format!("Cannot get block_uid by block id {}.", block_id))
            })
    }

    fn get_key_block_uid(&self) -> Result<i64> {
        blocks_microblocks
            .select(diesel::expression::sql_literal::sql("max(uid)"))
            .filter(blocks_microblocks::time_stamp.is_not_null())
            .get_result(self)
            .map_err(|err| Error::new(AppError::DbError(err)).context("Cannot get key block uid."))
    }

    fn get_total_block_id(&self) -> Result<Option<String>> {
        blocks_microblocks
            .select(blocks_microblocks::id)
            .filter(blocks_microblocks::time_stamp.is_null())
            .order(blocks_microblocks::uid.desc())
            .first(self)
            .optional()
            .map_err(|err| Error::new(AppError::DbError(err)).context("Cannot get total block id."))
    }

    fn get_next_update_uid(&self) -> Result<i64> {
        data_entries_uid_seq
            .select(data_entries_uid_seq::last_value)
            .first(self)
            .map_err(|err| {
                Error::new(AppError::DbError(err)).context("Cannot get next update uid.")
            })
    }

    fn insert_blocks_or_microblocks(&self, blocks: &Vec<BlockMicroblock>) -> Result<Vec<i64>> {
        diesel::insert_into(blocks_microblocks::table)
            .values(blocks)
            .returning(blocks_microblocks::uid)
            .get_results(self)
            .map_err(|err| Error::new(AppError::DbError(err)))
    }

    fn insert_data_entries(&self, entries: &Vec<InsertableDataEntry>) -> Result<()> {
        // one data entry has 29 columns
        // pg cannot insert more then 65535
        // so the biggest chunk should be less then 2259
        let chunk_size = 2000;
        entries
            .to_owned()
            .chunks(chunk_size)
            .into_iter()
            .try_fold((), |_, chunk| {
                let mut  recs : Vec<_> = vec![];
                let mut  hist_uids : Vec<_> = vec![];

                diesel::insert_into(data_entries::table)
                    .values(chunk)
                    .returning((data_entries::address, data_entries::key, data_entries::uid, data_entries::block_uid))
                    .get_results(self)
                    .map(|rows: Vec<(String, String, i64, i64)>| {
                        recs = rows.into_iter()
                                .map(|(address, key, data_entry_uid, block_uid)| InsertedDataEntry {
                                    address: address,
                                    key: key,
                                    data_entry_uid: data_entry_uid,
                                    block_uid: block_uid,
                                    height: None,
                                    block_timestamp: None
                                }).collect();

                    })
                    .map_err(|err| Error::new(AppError::DbError(err)))?;

                diesel::insert_into(data_entries_history_keys::table)
                    .values(recs)
                    .returning(data_entries_history_keys::uid)
                    .get_results(self)
                    .map(|r: Vec<i64>| {
                        hist_uids = r;
                    })
                    .map_err(|err| Error::new(AppError::DbError(err)))?;

                diesel::sql_query(r#"
                        update data_entries_history_keys hk set
                            height = (select height from blocks_microblocks where uid = hk.block_uid),
                            block_timestamp = (select to_timestamp(time_stamp / 1000) from blocks_microblocks where uid = hk.block_uid)
                        where hk.uid  = ANY($1)
                    "#)
                    .bind::<Array<BigInt>, _>(hist_uids)
                    .execute(self)
                    .map(|_| ())
                    .map_err(|err| Error::new(AppError::DbError(err)))
            })
    }

    fn close_superseded_by(&self, updates: &Vec<DataEntryUpdate>) -> Result<()> {
        let mut addresses = vec![];
        let mut keys = vec![];
        let mut superseded_bys = vec![];
        updates.iter().for_each(|u| {
            addresses.push(&u.address);
            keys.push(&u.key);
            superseded_bys.push(&u.superseded_by);
        });

        diesel::sql_query("UPDATE data_entries SET superseded_by = updates.superseded_by FROM (SELECT UNNEST($1) as address, UNNEST($2) as key, UNNEST($3) as superseded_by) as updates where data_entries.address = updates.address and data_entries.key = updates.key and data_entries.superseded_by = $4")
                .bind::<Array<VarChar>, _>(addresses)
                .bind::<Array<VarChar>, _>(keys)
                .bind::<Array<BigInt>, _>(superseded_bys)
                .bind::<BigInt, _>(MAX_UID)
            .execute(self)
            .map(|_| ())
            .map_err(|err| Error::new(AppError::DbError(err)))
    }

    fn reopen_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()> {
        diesel::sql_query("UPDATE data_entries SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE data_entries.superseded_by = current.superseded_by;")
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<BigInt>, _>(current_superseded_by)
            .execute(self)
            .map(|_| ())
            .map_err(|err| Error::new(AppError::DbError(err)))
    }

    fn set_next_update_uid(&self, new_uid: i64) -> Result<()> {
        diesel::sql_query(format!(
            "select setval('data_entries_uid_seq', {}, false);", // 3rd param - is called; in case of true, value'll be incremented before returning
            new_uid
        ))
        .execute(self)
        .map(|_| ())
        .map_err(|err| Error::new(AppError::DbError(err)))
    }

    fn change_block_id(&self, block_uid: &i64, new_block_id: &str) -> Result<()> {
        diesel::update(blocks_microblocks::table)
            .set(blocks_microblocks::id.eq(new_block_id))
            .filter(blocks_microblocks::uid.eq(block_uid))
            .execute(self)
            .map(|_| ())
            .map_err(|err| Error::new(AppError::DbError(err)))
    }

    fn update_data_entries_block_references(&self, block_uid: &i64) -> Result<()> {
        diesel::update(data_entries::table)
            .set(data_entries::block_uid.eq(block_uid))
            .filter(data_entries::block_uid.gt(block_uid))
            .execute(self)
            .map(|_| ())
            .map_err(|err| Error::new(AppError::DbError(err)))?;

        diesel::update(data_entries_history_keys::table)
            .set(data_entries_history_keys::block_uid.eq(block_uid))
            .filter(data_entries_history_keys::block_uid.gt(block_uid))
            .execute(self)
            .map(|_| ())
            .map_err(|err| Error::new(AppError::DbError(err)))?;

        Ok(())
    }

    fn delete_microblocks(&self) -> Result<()> {
        diesel::delete(blocks_microblocks::table)
            .filter(blocks_microblocks::time_stamp.is_null())
            .execute(self)
            .map(|_| ())
            .map_err(|err| Error::new(AppError::DbError(err)))
    }

    fn rollback_blocks_microblocks(&self, block_uid: &i64) -> Result<()> {
        diesel::delete(blocks_microblocks::table)
            .filter(blocks_microblocks::uid.gt(block_uid))
            .execute(self)
            .map(|_| ())
            .map_err(|err| Error::new(AppError::DbError(err)))
    }

    fn rollback_data_entries(&self, block_uid: &i64) -> Result<Vec<DeletedDataEntry>> {
        diesel::delete(data_entries::table)
            .filter(data_entries::block_uid.gt(block_uid))
            .returning((data_entries::address, data_entries::key, data_entries::uid))
            .get_results(self)
            .map(|des| {
                des.into_iter()
                    .map(|(de_address, de_key, de_uid)| DeletedDataEntry {
                        address: de_address,
                        key: de_key,
                        uid: de_uid,
                    })
                    .collect()
            })
            .map_err(|err| Error::new(AppError::DbError(err)))
    }
}

use super::{BlockMicroblock, DataEntriesRepo, DataEntryUpdate, InsertableDataEntry};
use crate::error::Error;
use crate::schema::blocks_microblocks;
use crate::schema::blocks_microblocks::dsl::*;
use crate::schema::data_entries;
use crate::schema::data_entries_uid_seq;
use crate::schema::data_entries_uid_seq::dsl::*;
use diesel::prelude::*;
use diesel::sql_types::{Array, BigInt, VarChar};
use diesel::PgConnection;
const MAX_UID: i64 = std::i64::MAX - 1;

pub struct DataEntriesRepoImpl {
    conn: PgConnection,
}

impl DataEntriesRepoImpl {
    pub fn new(conn: PgConnection) -> DataEntriesRepoImpl {
        DataEntriesRepoImpl { conn: conn }
    }
}

#[derive(Debug, QueryableByName)]
#[table_name = "data_entries_uid_seq"]
struct DataEntriesUidSeq {
    last_value: u64,
}

impl DataEntriesRepo for DataEntriesRepoImpl {
    fn transaction(&self, f: impl FnOnce() -> Result<(), Error>) -> Result<(), Error> {
        self.conn.transaction(|| f())
    }

    fn get_last_height(&self) -> Result<i32, Error> {
        blocks_microblocks
            .select(diesel::expression::sql_literal::sql("max(height)"))
            .get_result(&self.conn)
            .map_err(|err| Error::DbError(err))
    }

    fn get_block_uid(&self, block_id: &str) -> Result<i64, Error> {
        blocks_microblocks
            .select(blocks_microblocks::uid)
            .filter(blocks_microblocks::id.eq(block_id))
            .get_result(&self.conn)
            .map_err(|err| Error::DbError(err))
    }

    fn get_key_block_uid(&self) -> Result<i64, Error> {
        blocks_microblocks
            .select(diesel::expression::sql_literal::sql("max(uid)"))
            .filter(blocks_microblocks::time_stamp.is_null())
            .get_result(&self.conn)
            .map_err(|err| Error::DbError(err))
    }

    fn get_total_block_id(&self) -> Result<Option<String>, Error> {
        blocks_microblocks
            .select(blocks_microblocks::id)
            .filter(blocks_microblocks::time_stamp.is_null())
            .order(blocks_microblocks::uid.desc())
            .first(&self.conn)
            .optional()
            .map_err(|err| Error::DbError(err))
    }

    fn get_next_update_uid(&self) -> Result<i64, Error> {
        data_entries_uid_seq
            .select(data_entries_uid_seq::last_value)
            .first(&self.conn)
            .map_err(|err| Error::DbError(err))
    }

    fn insert_blocks_or_microblocks(
        &self,
        blocks: &Vec<BlockMicroblock>,
    ) -> Result<Vec<i64>, Error> {
        diesel::insert_into(blocks_microblocks::table)
            .values(blocks)
            .returning(blocks_microblocks::uid)
            .get_results(&self.conn)
            .map_err(|err| Error::DbError(err))
    }

    fn insert_data_entries(&self, entries: &Vec<InsertableDataEntry>) -> Result<(), Error> {
        // one data entry has 29 columns
        // pg cannot insert more then 65535
        // so the biggest chunk should be less then 2259
        let chunk_size = 2000;
        entries
            .to_owned()
            .chunks(chunk_size)
            .into_iter()
            .try_fold((), |_, chunk| {
                diesel::insert_into(data_entries::table)
                    .values(chunk)
                    .execute(&self.conn)
                    .map(|_| ())
                    .map_err(|err| Error::DbError(err))
            })
    }

    fn close_superseded_by(&self, updates: &Vec<DataEntryUpdate>) -> Result<(), Error> {
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
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| Error::DbError(err))
    }

    fn reopen_superseded_by(&self, current_superseded_by: &i64) -> Result<(), Error> {
        diesel::update(data_entries::table)
            .set(data_entries::superseded_by.eq(MAX_UID))
            .filter(data_entries::superseded_by.eq(current_superseded_by))
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| Error::DbError(err))
    }

    fn set_next_update_uid(&self, new_uid: i64) -> Result<(), Error> {
        diesel::sql_query(format!(
            "select setval('data_entries_uid_seq', {}, false);", // 3rd param - is called; in case of true, value'll be incremented before returning
            new_uid
        ))
        .execute(&self.conn)
        .map(|_| ())
        .map_err(|err| Error::DbError(err))
    }

    fn change_block_id(&self, block_uid: &i64, new_block_id: &str) -> Result<(), Error> {
        diesel::update(blocks_microblocks::table)
            .set(blocks_microblocks::id.eq(new_block_id))
            .filter(blocks_microblocks::uid.gt(block_uid))
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| Error::DbError(err))
    }

    fn update_data_entries_block_references(&self, block_uid: &i64) -> Result<(), Error> {
        diesel::update(data_entries::table)
            .set(data_entries::block_uid.eq(block_uid))
            .filter(data_entries::block_uid.gt(block_uid))
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| Error::DbError(err))
    }

    fn delete_microblocks(&self) -> Result<(), Error> {
        diesel::delete(blocks_microblocks::table)
            .filter(blocks_microblocks::time_stamp.is_null())
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| Error::DbError(err))
    }

    fn delete_last_block(&self) -> Result<Option<i32>, Error> {
        diesel::delete(blocks_microblocks.filter(blocks_microblocks::height.eq(
            diesel::expression::sql_literal::sql("(select max(height) from blocks_microblocks)"),
        )))
        .returning(blocks_microblocks::height)
        .get_result(&self.conn)
        .optional()
        .map_err(|err| Error::DbError(err))
    }

    fn rollback_blocks_microblocks(&self, block_uid: &i64) -> Result<(), Error> {
        diesel::delete(blocks_microblocks::table)
            .filter(blocks_microblocks::uid.gt(block_uid))
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| Error::DbError(err))
    }

    fn rollback_data_entries(&mut self, block_uid: &i64) -> Result<(), Error> {
        diesel::delete(data_entries::table)
            .filter(data_entries::block_uid.gt(block_uid))
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|err| Error::DbError(err))
    }
}

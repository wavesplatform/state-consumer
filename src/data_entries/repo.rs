use super::{BlockMicroblock, DataEntriesRepo, DataEntryUpdate, InsertableDataEntry};
use crate::error::Error;
use crate::schema::blocks_microblocks;
use crate::schema::blocks_microblocks::dsl::*;
use crate::schema::data_entries;
use crate::schema::data_entries_uid_seq;
use crate::schema::data_entries_uid_seq::dsl::*;
use crate::schema::last_handled_height;
use crate::schema::last_handled_height::dsl::*;
use diesel::prelude::*;
use diesel::PgConnection;
use std::sync::Arc;

const MAX_UID: i64 = std::i64::MAX - 1;

pub struct DataEntriesRepoImpl {
    conn: Arc<PgConnection>,
}

impl DataEntriesRepoImpl {
    pub fn new(conn: Arc<PgConnection>) -> DataEntriesRepoImpl {
        DataEntriesRepoImpl { conn: conn }
    }
}

#[derive(Debug, QueryableByName)]
#[table_name = "data_entries_uid_seq"]
struct DataEntriesUidSeq {
    last_value: u64,
}

impl DataEntriesRepo for DataEntriesRepoImpl {
    fn transaction(
        &self,
        f: impl FnOnce(Arc<PgConnection>) -> Result<(), Error>,
    ) -> Result<(), Error> {
        self.conn.transaction(|| f(self.conn.clone()))
    }

    fn get_last_handled_height(&self) -> Result<u32, Error> {
        last_handled_height
            .select(last_handled_height::height)
            .first::<i32>(&self.conn as &PgConnection as &PgConnection)
            .map(|h| h as u32)
            .map_err(|err| Error::DbError(err))
    }

    fn set_last_handled_height(&mut self, h: u32) -> Result<(), Error> {
        diesel::update(last_handled_height::table)
            .set(last_handled_height::height.eq(h as i32))
            .execute(&self.conn as &PgConnection)
            .map(|_| ())
            .map_err(|err| Error::DbError(err))
    }

    fn get_block_uid(&mut self, block_id: &str) -> Result<i64, Error> {
        blocks_microblocks
            .select(blocks_microblocks::uid)
            .filter(blocks_microblocks::id.eq(block_id))
            .get_result(&self.conn as &PgConnection)
            .map_err(|err| Error::DbError(err))
    }

    fn get_key_block_uid(&mut self) -> Result<Option<i64>, Error> {
        blocks_microblocks
            .select(diesel::dsl::max(blocks_microblocks::uid))
            .filter(blocks_microblocks::time_stamp.is_null())
            .order(blocks_microblocks::uid.desc())
            .first(&self.conn as &PgConnection)
            .map_err(|err| Error::DbError(err))
    }

    fn get_total_block_id(&mut self) -> Result<Option<String>, Error> {
        blocks_microblocks
            .select(blocks_microblocks::id)
            .filter(blocks_microblocks::time_stamp.is_null())
            .order(blocks_microblocks::uid.desc())
            .first(&self.conn as &PgConnection)
            .optional()
            .map_err(|err| Error::DbError(err))
    }

    fn get_next_update_uid(&mut self) -> Result<i64, Error> {
        data_entries_uid_seq
            .select(data_entries_uid_seq::last_value)
            .first(&self.conn as &PgConnection)
            .map_err(|err| Error::DbError(err))
    }

    fn insert_blocks_or_microblocks(
        &mut self,
        blocks: &Vec<BlockMicroblock>,
    ) -> Result<Vec<i64>, Error> {
        diesel::insert_into(blocks_microblocks::table)
            .values(blocks)
            .returning(blocks_microblocks::uid)
            .get_results(&self.conn as &PgConnection)
            .map_err(|err| Error::DbError(err))
    }

    fn insert_data_entries(&mut self, entries: &Vec<InsertableDataEntry>) -> Result<(), Error> {
        // one data entry has 29 columns
        // pg cannot insert more then 65535
        // so the biggest chunk should be less then 2259
        let chunk_size = 2000;
        entries
            .to_owned()
            .chunks(chunk_size)
            .into_iter()
            .fold(Ok(()), |_, chunk| {
                diesel::insert_into(data_entries::table)
                    .values(chunk)
                    .execute(&self.conn as &PgConnection)
                    .map(|_| ())
                    .map_err(|err| Error::DbError(err))
            })
    }

    fn close_superseded_by(&mut self, updates: &Vec<DataEntryUpdate>) -> Result<(), Error> {
        updates
            .into_iter()
            .try_for_each::<_, Result<(), Error>>(|upd| {
                diesel::update(data_entries::table)
                    .set(data_entries::superseded_by.eq(upd.superseded_by))
                    .filter(data_entries::address.eq(&upd.address))
                    .filter(data_entries::key.eq(&upd.key))
                    .filter(data_entries::superseded_by.eq(MAX_UID))
                    .execute(&self.conn as &PgConnection)
                    .map(|_| ())
                    .map_err(|err| Error::DbError(err))
            })
    }

    fn reopen_superseded_by(&mut self, current_superseded_by: &i64) -> Result<(), Error> {
        diesel::update(data_entries::table)
            .set(data_entries::superseded_by.eq(MAX_UID))
            .filter(data_entries::superseded_by.eq(current_superseded_by))
            .execute(&self.conn as &PgConnection)
            .map(|_| ())
            .map_err(|err| Error::DbError(err))
    }

    fn set_next_update_uid(&mut self, new_uid: i64) -> Result<(), Error> {
        diesel::sql_query(format!(
            "alter sequence data_entries_uid_seq restart with {};",
            new_uid
        ))
        .execute(&self.conn as &PgConnection)
        .map(|_| ())
        .map_err(|err| Error::DbError(err))
    }

    fn change_block_id(&mut self, block_uid: &i64, new_block_id: &str) -> Result<(), Error> {
        diesel::update(blocks_microblocks::table)
            .set(blocks_microblocks::id.eq(new_block_id))
            .filter(blocks_microblocks::uid.gt(block_uid))
            .execute(&self.conn as &PgConnection)
            .map(|_| ())
            .map_err(|err| Error::DbError(err))
    }

    fn update_data_entries_block_references(&mut self, block_uid: &i64) -> Result<(), Error> {
        diesel::update(data_entries::table)
            .set(data_entries::block_uid.eq(block_uid))
            .filter(data_entries::block_uid.gt(block_uid))
            .execute(&self.conn as &PgConnection)
            .map(|_| ())
            .map_err(|err| Error::DbError(err))
    }

    fn delete_microblocks(&mut self) -> Result<(), Error> {
        diesel::delete(blocks_microblocks::table)
            .filter(blocks_microblocks::time_stamp.is_null())
            .execute(&self.conn as &PgConnection)
            .map(|_| ())
            .map_err(|err| Error::DbError(err))
    }

    fn rollback_blocks_microblocks(&mut self, block_uid: &i64) -> Result<(), Error> {
        diesel::delete(blocks_microblocks::table)
            .filter(blocks_microblocks::uid.gt(block_uid))
            .execute(&self.conn as &PgConnection)
            .map(|_| ())
            .map_err(|err| Error::DbError(err))
    }

    fn rollback_data_entries(&mut self, block_uid: &i64) -> Result<(), Error> {
        diesel::delete(data_entries::table)
            .filter(data_entries::block_uid.gt(block_uid))
            .execute(&self.conn as &PgConnection)
            .map(|_| ())
            .map_err(|err| Error::DbError(err))
    }
}

// #[cfg(test)]
// pub(crate) mod tests {
//     use super::*;
//     use crate::{
//         data_entries::{BalancesRepo, UsdnBalanceUpdate},
//         db::tests::PG_POOL_LOCAL,
//     };
//     use chrono::{Duration, NaiveDateTime};
//     use once_cell::sync::Lazy;

//     pub static REPO: Lazy<BalancesRepoImpl> =
//         Lazy::new(|| BalancesRepoImpl::new(PG_POOL_LOCAL.clone()));

//     #[test]
//     fn last_handled_height_on_empty_db() {
//         reset_pg();
//         let last_handled_height_on_empty = REPO.get_last_handled_height().unwrap();
//         assert_eq!(last_handled_height_on_empty, 0);
//         reset_pg();
//     }

//     #[test]
//     fn set_and_get_last_handled_height() {
//         reset_pg();
//         REPO.clone().set_last_handled_height(100).unwrap();
//         let h = REPO.get_last_handled_height().unwrap();
//         assert_eq!(h, 100);
//         reset_pg();
//     }

//     #[test]
//     fn insert_updates() {
//         reset_pg();

//         // 1 Jan 2020
//         let time = NaiveDateTime::from_timestamp(1577836800, 0);

//         let updates = vec![
//             UsdnBalanceUpdate {
//                 address: "address1".to_owned(),
//                 timestamp: time,
//                 balance: 995.5,
//                 origin_transaction_id: "tx1".to_owned(),
//                 height: 1,
//             },
//             UsdnBalanceUpdate {
//                 address: "address2".to_owned(),
//                 timestamp: time + Duration::seconds(5),
//                 balance: 201.4,
//                 origin_transaction_id: "tx2".to_owned(),
//                 height: 2,
//             },
//         ];

//         REPO.clone().insert_updates(&updates).unwrap();

//         let h = REPO.get_last_handled_height().unwrap();
//         assert_eq!(h, 2);

//         reset_pg()
//     }

//     #[test]
//     fn joins_addresses_correctly() {
//         let addresses = vec![
//             String::from("qwe"),
//             String::from("asd"),
//             String::from("zxc"),
//         ];
//         let joined = join_addresses(&addresses);
//         assert_eq!(joined, "'qwe','asd','zxc'".to_owned());
//     }

//     fn reset_pg() {
//         diesel::sql_query("truncate usdn_balance_updates restart identity;")
//             .execute(&PG_POOL_LOCAL.get().unwrap())
//             .unwrap();
//     }
// }

pub mod daemon;
pub mod repo;
pub mod updates;

use crate::error::Error;
use crate::schema::blocks_microblocks;
use crate::schema::data_entries;
use async_trait::async_trait;
use diesel::sql_types::{BigInt, Nullable, Text};
use diesel::PgConnection;
use diesel::{Insertable, QueryableByName};
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub const FRAGMENT_SEPARATOR: &str = "__";
pub const STRING_SEPARATOR: &str = "$";
pub const INTEGER_SEPARATOR: &str = "#";

pub static RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(&format!(
        r"^(\{0}\w*|{1}-?[0-9]+)$",
        STRING_SEPARATOR, INTEGER_SEPARATOR
    ))
    .unwrap()
});

#[derive(Debug, Clone)]
pub struct Config {
    pub blockchain_updates_url: String,
    pub blocks_per_request: usize,
    pub starting_height: u32,
}

#[derive(Clone, Debug)]
pub struct DataEntry {
    pub address: String,
    pub key: String,
    pub transaction_id: String,
    pub value_binary: Option<Vec<u8>>,
    pub value_bool: Option<bool>,
    pub value_integer: Option<i64>,
    pub value_string: Option<String>,
}

impl PartialEq for DataEntry {
    fn eq(&self, other: &DataEntry) -> bool {
        (&self.address, &self.key, &self.transaction_id)
            == (&other.address, &other.key, &other.transaction_id)
    }
}

impl Eq for DataEntry {}

impl Hash for DataEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.hash(state);
        self.key.hash(state);
        self.transaction_id.hash(state);
    }
}

#[derive(Clone, Debug, Insertable, QueryableByName)]
#[table_name = "data_entries"]
pub struct InsertableDataEntry {
    pub block_uid: i64,
    pub transaction_id: String,
    pub uid: i64,
    pub superseded_by: i64,
    pub address: String,
    pub key: String,
    #[sql_type = "Nullable<Text>"]
    pub value_binary: Option<Vec<u8>>,
    pub value_bool: Option<bool>,
    #[sql_type = "Nullable<BigInt>"]
    pub value_integer: Option<i64>,
    pub value_string: Option<String>,
    pub fragment_0_integer: Option<i32>,
    pub fragment_0_string: Option<String>,
    pub fragment_1_integer: Option<i32>,
    pub fragment_1_string: Option<String>,
    pub fragment_2_integer: Option<i32>,
    pub fragment_2_string: Option<String>,
    pub fragment_3_integer: Option<i32>,
    pub fragment_3_string: Option<String>,
    pub fragment_4_integer: Option<i32>,
    pub fragment_4_string: Option<String>,
    pub fragment_5_integer: Option<i32>,
    pub fragment_5_string: Option<String>,
    pub fragment_6_integer: Option<i32>,
    pub fragment_6_string: Option<String>,
    pub fragment_7_integer: Option<i32>,
    pub fragment_7_string: Option<String>,
    pub fragment_8_integer: Option<i32>,
    pub fragment_8_string: Option<String>,
    pub fragment_9_integer: Option<i32>,
    pub fragment_9_string: Option<String>,
    pub fragment_10_integer: Option<i32>,
    pub fragment_10_string: Option<String>,
}

#[derive(Clone, Debug, Insertable)]
#[table_name = "data_entries"]
pub struct DataEntryUpdate {
    pub uid: i64,
    pub address: String,
    pub key: String,
}

#[async_trait]
pub trait DataEntriesSource {
    async fn fetch_updates(
        &self,
        from_height: u32,
        to_height: u32,
    ) -> Result<BlockchainUpdateWithHeight, Error>;
}

#[derive(Clone, Debug, Insertable)]
#[table_name = "blocks_microblocks"]
pub struct BlockMicroblock {
    id: String,
    time_stamp: Option<i64>,
    height: i32,
}

#[derive(Clone, Debug)]
pub struct BlockMicroblockAppend {
    id: String,
    time_stamp: Option<i64>,
    height: u32,
    data_entries: HashSet<DataEntry>,
}

#[derive(Clone, Debug)]
pub enum BlockchainUpdate {
    Block(BlockMicroblockAppend),
    Microblock(BlockMicroblockAppend),
    Rollback(String),
}

#[derive(Debug)]
pub struct BlockchainUpdateWithHeight {
    pub height: u32,
    pub updates: Vec<BlockchainUpdate>,
}

pub trait DataEntriesRepo {
    fn transaction(
        &self,
        f: impl FnOnce(Arc<PgConnection>) -> Result<(), Error>,
    ) -> Result<(), Error>;

    fn get_last_handled_height(&self) -> Result<u32, Error>;

    fn set_last_handled_height(&mut self, new_height: u32) -> Result<(), Error>;

    fn get_block_uid(&mut self, block_id: &str) -> Result<i64, Error>;

    fn get_key_block_uid(&mut self) -> Result<Option<i64>, Error>;

    fn get_total_block_id(&mut self) -> Result<Option<String>, Error>;

    fn get_last_update_uid(&mut self) -> Result<i64, Error>;

    fn insert_blocks_or_microblocks(
        &mut self,
        blocks: &Vec<BlockMicroblock>,
    ) -> Result<Vec<i64>, Error>;

    fn insert_data_entries(&mut self, entries: &Vec<InsertableDataEntry>) -> Result<(), Error>;

    fn close_superseded_by(&mut self, updates: &Vec<DataEntryUpdate>) -> Result<(), Error>;

    fn reopen_superseded_by(&mut self, current_superseded_by: &i64) -> Result<(), Error>;

    fn set_last_update_uid(&mut self, uid: i64) -> Result<(), Error>;

    fn change_block_id(&mut self, block_uid: &i64, new_block_id: &str) -> Result<(), Error>;

    fn update_data_entries_block_references(&mut self, block_uid: &i64) -> Result<(), Error>;

    fn delete_microblocks(&mut self) -> Result<(), Error>;

    fn rollback_blocks_microblocks(&mut self, block_uid: &i64) -> Result<(), Error>;

    fn rollback_data_entries(&mut self, block_uid: &i64) -> Result<(), Error>;
}

pub mod daemon;
pub mod repo;
pub mod updates;

use crate::error::Error;
use crate::schema::blocks_microblocks;
use crate::schema::data_entries;
use async_trait::async_trait;
use diesel::sql_types::{BigInt, Nullable, Text};
use diesel::Insertable;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

pub const FRAGMENT_SEPARATOR: &str = "__";
pub const STRING_DESCRIPTOR: &str = "$";
pub const INTEGER_DESCRIPTOR: &str = "#";

#[derive(Debug, Clone)]
pub struct Config {
    pub blockchain_updates_url: String,
    pub updates_per_request: usize,
    pub max_wait_time_in_secs: u64,
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
        (&self.address, &self.key) == (&other.address, &other.key)
    }
}

impl Eq for DataEntry {}

impl Hash for DataEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.hash(state);
        self.key.hash(state);
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

impl PartialEq for InsertableDataEntry {
    fn eq(&self, other: &InsertableDataEntry) -> bool {
        (&self.address, &self.key) == (&other.address, &other.key)
    }
}

impl Eq for InsertableDataEntry {}

impl Hash for InsertableDataEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.hash(state);
        self.key.hash(state);
    }
}

#[derive(Clone, Debug, Insertable)]
#[table_name = "data_entries"]
pub struct DataEntryUpdate {
    pub superseded_by: i64,
    pub address: String,
    pub key: String,
}

#[async_trait]
pub trait DataEntriesSource {
    async fn fetch_updates(
        &self,
        tx: UnboundedSender<BlockchainUpdatesWithLastHeight>,
        from_height: u32,
        batch_max_size: usize,
        batch_max_time: Duration,
    ) -> Result<(), Error>;
}

#[derive(Clone, Debug, Insertable, QueryableByName)]
#[table_name = "blocks_microblocks"]
pub struct BlockMicroblock {
    pub id: String,
    pub time_stamp: Option<i64>,
    pub height: i32,
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
pub struct BlockchainUpdatesWithLastHeight {
    pub last_height: u32,
    pub updates: Vec<BlockchainUpdate>,
}

pub trait DataEntriesRepo {
    fn transaction(&self, f: impl FnOnce() -> Result<(), Error>) -> Result<(), Error>;

    fn get_block_uid(&self, block_id: &str) -> Result<i64, Error>;

    fn get_key_block_uid(&self) -> Result<i64, Error>;

    fn get_total_block_id(&self) -> Result<Option<String>, Error>;

    fn get_next_update_uid(&self) -> Result<i64, Error>;

    fn insert_blocks_or_microblocks(
        &self,
        blocks: &Vec<BlockMicroblock>,
    ) -> Result<Vec<i64>, Error>;

    fn insert_data_entries(&self, entries: &Vec<InsertableDataEntry>) -> Result<(), Error>;

    fn close_superseded_by(&self, updates: &Vec<DataEntryUpdate>) -> Result<(), Error>;

    fn reopen_superseded_by(&self, current_superseded_by: &i64) -> Result<(), Error>;

    fn set_next_update_uid(&self, uid: i64) -> Result<(), Error>;

    fn change_block_id(&self, block_uid: &i64, new_block_id: &str) -> Result<(), Error>;

    fn update_data_entries_block_references(&self, block_uid: &i64) -> Result<(), Error>;

    fn delete_microblocks(&self) -> Result<(), Error>;

    fn delete_last_block(&self) -> Result<Option<i32>, Error>;

    fn rollback_blocks_microblocks(&self, block_uid: &i64) -> Result<(), Error>;

    fn rollback_data_entries(&mut self, block_uid: &i64) -> Result<(), Error>;
}

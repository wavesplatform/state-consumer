pub mod daemon;
pub mod repo;
pub mod updates;

use crate::error::Error;
use crate::schema::data_entries;
use async_trait::async_trait;
use diesel::pg::Pg;
use diesel::serialize;
use diesel::sql_types::{BigInt, Nullable, Text, VarChar};
use diesel::types::ToSql;
use diesel::{Insertable, Queryable, QueryableByName};
use std::io::Write;

#[derive(Debug, Clone)]
pub struct Config {
    pub blockchain_updates_url: String,
    pub blocks_per_request: usize,
    pub starting_height: u32,
}

#[derive(Clone, Debug, QueryableByName)]
#[table_name = "data_entries"]
pub struct DataEntry {
    pub address: String,
    pub key: String,
    pub height: i32,
    pub value_binary: Option<Vec<u8>>,
    pub value_bool: Option<bool>,
    pub value_integer: Option<i64>,
    pub value_string: Option<String>,
}

#[derive(Clone, Debug, Insertable, QueryableByName)]
#[table_name = "data_entries"]
pub struct InsertableDataEntry {
    pub address: String,
    pub key: String,
    pub height: i32,
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

#[derive(SqlType, QueryId)]
#[postgres(type_name = "double_varchar_tuple")]
pub struct DoubleVarCharTupleType;

#[derive(Clone, Debug, Queryable, QueryableByName)]
#[table_name = "data_entries"]
pub struct DeletableDataEntry {
    pub address: String,
    pub key: String,
}

impl ToSql<DoubleVarCharTupleType, Pg> for DeletableDataEntry {
    fn to_sql<W: Write>(&self, out: &mut serialize::Output<W, Pg>) -> serialize::Result {
        serialize::WriteTuple::<(VarChar, VarChar)>::write_tuple(
            &(self.address.clone(), self.key.clone()),
            out,
        )
    }
}

#[derive(Clone, Debug, Queryable, QueryableByName)]
#[table_name = "data_entries"]
pub struct DeletableDataEntryWithHeight {
    pub address: String,
    pub key: String,
    pub height: i32,
}

#[async_trait]
pub trait DataEntriesSource {
    async fn fetch_updates(
        &self,
        from_height: u32,
        to_height: u32,
    ) -> Result<
        (
            i32,
            Vec<InsertableDataEntry>,
            Vec<DeletableDataEntryWithHeight>,
        ),
        Error,
    >;
}

pub trait DataEntriesRepo {
    fn get_last_handled_height(&self) -> Result<u32, Error>;

    fn set_last_handled_height(&mut self, new_height: u32) -> Result<(), Error>;

    fn insert_entries(&mut self, entries: &[InsertableDataEntry]) -> Result<(), Error>;

    fn delete_entries(&mut self, entries: &[DeletableDataEntry]) -> Result<(), Error>;
}

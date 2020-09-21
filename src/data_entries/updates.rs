use super::{
    BlockMicroblockAppend, BlockchainUpdate, BlockchainUpdatesWithLastHeight, DataEntriesSource,
    DataEntry,
};
use crate::error::Error;
use async_trait::async_trait;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::time::{Duration, Instant};
use waves_protobuf_schemas::waves::{
    data_transaction_data::data_entry::Value,
    events::{
        blockchain_updated::append::{BlockAppend, Body, MicroBlockAppend},
        blockchain_updated::Append,
        blockchain_updated::Update,
        grpc::{
            blockchain_updates_api_client::BlockchainUpdatesApiClient, SubscribeEvent,
            SubscribeRequest,
        },
        BlockchainUpdated,
    },
};

#[derive(Clone)]
pub struct DataEntriesSourceImpl {
    grpc_client: BlockchainUpdatesApiClient<tonic::transport::Channel>,
}

impl DataEntriesSourceImpl {
    pub async fn new(blockchain_updates_url: &str) -> Result<Self, Error> {
        Ok(Self {
            grpc_client: BlockchainUpdatesApiClient::connect(blockchain_updates_url.to_owned())
                .await?,
        })
    }
}

#[async_trait]
impl DataEntriesSource for DataEntriesSourceImpl {
    async fn fetch_updates(
        &self,
        from_height: u32,
        batch_max_size: usize,
        batch_max_wait_time: Duration,
    ) -> Result<BlockchainUpdatesWithLastHeight, Error> {
        let request = tonic::Request::new(SubscribeRequest {
            from_height: from_height as i32,
            to_height: 0,
        });

        let mut stream: tonic::Streaming<SubscribeEvent> = self
            .grpc_client
            .clone()
            .subscribe(request)
            .await?
            .into_inner();

        let mut result = vec![];
        let mut last_height = from_height;

        let now = Instant::now();
        let mut should_receive_more = true;
        while should_receive_more {
            match stream.message().await? {
                Some(SubscribeEvent {
                    update: Some(update),
                }) => {
                    last_height = update.height as u32;
                    match BlockchainUpdate::try_from(update) {
                        Ok(upd) => {
                            result.push(upd.clone());
                            match upd {
                                BlockchainUpdate::Block(_) => {
                                    if result.len() >= batch_max_size
                                        || now.elapsed().ge(&batch_max_wait_time)
                                    {
                                        should_receive_more = false;
                                    }
                                }
                                BlockchainUpdate::Microblock(_) | BlockchainUpdate::Rollback(_) => {
                                    should_receive_more = false
                                }
                            }
                        }
                        Err(_) => {}
                    }
                }
                _ => {}
            }
        }

        Ok(BlockchainUpdatesWithLastHeight {
            last_height: last_height,
            updates: result,
        })
    }
}

impl TryFrom<BlockchainUpdated> for BlockchainUpdate {
    type Error = Error;

    fn try_from(value: BlockchainUpdated) -> Result<Self, Self::Error> {
        match value.update {
            Some(Update::Append(Append {
                body,
                transaction_ids,
                transaction_state_updates,
                ..
            })) => {
                let height = value.height;

                let data_entries: HashSet<DataEntry> = transaction_state_updates
                    .iter()
                    .enumerate()
                    .flat_map::<HashSet<DataEntry>, _>(|(idx, su)| {
                        su.data_entries
                            .iter()
                            .map(|de| {
                                let deu = de.data_entry.as_ref().unwrap();

                                let mut value_string: Option<String> = None;
                                let mut value_integer: Option<i64> = None;
                                let mut value_bool: Option<bool> = None;
                                let mut value_binary: Option<Vec<u8>> = None;

                                match deu.value.as_ref() {
                                    Some(value) => match value {
                                        Value::IntValue(v) => value_integer = Some(v.to_owned()),
                                        Value::BoolValue(v) => value_bool = Some(v.to_owned()),
                                        Value::BinaryValue(v) => value_binary = Some(v.to_owned()),
                                        Value::StringValue(v) => {
                                            value_string = Some(v.replace("\0", "\\0").to_owned())
                                        }
                                    },
                                    None => {}
                                }

                                DataEntry {
                                    address: bs58::encode(&de.address).into_string(),
                                    // nul symbol is badly processed at least by PostgreSQL
                                    // so escape this for safety
                                    key: de
                                        .data_entry
                                        .as_ref()
                                        .unwrap()
                                        .key
                                        .clone()
                                        .replace("\0", "\\0"),
                                    transaction_id: bs58::encode(
                                        &transaction_ids.get(idx).unwrap(),
                                    )
                                    .into_string(),
                                    value_binary: value_binary,
                                    value_bool: value_bool,
                                    value_integer: value_integer,
                                    value_string: value_string,
                                }
                            })
                            .collect()
                    })
                    .collect();

                match body {
                    Some(Body::Block(BlockAppend { block, .. })) => {
                        Ok(BlockchainUpdate::Block(BlockMicroblockAppend {
                            id: bs58::encode(&value.id).into_string(),
                            time_stamp: block
                                .clone()
                                .map(|b| b.header.map(|h| Some(h.timestamp)).unwrap_or(None))
                                .unwrap_or(None),
                            height: height as u32,
                            data_entries: data_entries,
                        }))
                    }
                    Some(Body::MicroBlock(MicroBlockAppend { micro_block, .. })) => {
                        Ok(BlockchainUpdate::Microblock(BlockMicroblockAppend {
                            id: bs58::encode(&micro_block.as_ref().unwrap().total_block_id)
                                .into_string(),
                            time_stamp: None,
                            height: height as u32,
                            data_entries: data_entries,
                        }))
                    }
                    _ => Err(Error::InvalidMessage("Append body is empty.".to_string())),
                }
            }
            Some(Update::Rollback(_)) => Ok(BlockchainUpdate::Rollback(
                bs58::encode(&value.id).into_string(),
            )),
            _ => Err(Error::InvalidMessage(
                "Unknown blockchain update case.".to_string(),
            )),
        }
    }
}

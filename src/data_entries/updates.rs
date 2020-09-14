use super::{
    BlockMicroblockAppend, BlockchainUpdate, BlockchainUpdateWithHeight, DataEntriesSource,
    DataEntry,
};
use crate::error::Error;
use crate::log::APP_LOG;
use async_trait::async_trait;
use slog::info;
use std::collections::HashSet;
use waves_protobuf_schemas::waves::{
    data_transaction_data::data_entry::Value,
    events::{
        blockchain_updated::append::{BlockAppend, Body, MicroBlockAppend},
        blockchain_updated::Append,
        blockchain_updated::Update,
        grpc::blockchain_updates_api_client::BlockchainUpdatesApiClient,
        grpc::GetBlockUpdatesRangeRequest,
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
        to_height: u32,
    ) -> Result<BlockchainUpdateWithHeight, Error> {
        let request = tonic::Request::new(GetBlockUpdatesRangeRequest {
            from_height: from_height as i32,
            to_height: to_height as i32,
        });

        let updates: Vec<BlockchainUpdated> = self
            .grpc_client
            .clone()
            .get_block_updates_range(request)
            .await?
            .into_inner()
            .updates;

        let mut result = vec![];

        let mut last_height = from_height;

        info!(APP_LOG, "processing {:?} updates", updates.len());

        updates.iter().for_each(|u| match &u.update {
            Some(Update::Append(Append {
                body,
                transaction_ids,
                transaction_state_updates,
                ..
            })) => {
                let height = u.height;

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
                                        Value::StringValue(v) => value_string = Some(v.to_owned()),
                                    },
                                    None => {}
                                }

                                DataEntry {
                                    address: bs58::encode(&de.address).into_string(),
                                    key: de.data_entry.as_ref().unwrap().key.clone(),
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
                        result.push(BlockchainUpdate::Block(BlockMicroblockAppend {
                            id: bs58::encode(&u.id).into_string(),
                            time_stamp: block
                                .clone()
                                .map(|b| b.header.map(|h| Some(h.timestamp)).unwrap_or(None))
                                .unwrap_or(None),
                            height: height as u32,
                            data_entries: data_entries,
                        }));
                    }
                    Some(Body::MicroBlock(MicroBlockAppend { micro_block, .. })) => {
                        result.push(BlockchainUpdate::Microblock(BlockMicroblockAppend {
                            id: bs58::encode(&micro_block.as_ref().unwrap().total_block_id)
                                .into_string(),
                            time_stamp: None,
                            height: height as u32,
                            data_entries: data_entries,
                        }));
                    }
                    None => {}
                }
            }
            Some(Update::Rollback(_)) => result.push(BlockchainUpdate::Rollback(
                bs58::encode(&u.id).into_string(),
            )),
            _ => {}
        });

        match updates.iter().last() {
            Some(u) => last_height = u.height as u32,
            None => (),
        }

        Ok(BlockchainUpdateWithHeight {
            height: last_height,
            updates: result,
        })
    }
}

// #[cfg(test)]
// mod tests {
//     use super::DataEntriesSourceImpl;
//     use crate::data_entries::DataEntriesSource;
//     use crate::config::tests::BALANCES_STAGENET;

//     #[tokio::test]
//     async fn empty_block_range() {
//         let r = DataEntriesSourceImpl::new(
//             &BALANCES_STAGENET.blockchain_updates_url,
//         )
//         .await
//         .unwrap();

//         let updates = r.fetch_updates(1, 2).await.unwrap();

//         assert!(updates.is_empty());
//     }

//     #[tokio::test]
//     async fn usdn_updates_fetched_and_decoded() {
//         let height_with_usdn_transactions = 390882;

//         let r = BalancesSourceImpl::new(
//             &BALANCES_STAGENET.blockchain_updates_url,
//             &BALANCES_STAGENET.usdn_asset_id,
//         )
//         .await
//         .unwrap();

//         let updates = r
//             .fetch_updates(
//                 height_with_usdn_transactions,
//                 height_with_usdn_transactions + 1,
//             )
//             .await
//             .unwrap();

//         assert_eq!(updates.len(), 6);
//     }
// }

use super::{DataEntriesSource, DeletableDataEntryWithHeight, InsertableDataEntry};
use crate::error::Error;
use crate::log::APP_LOG;
use async_trait::async_trait;
use once_cell::sync::Lazy;
use regex::Captures;
use regex::Regex;
use slog::info;
use waves_protobuf_schemas::waves::{
    data_transaction_data::data_entry::Value,
    events::{
        blockchain_updated::Append, blockchain_updated::Update,
        grpc::blockchain_updates_api_client::BlockchainUpdatesApiClient,
        grpc::GetBlockUpdatesRangeRequest, BlockchainUpdated,
    },
};

const STRING_SEPARATOR: &str = "$";
const INTEGER_SEPARATOR: &str = "#";

static RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(&format!(r"^(\{0}\w*|{1}[0-9-]+)(\{0}\w*|{1}[0-9-]+)?(\{0}\w*|{1}[0-9-]+)?(\{0}\w*|{1}[0-9-]+)?(\{0}\w*|{1}[0-9-]+)?(\{0}\w*|{1}[0-9-]+)?(\{0}\w*|{1}[0-9-]+)?(\{0}\w*|{1}[0-9-]+)?(\{0}\w*|{1}[0-9-]+)?", STRING_SEPARATOR, INTEGER_SEPARATOR)).unwrap()
});

fn extract_string_fragment(caps: &Captures, position: usize) -> Option<String> {
    caps.get(position + 1).map_or(None, |fr| {
        if fr.as_str().starts_with(STRING_SEPARATOR) {
            Some(fr.as_str().trim_start_matches(STRING_SEPARATOR).to_owned())
        } else {
            None
        }
    })
}

fn extract_integer_fragment(caps: &Captures, position: usize) -> Option<i32> {
    caps.get(position + 1).map_or(None, |fr| {
        if fr.as_str().starts_with(INTEGER_SEPARATOR) {
            fr.as_str()
                .trim_start_matches(INTEGER_SEPARATOR)
                .parse()
                .ok()
        } else {
            None
        }
    })
}

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

    fn collect_data_entries(
        &self,
        update: &BlockchainUpdated,
    ) -> Result<(Vec<InsertableDataEntry>, Vec<DeletableDataEntryWithHeight>), Error> {
        match &update.update {
            Some(Update::Append(Append {
                transaction_state_updates,
                ..
            })) => {
                let height = update.height;

                let mut to_insert = Vec::new();
                let mut to_delete = Vec::new();

                transaction_state_updates.iter().for_each(|su| {
                    info!(APP_LOG, "su data entries {}", su.data_entries.len());
                    su.data_entries
                        .iter()
                        .filter(|de| {
                            self.is_suitable_for_index(&de.data_entry.as_ref().unwrap().key)
                        })
                        .for_each(|de| {
                            let key = &de.data_entry.as_ref().unwrap().key;
                            let caps = RE.captures(key).unwrap();

                            let mut value_string: Option<String> = None;
                            let mut value_integer: Option<i64> = None;
                            let mut value_bool: Option<bool> = None;
                            let mut value_binary: Option<Vec<u8>> = None;

                            if let Some(deu) = de.data_entry.as_ref() {
                                match deu.value.as_ref() {
                                    Some(value) => {
                                        match value {
                                            Value::IntValue(v) => {
                                                value_integer = Some(v.to_owned())
                                            }
                                            Value::BoolValue(v) => value_bool = Some(v.to_owned()),
                                            Value::BinaryValue(v) => {
                                                value_binary = Some(v.to_owned())
                                            }
                                            Value::StringValue(v) => {
                                                value_string = Some(v.to_owned())
                                            }
                                        }
                                        to_insert.push(InsertableDataEntry {
                                            address: bs58::encode(&de.address).into_string(),
                                            key: key.clone(),
                                            height: height,
                                            value_binary: value_binary,
                                            value_bool: value_bool,
                                            value_integer: value_integer,
                                            value_string: value_string,
                                            fragment_0_integer: extract_integer_fragment(&caps, 0),
                                            fragment_0_string: extract_string_fragment(&caps, 0),
                                            fragment_1_integer: extract_integer_fragment(&caps, 1),
                                            fragment_1_string: extract_string_fragment(&caps, 1),
                                            fragment_2_integer: extract_integer_fragment(&caps, 2),
                                            fragment_2_string: extract_string_fragment(&caps, 2),
                                            fragment_3_integer: extract_integer_fragment(&caps, 3),
                                            fragment_3_string: extract_string_fragment(&caps, 3),
                                            fragment_4_integer: extract_integer_fragment(&caps, 4),
                                            fragment_4_string: extract_string_fragment(&caps, 4),
                                            fragment_5_integer: extract_integer_fragment(&caps, 5),
                                            fragment_5_string: extract_string_fragment(&caps, 5),
                                            fragment_6_integer: extract_integer_fragment(&caps, 6),
                                            fragment_6_string: extract_string_fragment(&caps, 6),
                                            fragment_7_integer: extract_integer_fragment(&caps, 7),
                                            fragment_7_string: extract_string_fragment(&caps, 7),
                                            fragment_8_integer: extract_integer_fragment(&caps, 8),
                                            fragment_8_string: extract_string_fragment(&caps, 8),
                                            fragment_9_integer: extract_integer_fragment(&caps, 9),
                                            fragment_9_string: extract_string_fragment(&caps, 9),
                                            fragment_10_integer: extract_integer_fragment(
                                                &caps, 10,
                                            ),
                                            fragment_10_string: extract_string_fragment(&caps, 10),
                                        })
                                    }
                                    None => to_delete.push(DeletableDataEntryWithHeight {
                                        address: bs58::encode(&de.address).into_string(),
                                        key: key.clone(),
                                        height: height,
                                    }),
                                }
                            }
                        });
                });

                Ok((to_insert, to_delete))
            }
            _ => Err(Error::InvalidMessage(format!(
                "No valid block append field provided, got: {:?}",
                update,
            ))),
        }
    }

    fn is_suitable_for_index(&self, key: &str) -> bool {
        RE.is_match(key)
    }
}

#[async_trait]
impl DataEntriesSource for DataEntriesSourceImpl {
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
    > {
        info!(APP_LOG, "from {} to {}", from_height, to_height);
        let request = tonic::Request::new(GetBlockUpdatesRangeRequest {
            from_height: from_height as i32,
            to_height: to_height as i32,
        });

        let updates = self
            .grpc_client
            .clone()
            .get_block_updates_range(request)
            .await?
            .into_inner()
            .updates;

        let mut to_insert = vec![];
        let mut to_delete = vec![];
        let mut last_height = from_height as i32;

        info!(APP_LOG, "processing {:?} updates", updates.len());

        updates.iter().for_each(|u| {
            let mut next = self.collect_data_entries(u).unwrap();
            to_insert.append(next.0.as_mut());
            to_delete.append(next.1.as_mut());
        });

        match updates.iter().last() {
            Some(u) => last_height = u.height,
            None => (),
        }

        Ok((last_height, to_insert, to_delete))
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

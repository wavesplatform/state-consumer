use super::{
    BlockMicroblockAppend, BlockchainUpdate, BlockchainUpdatesWithLastHeight, DataEntriesSource,
    Transfer, Transfers,
};
use crate::error::AppError;
use anyhow::Result;
use async_trait::async_trait;
use std::convert::TryFrom;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{channel, Receiver, Sender};
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
    transaction::Data,
    InvokeScriptTransactionData, MassTransferTransactionData, PaymentTransactionData,
    SignedTransaction, Transaction, TransferTransactionData,
};

#[derive(Clone)]
pub struct DataEntriesSourceImpl {
    grpc_client: BlockchainUpdatesApiClient<tonic::transport::Channel>,
}

impl DataEntriesSourceImpl {
    pub async fn new(blockchain_updates_url: &str) -> Result<Self> {
        Ok(Self {
            grpc_client: BlockchainUpdatesApiClient::connect(blockchain_updates_url.to_owned())
                .await?,
        })
    }

    async fn run(
        &self,
        mut stream: tonic::Streaming<SubscribeEvent>,
        mut tx: Sender<BlockchainUpdatesWithLastHeight>,
        from_height: u32,
        batch_max_size: usize,
        batch_max_wait_time: Duration,
    ) -> Result<()> {
        let mut result = vec![];
        let mut last_height = from_height;

        let mut start = Instant::now();
        let mut should_receive_more = true;

        loop {
            match stream.message().await? {
                Some(SubscribeEvent {
                    update: Some(update),
                }) => Ok({
                    last_height = update.height as u32;
                    match BlockchainUpdate::try_from(update) {
                        Ok(upd) => Ok({
                            result.push(upd.clone());
                            match upd {
                                BlockchainUpdate::Block(_) => {
                                    if result.len() >= batch_max_size
                                        || start.elapsed().ge(&batch_max_wait_time)
                                    {
                                        should_receive_more = false;
                                    }
                                }
                                BlockchainUpdate::Microblock(_) | BlockchainUpdate::Rollback(_) => {
                                    should_receive_more = false
                                }
                            }
                        }),
                        Err(err) => Err(err),
                    }?;
                }),
                _ => Err(AppError::StreamReceiveEmpty(
                    "Empty message was received from the node.".to_string(),
                )),
            }?;

            if !should_receive_more {
                tx.send(BlockchainUpdatesWithLastHeight {
                    last_height: last_height,
                    updates: result.clone(),
                })
                .await?;
                should_receive_more = true;
                start = Instant::now();
                result.clear();
            }
        }
    }
}

#[async_trait]
impl DataEntriesSource for DataEntriesSourceImpl {
    async fn stream(
        self,
        from_height: u32,
        batch_max_size: usize,
        batch_max_wait_time: Duration,
    ) -> Result<Receiver<BlockchainUpdatesWithLastHeight>> {
        let request = tonic::Request::new(SubscribeRequest {
            from_height: from_height as i32,
            to_height: 0,
        });

        let stream: tonic::Streaming<SubscribeEvent> = self
            .grpc_client
            .clone()
            .subscribe(request)
            .await?
            .into_inner();

        let (tx, rx) = channel::<BlockchainUpdatesWithLastHeight>(batch_max_size);

        tokio::spawn(async move {
            self.run(stream, tx, from_height, batch_max_size, batch_max_wait_time)
                .await
        });

        Ok(rx)
    }
}

impl From<SignedTransaction> for Transfers {
    fn from(tx: SignedTransaction) -> Transfers {
        tx.transaction
            .and_then(|transaction| {
                let Transaction {
                    chain_id,
                    sender_public_key,
                    fee,
                    timestamp,
                    version,
                    data,
                } = transaction;

                data.map(|data| match data {
                    Data::InvokeScript(InvokeScriptTransactionData {
                        d_app, payments, ..
                    }) => unimplemented!(),

                    Data::Payment(PaymentTransactionData {
                        recipient_address,
                        amount,
                        ..
                    }) => unimplemented!(),

                    Data::Transfer(TransferTransactionData {
                        recipient, amount, ..
                    }) => unimplemented!(),

                    Data::MassTransfer(MassTransferTransactionData {
                        transfers,
                        asset_id,
                        ..
                    }) => unimplemented!(),

                    _ => vec![],
                })
            })
            .map_or_else(|| Transfers(vec![]), Transfers)
    }
}

impl TryFrom<BlockchainUpdated> for BlockchainUpdate {
    type Error = AppError;

    fn try_from(value: BlockchainUpdated) -> Result<Self, Self::Error> {
        use BlockchainUpdate::{Block, Microblock, Rollback};

        match value.update {
            Some(Update::Append(Append {
                body,
                transaction_ids,
                transaction_state_updates,
                ..
            })) => {
                let height = value.height;

                let txs: Vec<SignedTransaction> = match body {
                    Some(Body::Block(BlockAppend { ref block, .. })) => {
                        Ok(block.clone().map(|it| it.transactions))
                    }
                    Some(Body::MicroBlock(MicroBlockAppend {
                        ref micro_block, ..
                    })) => Ok(micro_block
                        .clone()
                        .and_then(|it| it.micro_block.map(|it| it.transactions))),
                    _ => Err(AppError::InvalidMessage(
                        "Append body is empty.".to_string(),
                    )),
                }
                .map_or_else(
                    |_| vec![],
                    |txs| {
                        txs.iter()
                            .filter_map(|tx| match tx {
                                InvokeScriptTransactionData => None,
                                MassTransferTransactionData => None,
                                PaymentTransactionData => None,
                                TransferTransactionData => None,
                            })
                            .collect()
                    },
                );

                let transfers: Vec<Transfer> = txs
                    .into_iter()
                    .flat_map(|tx| {
                        let t: Transfers = tx.into();
                        t.0
                    })
                    .collect();

                match body {
                    Some(Body::Block(BlockAppend { block, .. })) => {
                        Ok(Block(BlockMicroblockAppend {
                            id: bs58::encode(&value.id).into_string(),
                            time_stamp: block
                                .clone()
                                .map(|b| b.header.map(|h| Some(h.timestamp)).unwrap_or(None))
                                .unwrap_or(None),
                            height: height as u32,
                            data_entries: vec![],
                            transfers,
                        }))
                    }
                    Some(Body::MicroBlock(MicroBlockAppend { micro_block, .. })) => {
                        Ok(Microblock(BlockMicroblockAppend {
                            id: bs58::encode(&micro_block.as_ref().unwrap().total_block_id)
                                .into_string(),
                            time_stamp: None,
                            height: height as u32,
                            data_entries: vec![],
                            transfers,
                        }))
                    }
                    _ => Err(AppError::InvalidMessage(
                        "Append body is empty.".to_string(),
                    )),
                }
            }
            Some(Update::Rollback(_)) => Ok(Rollback(bs58::encode(&value.id).into_string())),
            _ => Err(AppError::InvalidMessage(
                "Unknown blockchain update.".to_string(),
            )),
        }
    }
}

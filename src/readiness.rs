use crate::data_entries::DataEntriesRepo;
use crate::{data_entries::repo::DataEntriesRepoImpl, SyncMode};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use wavesexchange_log::error;
use wavesexchange_warp::endpoints::liveness::Readiness;

pub fn channel(
    repo: Arc<DataEntriesRepoImpl>,
    mut sync_mode_rx: UnboundedReceiver<SyncMode>,
    max_block_age: std::time::Duration,
) -> UnboundedReceiver<Readiness> {
    // здесь нужно:
    // 1. отслеживать sync_mode_rx и класть последнее состояние в какую-нибудь переменную
    // 2. запустить поток, который будет:
    //   - опрашивать репо на предмет last block timestamp
    //   - сравнивать с now
    //   - при отставании, большем max_block_age, но только если
    //     ОДНОВРЕМЕННО с этим текущий режим — SyncMode::Realtime,
    //     слать в out канал Readiness::Dead

    // после этого продолжать опрашивать блоки, и продолжать слушать sync_mode_rx
    // если какой-то из параметров перестал удовлетворять условию «мёртвости»,
    // нужно «оживить» сервис, послав сообщение Readiness::Ready

    let (readiness_tx, readiness_rx) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(async move {
        let mut current_mode = SyncMode::Historical;
        loop {
            tokio::select! {
                mode = sync_mode_rx.recv() => {
                    if let Some(received_mode) = mode {
                        current_mode = received_mode;
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    match repo.get_last_block_timestamp() {
                        Ok(last_block_timestamp) => {
                            match last_block_timestamp.time_stamp {
                                Some(timestamp) => {
                                    let now = chrono::Utc::now().timestamp_millis();
                                    if (now - timestamp) > max_block_age.as_millis() as i64 && current_mode == SyncMode::Realtime {
                                        readiness_tx.send(Readiness::Dead).unwrap();
                                    } else {
                                        readiness_tx.send(Readiness::Ready).unwrap();
                                    }
                                },
                                None => {
                                    error!("Could not get last block timestamp");
                                    readiness_tx.send(Readiness::Ready).unwrap();
                                },
                            }
                        }
                        Err(err) => {
                            error!("Error while fetching last block timestamp: {}", err);
                            readiness_tx.send(Readiness::Dead).unwrap();
                        }
                    }
                }
            }
        }
    });

    readiness_rx
}

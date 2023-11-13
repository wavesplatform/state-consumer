use crate::data_entries::{DataEntriesRepo, DataEntriesRepoOperations};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::UnboundedReceiver;
use wavesexchange_log::{debug, error};
use wavesexchange_warp::endpoints::Readiness;

const POLL_INTERVAL_SECS: u64 = 60;

struct LastBlock {
    timestamp: i64,
    last_change: Instant,
}

pub fn channel<U>(repo: Arc<U>, max_block_age: std::time::Duration) -> UnboundedReceiver<Readiness>
where
    U: DataEntriesRepo + Send + Sync + 'static,
{
    let (readiness_tx, readiness_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut last_block = LastBlock {
        timestamp: 0,
        last_change: Instant::now(),
    };

    tokio::spawn(async move {
        loop {
            let send = |status: Readiness| {
                if readiness_tx.send(status).is_err() {
                    error!("Failed to send {:?} status", status);
                }
            };

            tokio::time::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECS)).await;

            match repo.execute(|ops| ops.get_last_block_timestamp()) {
                Ok(last_block_timestamp) => {
                    if let Some(timestamp) = last_block_timestamp.time_stamp {
                        debug!("Current timestamp: {}", timestamp);
                        let now = Instant::now();
                        if timestamp > last_block.timestamp {
                            last_block.timestamp = timestamp;
                            last_block.last_change = now;
                            debug!("Sending status: Ready");
                            send(Readiness::Ready);
                        } else {
                            if now.duration_since(last_block.last_change) > max_block_age {
                                debug!("Sending status: Dead");
                                send(Readiness::Dead);
                            } else {
                                debug!("Sending status: Ready");
                                send(Readiness::Ready);
                            }
                        }
                    } else {
                        error!("Could not get last block timestamp");
                        send(Readiness::Ready);
                    }
                }
                Err(err) => {
                    error!("Error while fetching last block timestamp: {}", err);
                    send(Readiness::Dead);
                }
            }
        }
    });

    readiness_rx
}

use crate::data_entries::{DataEntriesRepo, DataEntriesRepoOperations};
use crate::SyncMode;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::Mutex;
use wavesexchange_log::{debug, error};
use wavesexchange_warp::endpoints::Readiness;

const POLL_INTERVAL_SECS: u64 = 60;

pub fn channel<U>(
    repo: Arc<U>,
    mut sync_mode_rx: UnboundedReceiver<SyncMode>,
    max_block_age: std::time::Duration,
) -> UnboundedReceiver<Readiness>
where
    U: DataEntriesRepo + Send + Sync + 'static,
{
    let (readiness_tx, readiness_rx) = tokio::sync::mpsc::unbounded_channel();

    let sync_mode = Arc::new(Mutex::new(SyncMode::Historical));
    let sync_mode_clone = sync_mode.clone();

    tokio::spawn(async move {
        while let Some(received_mode) = sync_mode_rx.recv().await {
            let mut current_mode = sync_mode.lock().await;
            *current_mode = received_mode;
            debug!("Current mode: {:?}", *current_mode);
        }
    });

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
                        let now = chrono::Utc::now().timestamp_millis();
                        let current_mode = sync_mode_clone.lock().await;
                        if (now - timestamp) > max_block_age.as_millis() as i64
                            && *current_mode == SyncMode::Realtime
                        {
                            debug!("Sending status: Dead");
                            send(Readiness::Dead);
                        } else {
                            debug!("Sending status: Ready");
                            send(Readiness::Ready);
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

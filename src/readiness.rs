use crate::{data_entries::repo::DataEntriesRepoImpl, SyncMode};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use wavesexchange_warp::endpoints::liveness::Readiness;

pub fn stream(
    _repo: Arc<DataEntriesRepoImpl>,
    _sync_mode_rx: UnboundedReceiver<SyncMode>,
    _max_block_age: std::time::Duration,
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
    todo!("impl")
}

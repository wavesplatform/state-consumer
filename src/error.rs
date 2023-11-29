use crate::data_entries::BlockchainUpdatesWithLastHeight;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("LoadConfigFailed: {0}")]
    LoadConfigFailed(#[from] envy::Error),
    #[error("GrpcTransportError: {0}")]
    GrpcTransportError(#[from] tonic::transport::Error),
    #[error("GrpcError: {0}")]
    GrpcError(#[from] tonic::Status),
    #[error("InvalidMessage: {0}")]
    InvalidMessage(String),
    #[error("InvalidBase58String: {0}")]
    InvalidBase58String(#[from] bs58::decode::Error),
    #[error("DbError: {0}")]
    DbError(#[from] diesel::result::Error),
    #[error("ConnectionError: {0}")]
    ConnectionError(#[from] diesel::ConnectionError),
    #[error("SendError: {0}")]
    SendError(#[from] tokio::sync::mpsc::error::SendError<BlockchainUpdatesWithLastHeight>),
    #[error("JoinError: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("StreamClosed: {0}")]
    StreamClosed(String),
    #[error("LivenessCheckFailed: {0}")]
    LivenessCheckFailed(String),
}

impl Into<String> for AppError {
    fn into(self) -> String {
        self.to_string()
    }
}

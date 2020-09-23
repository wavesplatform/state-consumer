use crate::data_entries::BlockchainUpdatesWithLastHeight;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("LoadConfigFailed: {0}")]
    LoadConfigFailed(envy::Error),
    #[error("GrpcTransportError: {0}")]
    GrpcTransportError(tonic::transport::Error),
    #[error("GrpcError: {0}")]
    GrpcError(tonic::Status),
    #[error("InvalidMessage: {0}")]
    InvalidMessage(String),
    #[error("InvalidBase58String: {0}")]
    InvalidBase58String(bs58::decode::Error),
    #[error("DbError: {0}")]
    DbError(diesel::result::Error),
    #[error("ConnectionError: {0}")]
    ConnectionError(diesel::ConnectionError),
    #[error("SendError: {0}")]
    SendError(tokio::sync::mpsc::error::SendError<BlockchainUpdatesWithLastHeight>),
    #[error("JoinError: {0}")]
    JoinError(tokio::task::JoinError),
    #[error("RecvEmpty: {0}")]
    RecvEmpty(String),
}

use AppError::*;

impl From<diesel::ConnectionError> for AppError {
    fn from(v: diesel::ConnectionError) -> Self {
        ConnectionError(v)
    }
}

impl From<diesel::result::Error> for AppError {
    fn from(v: diesel::result::Error) -> Self {
        DbError(v)
    }
}

impl From<bs58::decode::Error> for AppError {
    fn from(v: bs58::decode::Error) -> Self {
        InvalidBase58String(v)
    }
}

impl From<tonic::Status> for AppError {
    fn from(v: tonic::Status) -> Self {
        GrpcError(v)
    }
}

impl From<tonic::transport::Error> for AppError {
    fn from(v: tonic::transport::Error) -> Self {
        GrpcTransportError(v)
    }
}

impl From<envy::Error> for AppError {
    fn from(err: envy::Error) -> Self {
        LoadConfigFailed(err)
    }
}

impl From<tokio::sync::mpsc::error::SendError<BlockchainUpdatesWithLastHeight>> for AppError {
    fn from(err: tokio::sync::mpsc::error::SendError<BlockchainUpdatesWithLastHeight>) -> Self {
        SendError(err)
    }
}

impl From<tokio::task::JoinError> for AppError {
    fn from(err: tokio::task::JoinError) -> Self {
        JoinError(err)
    }
}

impl Into<String> for AppError {
    fn into(self) -> String {
        self.to_string()
    }
}

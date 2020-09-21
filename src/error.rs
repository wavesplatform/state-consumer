use crate::data_entries::BlockchainUpdatesWithLastHeight;
use std::fmt::Display;

#[derive(Debug)]
pub enum Error {
    LoadConfigFailed(envy::Error),
    GrpcTransportError(tonic::transport::Error),
    GrpcError(tonic::Status),
    InvalidMessage(String),
    InvalidBase58String(bs58::decode::Error),
    DbError(diesel::result::Error),
    ConnectionError(diesel::ConnectionError),
    SendError(tokio::sync::mpsc::error::SendError<BlockchainUpdatesWithLastHeight>),
    JoinError(tokio::task::JoinError),
    RecvEmpty(String)
}

use Error::*;

impl From<diesel::ConnectionError> for Error {
    fn from(v: diesel::ConnectionError) -> Self {
        ConnectionError(v)
    }
}

impl From<diesel::result::Error> for Error {
    fn from(v: diesel::result::Error) -> Self {
        DbError(v)
    }
}

impl From<bs58::decode::Error> for Error {
    fn from(v: bs58::decode::Error) -> Self {
        InvalidBase58String(v)
    }
}

impl From<tonic::Status> for Error {
    fn from(v: tonic::Status) -> Self {
        GrpcError(v)
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(v: tonic::transport::Error) -> Self {
        GrpcTransportError(v)
    }
}

impl From<envy::Error> for Error {
    fn from(err: envy::Error) -> Self {
        LoadConfigFailed(err)
    }
}

impl From<tokio::sync::mpsc::error::SendError<BlockchainUpdatesWithLastHeight>> for Error {
    fn from(err: tokio::sync::mpsc::error::SendError<BlockchainUpdatesWithLastHeight>) -> Self {
        SendError(err)
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(err: tokio::task::JoinError) -> Self {
        JoinError(err)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadConfigFailed(err) => write!(f, "LoadConfigFailed: {}", err),
            GrpcTransportError(err) => write!(f, "GrpcTransportError: {}", err),
            GrpcError(err) => write!(f, "GrpcError: {}", err),
            InvalidMessage(message) => write!(f, "InvalidMessage: {}", message),
            InvalidBase58String(err) => write!(f, "InvalidBase58String: {}", err),
            DbError(err) => write!(f, "DbError: {}", err),
            ConnectionError(err) => write!(f, "ConnectionError: {}", err),
            SendError(err) => write!(f, "SendError: {}", err),
            JoinError(err) => write!(f, "JoinError: {}", err),
            RecvEmpty(err) => write!(f, "RecvEmpty: {}", err),
        }
    }
}

impl Into<String> for Error {
    fn into(self) -> String {
        self.to_string()
    }
}

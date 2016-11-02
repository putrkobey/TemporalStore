// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use async_trait::async_trait;
use bytes::Bytes;
use serde::Serialize;
use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Error)]
pub enum ObjectStoreError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("invalid object key: {0}")]
    InvalidKey(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("http error: {0}")]
    Http(String),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// A durable compare-and-set precondition failed: the object either did not hold the
    /// expected bytes (lost a race / stale owner) or the backend does not support a fenced
    /// conditional put. This is the single-writer FENCE signal — callers MUST treat it as a
    /// hard abort, never a retryable transient error.
    #[error("object store condition failed for {key}: {detail}")]
    ConditionFailed { key: String, detail: String },
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize)]
pub struct AppendBlobReceipt {
    pub key: String,
    pub start_offset: u64,
    pub end_offset: u64,
    pub bytes_written: u64,
    pub object_length: u64,
    pub physical_slab_count: usize,
    pub first_physical_offset: Option<u64>,
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError>;
    async fn append_blob(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<AppendBlobReceipt, ObjectStoreError> {
        let _ = (key, bytes);
        Err(ObjectStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "this object store does not implement append_blob; replacing an object is not an append",
        )))
    }

    async fn put_many(&self, objects: &[(String, Bytes)]) -> Result<(), ObjectStoreError> {
        for (key, bytes) in objects {
            self.put(key, bytes.clone()).await?;
        }
        Ok(())
    }
    /// Durable compare-and-set: atomically replace the object at `key` with `new` iff its
    /// current contents exactly equal `expected` (`None` meaning "the object must not exist").
    /// This is the substrate for a single-writer fence/lease: a stale owner that has been
    /// superseded observes a value it does not expect and is rejected with
    /// [`ObjectStoreError::ConditionFailed`]. The default implementation is FAIL-CLOSED — it
    /// refuses rather than performing an unfenced read-modify-write — so a backend without a
    /// real atomic conditional put can never silently degrade the fence to a racy no-op.
    /// Overridden with a real cross-process atomic CAS by [`FileObjectStore`].
    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<Bytes>,
        new: Bytes,
    ) -> Result<(), ObjectStoreError> {
        let _ = (expected, new);
        Err(ObjectStoreError::ConditionFailed {
            key: key.to_string(),
            detail: "compare_and_swap unsupported by this object store backend".to_string(),
        })
    }
    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError>;
    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, ObjectStoreError> {
        let bytes = self.get(key).await?;
        let start = usize::try_from(offset)
            .map_err(|_| ObjectStoreError::InvalidKey(format!("{key}: range offset too large")))?;
        let len = usize::try_from(length)
            .map_err(|_| ObjectStoreError::InvalidKey(format!("{key}: range length too large")))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| ObjectStoreError::InvalidKey(format!("{key}: range overflow")))?;
        if end > bytes.len() {
            return Err(ObjectStoreError::InvalidKey(format!(
                "{key}: range {offset}..{} exceeds object length {}",
                offset.saturating_add(length),
                bytes.len()
            )));
        }
        Ok(bytes.slice(start..end))
    }
    async fn get_many(&self, keys: &[String]) -> Result<Vec<(String, Bytes)>, ObjectStoreError> {
        let mut objects = Vec::with_capacity(keys.len());
        for key in keys {
            objects.push((key.clone(), self.get(key).await?));
        }
        Ok(objects)
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError>;
    async fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<String>, ObjectStoreError> {
        Ok(self
            .list(prefix)
            .await?
            .into_iter()
            .filter(|key| key.as_str() > after)
            .collect())
    }
    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError>;
    fn uri(&self, key: &str) -> String;
}

#[derive(Debug, Clone)]
pub struct MatrixObjectHttpStore {
    addr: String,
    uri: String,
    rpc_streams: Arc<Mutex<Vec<TcpStream>>>,
}

impl MatrixObjectHttpStore {
    pub fn new(uri: &str) -> Result<Self, ObjectStoreError> {
        let uri = uri.trim();
        let addr = uri
            .strip_prefix("matrixobject://")
            .ok_or_else(|| ObjectStoreError::InvalidKey(uri.to_string()))?
            .trim_end_matches('/')
            .to_string();
        if addr.is_empty() {
            return Err(ObjectStoreError::InvalidKey(uri.to_string()));
        }
        Ok(Self {
            addr,
            uri: uri.trim_end_matches('/').to_string(),
            rpc_streams: Arc::default(),
        })
    }
}

#[async_trait]
impl ObjectStore for MatrixObjectHttpStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        self.rpc_request(RPC_PUT, key, bytes.as_ref())?;
        Ok(())
    }

    async fn put_many(&self, objects: &[(String, Bytes)]) -> Result<(), ObjectStoreError> {
        if objects.is_empty() {
            return Ok(());
        }
        match self.rpc_request(RPC_PUT_MANY, "", &encode_rpc_put_many_request(objects)) {
            Ok(_) => Ok(()),
            Err(_) => {
                for (key, bytes) in objects {
                    self.put(key, bytes.clone()).await?;
                }
                Ok(())
            }
        }
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        let response = self.rpc_request(RPC_GET, key, &[])?;
        Ok(Bytes::from(response))
    }

    async fn get_many(&self, keys: &[String]) -> Result<Vec<(String, Bytes)>, ObjectStoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        match self.rpc_request(RPC_GET_MANY, "", keys.join("\n").as_bytes()) {
            Ok(response) => parse_rpc_get_many_response(response),
            Err(_) => {
                let mut objects = Vec::with_capacity(keys.len());
                for key in keys {
                    objects.push((key.clone(), self.get(key).await?));
                }
                Ok(objects)
            }
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        match self.rpc_request(RPC_LIST, prefix, &[]) {
            Ok(response) => Ok(parse_rpc_list_response(&response)),
            Err(_) => {
                let response: ListObjectResponse = matrixobject_post(
                    &self.addr,
                    "/v1/object/list",
                    &ListObjectRequest { prefix },
                )?;
                Ok(response.keys)
            }
        }
    }

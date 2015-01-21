// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::io::{Read, Write};
use std::net::TcpStream;

use crate::proxy_client::ProxyClient;
use crate::proxy_helpers::{io_error, json_error, parse_http_endpoint};
use crate::{Error, Result};

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub(crate) fn proxy_service_body(
        &self,
        key: &str,
        extra: &[(&str, serde_json::Value)],
    ) -> serde_json::Value {
        let mut body = serde_json::json!({
            "namespace": self.options.namespace_name,
            "table_name": self.options.table_name,
            "key": key,
        });
        let object = body.as_object_mut().expect("object body");
        for (name, value) in extra {
            object.insert((*name).to_string(), value.clone());
        }
        body
    }

    pub(crate) fn proxy_service_execute(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let response = self.proxy_service_request(path, body)?;
        Ok(response
            .get("response")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})))
    }

    pub(crate) fn proxy_service_request(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let response = self.post_raw(path, body)?;
        let status_ok = response
            .get("status")
            .and_then(|status| status.get("ok"))
            .and_then(|ok| ok.as_bool())
            .unwrap_or(false);
        if !status_ok {
            return Err(Error {
                code: 0,
                message: response
                    .get("status")
                    .and_then(|status| status.get("message"))
                    .and_then(|value| value.as_str())
                    .unwrap_or("proxy service request failed")
                    .to_string(),
            });
        }

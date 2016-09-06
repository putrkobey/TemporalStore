// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Every route this SDK posts to must be one the proxy answers.
//!
//! #1040 found eleven routes here that the dispatcher replies 404 to, and fixed them. Three of
//! those -- ControlStateCPCSet, ControlStateCPCQuery and ControlStateHquery -- had no served
//! equivalent, so they were left off this SDK rather than redirected; whether to add the routes
//! to the proxy or leave the methods off is a product decision that is still open.
//!
//! Methods calling all three were sitting in `src/proxy_control_state_cpc.rs` and
//! `src/proxy_control_state_hash.rs` the whole time. They were invisible because nothing compiled
//! those files, and restoring the module split would have shipped them. This test is what makes
//! the decision durable: the route list below is a claim that each entry is served, and anything
//! added to the SDK fails here until someone states which it is.
//!
//! The dispatcher's own arms are in `crates/temporalstore-rust/src/proxy/proxy_handle.rs`. This crate is
//! published standalone and cannot depend on it, so the list is checked by hand against those arms
//! and pinned here; it is not derived at run time.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

/// Routes the proxy serves and this SDK is allowed to call.
///
/// Adding a line here asserts that `crates/temporalstore-rust/src/proxy/proxy_handle.rs` matches it for
/// POST. If it does not, the call can only ever fail at run time.
const SERVED_ROUTES: &[&str] = &[
    "/ProxyService/BatchExecuteCmd",
    "/ProxyService/ControlStateCount",
    "/ProxyService/ControlStateFolQuery",
    "/ProxyService/ControlStateFolSet",
    "/ProxyService/ControlStateHset",
    "/ProxyService/ControlStateIncrement",
    "/ProxyService/ControlStateManager",
    "/ProxyService/Delete",
    "/ProxyService/ExecuteCmd",
    "/ProxyService/Exists",
    "/ProxyService/Expire",
    "/ProxyService/FeatureAdd",
    "/ProxyService/FeatureAggQuery",
    "/ProxyService/FeatureDelete",
    "/ProxyService/FeatureQuery",
    "/ProxyService/FeatureReplace",
    "/ProxyService/Get",
    "/ProxyService/GetConfig",
    "/ProxyService/HDel",
    "/ProxyService/HGet",
    "/ProxyService/HGetAll",
    "/ProxyService/HLen",
    "/ProxyService/HMGet",
    "/ProxyService/HMSet",
    "/ProxyService/HSet",
    "/ProxyService/OpenTable",
    "/ProxyService/RefreshTopology",
    "/ProxyService/SAdd",
    "/ProxyService/SMembers",
    "/ProxyService/SRem",
    "/ProxyService/SequenceAdd",
    "/ProxyService/SequenceQuery",
    "/ProxyService/Set",
    "/ProxyService/SetEx",
    "/ProxyService/TableBatchExecuteCmd",
    "/ProxyService/TableExecuteCmd",
    "/ProxyService/Ttl",
    "/ProxyService/UpdateConfig",
];

/// Routes the dispatcher has no arm for. Named rather than merely absent, so that re-adding a
/// method for one of them fails with the reason instead of with a diff.
const UNSERVED_ROUTES: &[&str] = &[
    "/ProxyService/ControlStateCPCQuery",
    "/ProxyService/ControlStateCPCSet",
    "/ProxyService/ControlStateHquery",
];

/// Route literals the SDK's own sources contain.
fn routes_called() -> BTreeSet<String> {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut found = BTreeSet::new();
    let mut dirs = vec![src];
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(&dir).expect("read src/") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                dirs.push(path);
                continue;

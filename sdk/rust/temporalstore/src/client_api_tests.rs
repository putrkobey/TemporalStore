// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#[cfg(feature = "direct")]
use crate::Client;
#[cfg(feature = "proxy")]
use crate::ProxyClient;

#[cfg(feature = "direct")]
#[test]
fn direct_client_exposes_c_abi_parity_methods() {
    let _: fn(&Client, &str, &str) -> crate::Result<()> = Client::put_string;
    let _: fn(&Client, &str, &str, u64) -> crate::Result<()> = Client::put_string_with_ttl;
    let _: fn(&Client, &str) -> crate::Result<String> = Client::get_string;
    let _: fn(&Client, &str, &str, &str) -> crate::Result<()> = Client::hset;
    let _: fn(&Client, &str, &str) -> crate::Result<String> = Client::hget;
    let _: fn(&Client, &str) -> crate::Result<Vec<(String, String)>> = Client::hgetall;
    let _: fn(&Client, &str) -> crate::Result<Vec<(String, String)>> = Client::scan_hash;
    let _: fn(&Client, &str, &str) -> crate::Result<()> = Client::hdel;
    let _: fn(&Client, &str) -> crate::Result<()> = Client::delete_object;
    let _: fn(&Client, &str, u64) -> crate::Result<()> = Client::expire;
    let _: fn(&Client, &str) -> crate::Result<u64> = Client::ttl;
    let _: fn(&Client, &str, &str) -> crate::Result<()> = Client::sadd;
    let _: fn(&Client, &str) -> crate::Result<Vec<String>> = Client::smembers;
    let _: fn(&Client, &str, &[crate::FeaturePoint]) -> crate::Result<()> =
        Client::add_feature_points;
    let _: fn(&Client, &str, u64, u64, u64) -> crate::Result<Vec<crate::FeaturePoint>> =
        Client::query_feature_points;
    let _: fn(
        &Client,
        &str,
        u64,
        u64,
        u64,
        &[crate::FeatureFilter],
    ) -> crate::Result<Vec<crate::FeaturePoint>> = Client::query_feature_points_filtered;
    let _: fn(
        &Client,
        &str,
        i64,
        u64,
        crate::ControlStatePrecision,
        &str,
        u64,
    ) -> crate::Result<()> = Client::control_state_increment;
    let _: fn(
        &Client,
        &str,

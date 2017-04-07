// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::state::SeenSet;

pub fn serialize<S>(value: &HashMap<std::sync::Arc<str>, SeenSet>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let encoded = value
        .iter()
        .map(|(key, seen)| {
            (
                key,
                seen.by_member
                    .iter()
                    .map(|(member, seen_at)| (member, seen_at))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    encoded.serialize(serializer)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<std::sync::Arc<str>, SeenSet>, D::Error>
where
    D: Deserializer<'de>,
{
    let encoded = HashMap::<String, Vec<(Vec<u8>, u64)>>::deserialize(deserializer)?;
    Ok(encoded
        .into_iter()

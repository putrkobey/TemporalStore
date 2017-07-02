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

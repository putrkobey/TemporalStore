// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::block_store::ElementEntry;

pub fn serialize<S>(
    value: &HashMap<super::state::ModelKey, BTreeMap<Vec<u8>, ElementEntry>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let encoded = value
        .iter()
        .map(|(key, members)| {
            (
                key,
                members
                    .iter()
                    .map(|(member, address)| (member, address))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    encoded.serialize(serializer)
}

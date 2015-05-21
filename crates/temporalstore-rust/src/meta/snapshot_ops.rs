// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! SingleNodeMeta snapshot export/install/save/load, extracted from meta.rs.

use super::*;

impl SingleNodeMeta {
    pub fn with_mutation_log(path: impl Into<PathBuf>) -> io::Result<Self> {
        let log = LocalMetaMutationLog::new(path)?;
        let meta = Self {
            mutation_log: Some(log.clone()),
            ..Self::default()
        };
        for record in log.load()? {
            // A line written before the time was recorded gets the current
            // clock, which is exactly what it has always been given.
            let at_ms = if record.at_ms == 0 {
                now_ms()
            } else {
                record.at_ms
            };
            meta.apply_mutation_at(record.mutation, at_ms);
        }
        Ok(meta)
    }

    pub fn export_snapshot(&self) -> MetaSnapshot {
        let state = self.inner.read().expect("meta lock poisoned");
        MetaSnapshot::from_state(&state, &self.counters)
    }

    pub(crate) fn state_from_snapshot(snapshot: MetaSnapshot) -> Result<MetaState, Status> {
        if snapshot.format_version != 1 {
            return Err(Status::error(
                "bad_snapshot",
                "unsupported metaserver snapshot version",
            ));
        }
        let mut tables = BTreeMap::new();
        let mut next_table_id = snapshot.next_table_id.max(1);
        for table in snapshot.tables {
            if table.namespace.is_empty() || table.table_name.is_empty() {
                return Err(Status::error(
                    "bad_snapshot",
                    "snapshot contains invalid table name",
                ));
            }
            next_table_id = next_table_id.max(table.table_id.saturating_add(1));
            let key = table_key(&table.namespace, &table.table_name);
            if tables.insert(key, TableRecord { info: table }).is_some() {
                return Err(Status::error(
                    "bad_snapshot",
                    "snapshot contains duplicate table",
                ));
            }
        }
        let mut restored = MetaState {
            shards: snapshot.shards.into_iter().collect(),
            servers: snapshot.servers,
            proxies: snapshot.proxies,
            proxy_groups: snapshot.proxy_groups,
            namespaces: snapshot.namespaces,
            tables,
            next_table_id,
            topology_version: snapshot.topology_version,
            topology_events: snapshot.topology_events,

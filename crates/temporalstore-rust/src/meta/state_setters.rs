// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! SingleNodeMeta server/proxy/table state setters, extracted from meta.rs.

use super::*;

impl SingleNodeMeta {
    pub(super) fn set_server_state(&self, request: StateChangeRequest, next: MetaEntityState) -> AckResponse {
        // Covers freeze, unfreeze and drop for servers in one place.
        if let Some(status) = self.meta_change_refusal() {
            return AckResponse { status };
        }
        if !self
            .inner
            .read()
            .expect("meta lock poisoned")
            .servers
            .contains_key(&request.endpoint)
        {
            return AckResponse {
                status: Status::error("not_found", "server not found"),
            };
        }
        let at_ms = self.record_mutation(match next {
            MetaEntityState::Frozen => MetaMutation::FreezeServer(request.clone()),
            MetaEntityState::Dropped => MetaMutation::DropServer(request.clone()),
            // Recorded like any other state change: an unfreeze that is not
            // replayed would be silently undone by mutation-log recovery.
            MetaEntityState::Normal => MetaMutation::UnfreezeServer(request.clone()),
        });
        self.apply_set_server_state(request, next, at_ms)
    }

    pub(super) fn apply_set_server_state(
        &self,

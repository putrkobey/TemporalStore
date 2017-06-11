// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::types::Command;

pub(super) fn proxy_command_is_write(command: &Command) -> bool {
    // Delegate to the engine's write-command classifier -- the same one that gates WAL
    // persistence, and the one the client and the data node already delegate to after a
    // hand-maintained subset drifted in each of them.
    //

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Subscribing to metadata change, rather than polling for it.
//!
//! Every metadata change already funnels through `record_topology_event`, which
//! stamps a version and appends to a bounded ring. Reading it meant polling that
//! ring and diffing, so anything that wanted to *react* to a change -- a proxy
//! refreshing a route, an operator tool watching a drain -- had to ask
//! repeatedly and work out for itself what was new.
//!
//! This turns that ring into something you can subscribe to. It deliberately
//! does not publish from inside `record_topology_event`: that runs while the
//! metadata write lock is held, and fanning out to subscribers there would put
//! their cost, and any one of them misbehaving, inside the lock. A pump reads
//! the ring on its own and fans out afterwards, so publishing stays exactly as
//! cheap as it was.

use super::*;
use std::collections::BTreeSet;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::Mutex;

/// How many undelivered events one subscriber may bank up before the oldest are
/// dropped. A subscriber that stops reading must not be able to grow the
/// metaserver's memory, and must not be able to slow anyone else down.
pub const SUBSCRIBER_QUEUE_DEPTH: usize = 128;

/// One metadata change, plus what the subscriber missed before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopologyNotice {
    pub event: TopologyChangeEvent,
    /// Events this subscriber did not receive between the previous notice and
    /// this one, either because it was not reading fast enough or because the
    /// ring recycled them before the pump got there.
    ///
    /// Reported rather than hidden: a subscriber that silently misses a drain
    /// or a freeze is worse than one that knows it has fallen behind.
    pub missed: u64,
}

/// A handle on the stream. Dropping it unsubscribes on the next fan-out.
#[derive(Debug)]
pub struct TopologySubscription {
    pub id: u64,
    receiver: Receiver<TopologyNotice>,
}

impl TopologySubscription {

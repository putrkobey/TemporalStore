// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Named points where a test can make the code stop or stall.
//!
//! A durable operation is usually several steps -- write, sync, rename, sync again -- and the
//! interesting failures live BETWEEN them. Testing those by hand means building the post-crash
//! state yourself: delete this, truncate that, reopen, check. That is only a test of the real
//! window if the state you built is the state the crash would have left, and getting that wrong
//! produces a test that passes for the wrong reason.
//!
//! A named point removes the guessing. Arm `wal/roll/after_rename`, run the roll, and the process
//! stops exactly there -- so what is on disk is, by construction, what a crash at that line leaves.
//!
//! ```ignore
//! let _armed = fault::arm("wal/roll/after_rename", FaultAction::Stop);
//! let outcome = std::panic::catch_unwind(|| store.append(...));
//! assert!(outcome.is_err());        // stopped mid-roll
//! // ... reopen and assert recovery does the right thing
//! ```
//!
//! Arming is per THREAD and lasts until the guard drops. That is deliberate: this suite runs
//! ~1200 tests across 16 threads, and its most persistent flakiness comes from tests reaching for
//! process-wide state. A point armed process-wide would stop unrelated tests at the same line.
//!
//! Cost when nothing is armed is one thread-local read. Points belong at rare, structural moments
//! -- a roll, a reclaim, a manifest install -- not on the per-record path.

use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Duration;

/// What an armed point does when reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultAction {
    /// Stop here, by panicking. The caller catches it; on disk, everything up to this line has
    /// happened and nothing after it has, which is what a crash at this line leaves behind.
    Stop,
    /// Stall here, to open a window another thread can act inside.
    Hang(Duration),
}

thread_local! {
    static ARMED: RefCell<HashMap<&'static str, FaultAction>> = RefCell::new(HashMap::new());
}

/// Arm `name` on THIS thread until the returned guard drops.
pub fn arm(name: &'static str, action: FaultAction) -> FaultGuard {
    ARMED.with(|armed| armed.borrow_mut().insert(name, action));
    FaultGuard { name }
}

/// Disarms its point when dropped, including while a panic unwinds -- which is the normal way a
/// `Stop` point ends, so leaving it armed would strand it for every later test on this thread.
pub struct FaultGuard {
    name: &'static str,
}

impl Drop for FaultGuard {
    fn drop(&mut self) {
        ARMED.with(|armed| armed.borrow_mut().remove(self.name));
    }
}

/// Reach a named point. Does nothing unless this thread has armed it.

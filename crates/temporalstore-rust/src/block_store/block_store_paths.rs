// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Every directory fsync this module issues, counted for the life of the process.
///
/// COUNTED RATHER THAN TIMED. What an fsync costs depends on the device, on the page cache and on
/// what else the box is doing; how MANY of them a round issues is the shape itself, and it reads
/// the same number on a loaded machine as on an idle one.
///
/// THE COUNTER SITS ON THE PRIMITIVE, NOT ON THE CALL SITE. A count taken inside
/// `move_slab_to_delayed_destroy` would stop seeing the fsyncs the moment they moved out of it --
/// which is exactly the change this exists to measure, so the instrument would go blind on the
/// one edit it is watching for. At `sync_dir`/`sync_parent_dir` it cannot: every directory fsync
/// the block store performs goes through one of these two functions, wherever it is called from.
///

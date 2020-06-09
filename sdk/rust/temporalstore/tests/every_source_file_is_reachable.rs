// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Every `.rs` file under `src/` must be reachable from a crate target.
//!
//! This crate shipped 88 files the compiler never saw. `src/lib.rs` had been reduced to a module
//! root in July 2026 and was then overwritten by a bulk commit that restored the pre-split
//! monolith and dropped every `mod` declaration with it; `src/bin/matrixark_rust_proxy_impl.rs`
//! lost its own 47 declarations the same way a week earlier. The files stayed in the tree, stayed
//! in the published tarball, and went on being hand-edited by later commits -- but nothing
//! compiled them, so none of those edits could fail, and two of them silently reverted fixes that
//! had already landed in the copy that did compile.
//!
//! A comment cannot catch that recurring. `cargo build` cannot either: a file nothing names is
//! exactly as green as a correct one. So the rule is asserted here, against the file system.

use std::collections::{BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Target roots, taken from the manifest's own `path = "..."` declarations rather than from a
/// list kept here. A new `[[bin]]` is then covered the moment it is declared, and a root this
/// test does not know about cannot make the sweep quietly pass.
fn declared_roots(manifest: &str) -> BTreeSet<PathBuf> {
    let mut roots = BTreeSet::new();
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        let Some(rest) = line.strip_prefix("path") else {
            continue;
        };
        let Some(rest) = rest.trim_start().strip_prefix('=') else {
            continue;
        };
        if let Some(value) = quoted(rest) {
            if value.ends_with(".rs") && value.starts_with("src/") {
                roots.insert(PathBuf::from(value));
            }
        }
    }
    roots
}

/// The first double-quoted run in `text`.
fn quoted(text: &str) -> Option<String> {
    let start = text.find('"')? + 1;
    let end = text[start..].find('"')? + start;
    Some(text[start..end].to_string())
}

/// `mod name;` declarations and `include!("...")` invocations in one file, as (explicit path,
/// module name) pairs. An explicit `#[path = "..."]` wins over name-based resolution.
fn declarations(source: &str) -> Vec<(Option<String>, Option<String>)> {
    let mut out = Vec::new();
    let mut pending_path: Option<String> = None;
    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("//") {
            continue;
        }

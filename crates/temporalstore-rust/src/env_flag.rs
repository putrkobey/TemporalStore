// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! One vocabulary for boolean environment flags.
//!
//! Six hand-rolled readers disagreed about what an operator had written. Four of them
//! (`metaserver`, `server`, `raft_node`, and `storage_backend::env_truthy`) matched the raw
//! string against `"1" | "true" | "TRUE" | "yes" | "YES"` with no trim and no lowercasing, and
//! treated everything else as `false` rather than as "not specified". For a flag whose default
//! is `true` that is the dangerous direction, because the value an operator is most likely to
//! write in order to KEEP it on turns it off:
//!
//! | written | that reader | this one |
//! |---|---|---|
//! | `on` / `On` / `ON` | `false` | `true` |
//! | `True` | `false` | `true` |
//! | `" 1"` (a stray space, as a unit file or heredoc leaves) | `false` | `true` |
//! | `wat` (anything unrecognised) | `false` | the default |
//! | `""` (set but empty) | `false` | the default |
//!
//! Every default-on flag in the tree reached one of those readers:
//! `TS_META_AUTO_REBALANCE_BALANCE`, `TS_META_REBALANCE_LOCATION_SCOPED`,
//! `TS_META_REBALANCE_PER_TABLE`, `TS_MATRIXOBJECT_CHECKPOINT_ON_START`,
//! `TS_MATRIXOBJECT_NETWORKED_CHECKPOINT_ON_START` and `TS_RAFT_ALLOW_PLAINTEXT`.
//!
//! The python side of this repository already settled the same argument and wrote it down in
//! `tools/test_env_flag_vocabulary.py`: "Boolean flags were parsed in six different
//! vocabularies. They disagreed on the two words an operator is most likely to reach for."
//! This is that vocabulary, for rust.

/// The words a boolean flag understands, or `None` when the value is not one of them.
///
/// `None` is deliberately distinct from `Some(false)`: a value nobody can read is not a
/// request to turn something off, and the caller falls back to the flag's own default.
pub fn parse_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Read `name` as a boolean, falling back to `default` when it is unset, empty, or holds
/// something outside the vocabulary.
pub fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|raw| parse_bool(&raw))
        .unwrap_or(default)
}

/// Read `name` as a number, falling back to `default` when it is unset, empty, or holds
/// something that does not parse.
///
/// The numeric half of the same argument [`env_bool`] settles. `usize::from_str` rejects
/// surrounding whitespace outright -- `" 64 ".parse::<usize>()` is an `Err` -- so a reader that
/// hands it the raw value discards what the operator set and falls back to the default with no
/// message. A shell export, a systemd `Environment=` line, a heredoc and a `.env` file all leave
/// whitespace behind, and the boolean reader directly above already tolerates every one of them:
///
/// | written | a raw `value.parse()` | this one |
/// |---|---|---|
/// | `64` | `64` | `64` |
/// | `" 64"` / `"64 "` / `"\t64\n"` | the default | `64` |
/// | `wat` | the default | the default |
///
/// Fourteen local copies of this function existed, in seven files, and none of them trimmed. The
/// crate had settled the vocabulary question for booleans and left the numeric twin sitting
/// directly underneath it in the same files.
pub fn env_number<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<T>().ok())
        .unwrap_or(default)
}

/// The value of a numeric flag, or `None` when it is not one.
///
/// `None` is distinct from a zero the same way [`parse_bool`]'s is from `Some(false)`: a value
/// nobody can read is not a request for zero. Callers that treat zero as "unset" keep doing that
/// themselves -- `env_usize_any` filters it out on purpose -- rather than having it decided here.
pub fn parse_number<T: std::str::FromStr>(raw: &str) -> Option<T> {
    raw.trim().parse::<T>().ok()
}

/// The value of `name`, or `None` when it is unset OR set to nothing.
///
/// `std::env::var` answers `Ok("")` for a variable that is present and empty -- which is what
/// `export NEW=$UNSET` leaves behind, and what clearing a field means. Read through `or_else`,
/// that `Ok("")` is the newer spelling WINNING with nothing in it, and the older spelling it was
/// meant to replace is never consulted however correctly a deployment set it:
///
/// ```text
/// TS_BLOCK_SLAB_TARGET_BYTES=""        the previous name held 2 MiB
/// -> neither honoured; the built-in 1 GiB default applied
///
/// TS_DATA_RAFT_READ_MODE=""            TS_SERVER_RAFT_READ_MODE=linearizable
/// -> panic!("invalid TS_DATA_RAFT_READ_MODE"), and the data node exits at startup
/// ```
///
/// The python side settled this and wrote it down in
/// `tools/test_a_blank_flag_falls_through_to_the_older_spelling.py`, and two chains in
/// `context_workflow/model_provider.rs` already spell the filter out by hand. This is that rule,
/// in one place, for the rest of them.
///
/// Whitespace-only counts as nothing, for the same reason [`env_number`] trims: a value a shell
/// leaves as `" "` is not a value.
pub fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

/// The value of the FIRST of `names` that holds one, or `None` when none of them does.
///
/// The milestone vocabulary rename moves operator-facing flag names, and an environment
/// variable is the one surface where a rename cannot be a clean break: nothing errors when a
/// name stops being read. The flag simply turns OFF, the built-in default applies, and a
/// deployment that was setting the old name keeps running with different behaviour and no
/// message. Spelling the previous name after the current one keeps such a deployment working.
///
/// Order is precedence: put the CURRENT name first and each previous spelling after it. Reads
/// go through [`env_value`], so a current name that is present but BLANK falls through to the
/// previous one rather than shadowing it with nothing.
pub fn env_value_any(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| env_value(name))
}

/// [`env_bool`] over a list of names, current spelling first. See [`env_value_any`].
pub fn env_bool_first(names: &[&str], default: bool) -> bool {
    env_value_any(names)
        .and_then(|raw| parse_bool(&raw))
        .unwrap_or(default)
}

/// [`env_number`] over a list of names, current spelling first. See [`env_value_any`].
pub fn env_number_first<T: std::str::FromStr>(names: &[&str], default: T) -> T {
    env_value_any(names)
        .and_then(|raw| parse_number::<T>(&raw))
        .unwrap_or(default)
}

/// The canonical directory default, unless the PREVIOUS spelling is the one that already exists.
///
/// A MOVED DEFAULT IS A DATA MIGRATION for every deployment that set neither spelling of the
/// variable. Moving it outright would bring the process up pointing at an empty directory while
/// the real blocks sat beside it under the old name, serving nothing, with no error anywhere --
/// the same silent-miss shape a renamed stored key has, arriving through the filesystem instead.
///
/// So the canonical name is chosen only when the previous directory is not already there: an
/// existing store keeps being served under the name it was created with, and a fresh one is
/// created under the canonical name. When BOTH exist the canonical one wins, because that is the
/// one this build writes and a deployment that wants the other has an explicit variable for it.
///
/// This is the filesystem's version of accepting both spellings, and it is NOT a substitute for
/// moving the data: it keeps an old directory readable rather than declaring it canonical.
pub fn directory_default_preferring_canonical(canonical: &str, previous: &str) -> String {
    if !std::path::Path::new(canonical).exists() && std::path::Path::new(previous).exists() {
        return previous.to_string();
    }
    canonical.to_string()
}

#[cfg(test)]
mod tests {

    /// The moved default picks the directory that EXISTS, and the canonical one otherwise.
    ///
    /// Four arms, because the interesting one is the third: with only the previous directory on
    /// disk the canonical name must NOT be returned. An implementation that simply returned the
    /// canonical name passes the first, second and fourth arms.
    #[test]
    fn a_moved_directory_default_keeps_serving_the_directory_that_exists() {
        let root = tempfile::tempdir().expect("tempdir");
        let canonical = root.path().join("blocks");
        let previous = root.path().join("pages");
        let c = canonical.to_string_lossy().to_string();
        let p = previous.to_string_lossy().to_string();

        // ARM 1: neither exists -- a fresh deployment is created under the canonical name.
        assert_eq!(
            super::directory_default_preferring_canonical(&c, &p),
            c,
            "a fresh deployment must be created under the canonical name"
        );


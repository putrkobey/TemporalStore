// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Redis string/expire/increment command handlers, extracted from redis.rs.

use super::*;
use crate::types::{Command, CommandResponse};

#[derive(Debug, Clone, Copy)]
pub(crate) struct SetOptions {
    pub(crate) ttl_ms: Option<u64>,
    pub(crate) condition: StringSetCondition,
    pub(crate) return_old: bool,
    /// `KEEPTTL`: replace the value, leave the deadline alone.
    ///
    /// Without it, `SET` has only two answers for the deadline on the key it is replacing --
    /// arm this new one, or throw the old one away -- and a caller who wanted the value
    /// changed and the countdown kept had to read the remaining time and write it back, which
    /// races the countdown it is trying to preserve and loses whatever elapsed in between.
    pub(crate) keep_ttl: bool,
}

pub(crate) fn parse_set_options(args: &[Vec<u8>]) -> Result<SetOptions, String> {
    let mut options = SetOptions {
        ttl_ms: None,
        condition: StringSetCondition::Always,
        return_old: false,
        keep_ttl: false,
    };
    let mut index = 0;
    while index < args.len() {
        match upper(&args[index]).as_str() {
            "EX" => {
                // `KEEPTTL` and an arming TTL are contradictory requests, so the pair is a
                // syntax error rather than a silent precedence rule. Checked BEFORE the
                // number is parsed so `SET k v KEEPTTL EX 0` is refused for the reason it is
                // actually wrong, not for the expire time.
                if options.keep_ttl {
                    return Err("ERR syntax error".to_string());
                }
                let Some(value) = args.get(index + 1) else {
                    return Err("ERR syntax error".to_string());
                };
                let seconds = parse_u64(value, "seconds")?;
                // rejects a non-positive expiry rather than writing an already-expired key.
                if seconds == 0 {
                    return Err("ERR invalid expire time in set".to_string());
                }
                if options.ttl_ms.replace(seconds.saturating_mul(1000)).is_some() {
                    return Err("ERR syntax error".to_string());
                }
                index += 2;
            }
            "PX" => {
                if options.keep_ttl {
                    return Err("ERR syntax error".to_string());
                }
                let Some(value) = args.get(index + 1) else {
                    return Err("ERR syntax error".to_string());
                };
                let milliseconds = parse_u64(value, "milliseconds")?;
                if milliseconds == 0 {
                    return Err("ERR invalid expire time in set".to_string());
                }
                if options.ttl_ms.replace(milliseconds).is_some() {
                    return Err("ERR syntax error".to_string());
                }
                index += 2;
            }
            "KEEPTTL" => {
                // Both directions of the contradiction are refused: `EX`/`PX` beside
                // `KEEPTTL` (checked in those arms) and `KEEPTTL` beside one already parsed
                // (checked here). A repeat of `KEEPTTL` alone is a syntax error too, matching
                // how `EX`, `NX` and `GET` each refuse their own repetition above and below.
                if options.ttl_ms.is_some() || options.keep_ttl {
                    return Err("ERR syntax error".to_string());
                }
                options.keep_ttl = true;
                index += 1;
            }
            "NX" => {
                if options.condition != StringSetCondition::Always {
                    return Err("ERR syntax error".to_string());
                }
                options.condition = StringSetCondition::IfNotExists;
                index += 1;
            }
            "XX" => {
                if options.condition != StringSetCondition::Always {
                    return Err("ERR syntax error".to_string());
                }
                options.condition = StringSetCondition::IfExists;
                index += 1;
            }
            "GET" => {
                if options.return_old {
                    return Err("ERR syntax error".to_string());
                }
                options.return_old = true;
                index += 1;
            }
            _ => return Err("ERR syntax error".to_string()),
        }
    }
    Ok(options)
}

/// A deadline that has ALREADY PASSED is a deletion, not a deadline.
///
/// The three commands below can all be handed a moment that is not in the future: `EXPIRE` and

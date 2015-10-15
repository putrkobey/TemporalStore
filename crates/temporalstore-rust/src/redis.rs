// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Redis/RESP-compatible command surface.
//!
//! Parses RESP command frames and dispatches them to the engine's data models:
//! strings, hashes, sets, features (and the sequence view), and the control-state
//! families (Counter/Distinct/Selection). Redis-native verbs (`SET`, `HSET`,
//! `SADD`, ...) and the TemporalStore extension verbs share one dispatch path;
//! see [`dispatch`] for the command table and [`command_table`] for the advertised
//! `COMMAND` descriptors.

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

#[path = "redis/redis_protocol.rs"]
mod protocol;
#[path = "redis/redis_proxy_server.rs"]
mod server;
#[path = "redis/redis_command_state.rs"]
mod state;
mod keyspace_commands;
mod string_commands;
mod set_commands;
mod response_helpers;
mod command_table;
#[path = "redis/redis_dispatch.rs"]
mod dispatch;

pub use protocol::{read_command, RespValue};
pub use server::serve_redis_proxy;
pub use state::RedisCommandState;
pub use dispatch::execute_redis_command_with_state;

use crate::client::{bucket_id_for_key, stable_key_hash};
use crate::types::{
    parse_feature_filters, Command, CommandResponse, FeatureFilter,
    FeaturePoint, ControlStateFamily, ControlStateSelectionType, ShardId, StringSetCondition,
};

use keyspace_commands::*;
pub(crate) use set_commands::*;
pub(crate) use response_helpers::*;
pub(crate) use command_table::*;
pub(crate) use string_commands::*;

pub fn execute_redis_command(
    args: Vec<Vec<u8>>,
    shard_id: ShardId,
    mut execute: impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let mut state = RedisCommandState::default();
    execute_redis_command_with_state(args, shard_id, &mut state, |command| execute(command))
}


fn control_state_family_for_command(command: &str) -> ControlStateFamily {
    // Accept both the historical family verbs (CPC*/FOL*/H*) and the descriptive
    // spellings (DISTINCT*/SELECTION*/COUNTER*) that match the renamed families.
    if command.starts_with("CPC") || command.starts_with("DISTINCT") {
        ControlStateFamily::Distinct
    } else if command.starts_with("FOL") || command.starts_with("SELECTION") {
        ControlStateFamily::Selection
    } else {
        // H*, COUNTER*, CONTROLSTATEH*
        ControlStateFamily::Counter
    }
}

fn control_state_family_key_for_resp(family: ControlStateFamily, key: &str) -> String {
    let family_name = match family {
        ControlStateFamily::Counter => "h",
        ControlStateFamily::Distinct => "cpc",
        ControlStateFamily::Selection => "fol",
    };
    format!("control_state:{family_name}:{key}")
}

fn redis_config_response(args: &[Vec<u8>], state: &mut RedisCommandState) -> RespValue {
    match upper(&args[1]).as_str() {
        "GET" if args.len() == 3 => {
            let key = string_arg(&args[2]);
            match state.config.get(&key) {
                Some(value) => RespValue::Array(vec![
                    RespValue::Bulk(Some(key.into_bytes())),
                    RespValue::Bulk(Some(value.clone().into_bytes())),
                ]),
                None => RespValue::Array(Vec::new()),
            }
        }
        "SET" if args.len() == 4 => {
            state
                .config
                .insert(string_arg(&args[2]), string_arg(&args[3]));
            RespValue::SimpleString("OK".to_string())
        }
        "REWRITE" if args.len() == 2 => RespValue::SimpleString("OK".to_string()),
        _ => RespValue::Error("ERR syntax error".to_string()),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum RedisStoredValue {
    String(Vec<u8>),
    Hash(Vec<(String, Vec<u8>)>),
    Set(Vec<Vec<u8>>),
}

fn copy_or_rename_key_response(
    source: &[u8],
    destination: &[u8],
    rename: bool,
    replace: bool,
    state: &mut RedisCommandState,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    let source_key = string_arg(source);
    let destination_key = string_arg(destination);
    let value = match load_stored_key_value(&source_key, execute) {
        Ok(Some(value)) => value,
        Ok(None) if rename => return RespValue::Error("ERR no such key".to_string()),
        Ok(None) => return RespValue::Integer(0),
        Err(err) => return RespValue::Error(err),
    };
    if !replace {
        match execute(Command::CommonExists {
            key: destination_key.clone(),
        }) {
            Ok(CommandResponse::Integer { value }) if value > 0 => {
                return if rename {
                    RespValue::Integer(0)
                } else {
                    RespValue::Integer(0)
                };
            }
            Ok(CommandResponse::Integer { .. }) => {}
            Ok(_) => return RespValue::Error("ERR invalid exists response".to_string()),
            Err(err) => return RespValue::Error(format!("ERR {err}")),
        }
    }
    if let Err(err) = store_stored_key_value(&destination_key, value, execute) {
        return RespValue::Error(err);
    }
    state.keyspace.insert(destination_key);
    if rename {
        if let Err(err) = execute(Command::CommonDelete {
            key: source_key.clone(),
        }) {
            return RespValue::Error(format!("ERR {err}"));
        }
        state.keyspace.remove(&source_key);
        RespValue::SimpleString("OK".to_string())
    } else {
        RespValue::Integer(1)
    }
}

fn load_stored_key_value(
    key: &str,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<Option<RedisStoredValue>, String> {
    match execute(Command::StringGet {
        key: key.to_string(),
    }) {
        Ok(CommandResponse::Bytes { value: Some(value) }) => {
            return Ok(Some(RedisStoredValue::String(value)));
        }
        Ok(CommandResponse::Bytes { value: None }) => {}
        Ok(_) => return Err("ERR invalid string response".to_string()),
        Err(err) => return Err(format!("ERR {err}")),
    }
    match execute(Command::HashGetAll {
        key: key.to_string(),
    }) {
        Ok(CommandResponse::HashEntries { entries }) if !entries.is_empty() => {
            return Ok(Some(RedisStoredValue::Hash(entries)));
        }
        Ok(CommandResponse::HashEntries { .. }) => {}
        Ok(_) => return Err("ERR invalid hash response".to_string()),
        Err(err) => return Err(format!("ERR {err}")),
    }
    match execute(Command::SetMembers {
        key: key.to_string(),
    }) {
        Ok(CommandResponse::Members { members }) if !members.is_empty() => {
            return Ok(Some(RedisStoredValue::Set(members)));
        }
        Ok(CommandResponse::Members { .. }) => {}
        Ok(_) => return Err("ERR invalid set response".to_string()),
        Err(err) => return Err(format!("ERR {err}")),
    }
    Ok(None)
}

fn store_stored_key_value(
    key: &str,
    value: RedisStoredValue,
    execute: &mut impl FnMut(Command) -> Result<CommandResponse, String>,
) -> Result<(), String> {
    match value {
        RedisStoredValue::String(value) => execute(Command::StringSet {
            key: key.to_string(),
            value,
        })
        .map(|_| ())
        .map_err(|err| format!("ERR {err}")),
        RedisStoredValue::Hash(entries) => execute(Command::HashMultiSet {
            key: key.to_string(),
            entries,
        })
        .map(|_| ())
        .map_err(|err| format!("ERR {err}")),
        RedisStoredValue::Set(members) => {
            for member in members {
                execute(Command::SetAdd {
                    key: key.to_string(),
                    member,
                })
                .map_err(|err| format!("ERR {err}"))?;
            }
            Ok(())
        }
    }
}

fn redis_partition_response(
    args: &[Vec<u8>],
    shard_id: ShardId,
    state: &mut RedisCommandState,
) -> RespValue {
    match upper(&args[1]).as_str() {
        "LOAD" if args.len() >= 3 => {
            let loaded = parse_u64(&args[2], "partition_id").unwrap_or(shard_id);
            state.loaded_shard_id = Some(loaded);
            RespValue::SimpleString("OK".to_string())
        }
        "UNLOAD" if args.len() >= 3 => {
            state.loaded_shard_id = None;
            RespValue::SimpleString("OK".to_string())
        }
        "INFO" if args.len() == 2 || args.len() == 3 => RespValue::Bulk(Some(
            format!(
                "partition_id:{}\r\npartition_loading_stats:{}\r\n",
                state.loaded_shard_id.unwrap_or(shard_id),
                if state.loaded_shard_id.is_some() {
                    "loaded"
                } else {
                    "not_exist"
                }
            )
            .into_bytes(),
        )),
        _ => RespValue::Error("ERR syntax error".to_string()),
    }
}

fn redis_command_response(args: &[Vec<u8>]) -> RespValue {
    match args.get(1).map(|value| upper(value)) {
        None => RespValue::Array(
            redis_supported_commands()
                .iter()
                .map(|command| RespValue::Bulk(Some(command.name.as_bytes().to_vec())))
                .collect(),
        ),
        Some(subcommand) if subcommand == "COUNT" && args.len() == 2 => {
            RespValue::Integer(redis_supported_commands().len() as i64)
        }
        Some(subcommand) if subcommand == "INFO" && args.len() >= 3 => {
            let commands = redis_supported_commands();
            RespValue::Array(
                args.iter()
                    .skip(2)
                    .map(|name| {
                        let name = upper(name);
                        commands
                            .iter()
                            .find(|command| command.name == name)
                            .map(|command| {
                                RespValue::Array(vec![
                                    RespValue::Bulk(Some(command.name.as_bytes().to_vec())),
                                    RespValue::Integer(command.arity),
                                    RespValue::Array(
                                        command
                                            .flags
                                            .iter()
                                            .map(|flag| {
                                                RespValue::Bulk(Some(flag.as_bytes().to_vec()))
                                            })
                                            .collect(),
                                    ),
                                ])
                            })
                            .unwrap_or(RespValue::Bulk(None))
                    })
                    .collect(),
            )
        }
        _ => RespValue::Error("ERR syntax error".to_string()),
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RedisCommandDescriptor {
    name: &'static str,
    arity: i64,
    flags: &'static [&'static str],
}


fn redis_info(section: &str, shard_id: ShardId, state: &RedisCommandState) -> String {
    let section = section.to_ascii_lowercase();
    let all = section == "all";
    let default = section == "default";
    let mut parts = Vec::new();
    if all || default || section == "server" {
        parts.push(
            "# Server\r\nredis_version:temporalstore-rust\r\nredis_mode:temporalstore\r\narch_bits:64\r\nmultiplexing_api:std-tcp\r\n".to_string(),
        );
    }
    if all || default || section == "clients" {
        parts.push("# Clients\r\nconnected_clients:1\r\nblocked_clients:0\r\n".to_string());
    }
    if all || default || section == "memory" {
        let maxmemory = state
            .config
            .get("maxmemory")
            .map(String::as_str)
            .unwrap_or("0");
        let policy = state
            .config
            .get("maxmemory-policy")
            .map(String::as_str)
            .unwrap_or("noeviction");
        parts.push(format!(
            "# Memory\r\nused_memory:0\r\nmaxmemory:{maxmemory}\r\nmaxmemory_policy:{policy}\r\n"
        ));
    }
    if all || default || section == "stats" {
        let loading = if state.loaded_shard_id.is_some() {
            "loaded"
        } else {
            "not_exist"
        };
        parts.push(format!(
            "# Stats\r\npartition_loading_stats:{loading}\r\ntotal_commands_processed:0\r\n"
        ));
    }
    if all || default || section == "replication" {
        let mut replication = "# Replication\r\n".to_string();
        if let Some((host, port)) = &state.master {
            replication.push_str("role:slave\r\n");
            replication.push_str(&format!(
                "master_host:{host}\r\nmaster_port:{port}\r\nmaster_link_status:up\r\n"
            ));
        } else {
            replication.push_str("role:master\r\n");
        }
        replication.push_str("connected_slaves:0\r\n");
        parts.push(replication);
    }
    if all || default || section == "cluster" {
        parts.push(format!(
            "# Cluster\r\ncluster_enabled:0\r\nloaded_shard_id:{}\r\n",
            state.loaded_shard_id.unwrap_or(shard_id)
        ));
    }
    if parts.is_empty() {
        return String::new();
    }
    parts.join("\r\n")
}


#[cfg(test)]
mod tests {
    use std::io::BufReader;

    use super::*;
    use crate::engine::TemporalEngine;
    use crate::types::{ExecuteRequest, SequenceFeatureRow};

    #[test]
    fn decrby_and_srandmember_guard_i64_min_like_native() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut state = RedisCommandState::default();
        let run = |state: &mut RedisCommandState, args: Vec<&str>| {
            execute_redis_command_with_state(
                args.into_iter().map(|arg| arg.as_bytes().to_vec()).collect(),
                1,
                state,
                |command| {
                    let response = engine.execute(ExecuteRequest {
                        shard_id: 1,
                        command,
                    });
                    if response.status.ok {
                        Ok(response.response)
                    } else {
                        Err(response.status.message)
                    }
                },
            )
        };

        // DECRBY i64::MIN: negating it overflows i64. returns an overflow error; Rust
        // must not panic (debug) or wrap-and-store a wrong value (release).
        run(&mut state, vec!["SET", "n", "0"]);
        assert!(matches!(
            run(&mut state, vec!["DECRBY", "n", "-9223372036854775808"]),
            RespValue::Error(_)
        ));
        assert_eq!(
            run(&mut state, vec!["GET", "n"]),
            RespValue::Bulk(Some(b"0".to_vec())),
            "the failed DECRBY must not have mutated the key"
        );

        // SRANDMEMBER with the i64::MIN negative count must be BOUNDED (fills zero
        // elements). unsigned_abs() would attempt ~2^63 iterations (hang / OOM).
        run(&mut state, vec!["SADD", "s", "a", "b", "c"]);
        assert!(
            matches!(
                run(&mut state, vec!["SRANDMEMBER", "s", "-9223372036854775808"]),
                RespValue::Array(ref members) if members.len() <= 3
            ),
            "SRANDMEMBER with i64::MIN count must be bounded, not an unbounded allocation"
        );
    }

    #[test]
    fn ttl_expire_match_native() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut state = RedisCommandState::default();
        let run = |state: &mut RedisCommandState, args: Vec<&str>| {
            execute_redis_command_with_state(
                args.into_iter().map(|arg| arg.as_bytes().to_vec()).collect(),
                1,
                state,
                |command| {
                    let response = engine.execute(ExecuteRequest {
                        shard_id: 1,
                        command,
                    });
                    if response.status.ok {
                        Ok(response.response)
                    } else {
                        Err(response.status.message)
                    }
                },
            )
        };

        // TTL sentinels pass through; positive ms rounds UP to seconds.
        assert_eq!(run(&mut state, vec!["TTL", "missing"]), RespValue::Integer(-2));
        run(&mut state, vec!["SET", "k", "v"]);
        assert_eq!(run(&mut state, vec!["TTL", "k"]), RespValue::Integer(-1));
        run(&mut state, vec!["PEXPIRE", "k", "1500"]);
        assert_eq!(
            run(&mut state, vec!["TTL", "k"]),
            RespValue::Integer(2),
            "TTL must ceil 1500ms -> 2s"
        );

        // EXPIRE on a missing key returns 0; on an existing key returns 1.
        assert_eq!(
            run(&mut state, vec!["EXPIRE", "nope", "100"]),
            RespValue::Integer(0),
            "EXPIRE on a missing key must return 0"
        );
        assert_eq!(run(&mut state, vec!["EXPIRE", "k", "100"]), RespValue::Integer(1));

        // PERSIST removes the timeout without touching the value: 1 when a timeout came off,
        // 0 for a key with none, 0 for a missing key -- and TTL reads -1 afterwards.
        assert_eq!(run(&mut state, vec!["PERSIST", "k"]), RespValue::Integer(1));
        assert_eq!(run(&mut state, vec!["TTL", "k"]), RespValue::Integer(-1));
        assert_eq!(
            run(&mut state, vec!["GET", "k"]),
            RespValue::Bulk(Some(b"v".to_vec())),
            "PERSIST must not disturb the value"
        );
        assert_eq!(
            run(&mut state, vec!["PERSIST", "k"]),
            RespValue::Integer(0),
            "a key with no timeout answers 0"
        );
        assert_eq!(
            run(&mut state, vec!["PERSIST", "missing"]),
            RespValue::Integer(0),
            "a missing key answers 0"
        );
    }

    /// Lists behave like the native ones: push answers the growing length, order is
    /// head-to-tail with LPUSH walking the head, pops drain the chosen end, negative LRANGE
    /// indices count from the tail, and TYPE says "list".
    #[test]
    fn list_commands_match_native() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut state = RedisCommandState::default();
        let mut run = |state: &mut RedisCommandState, args: Vec<&str>| {
            execute_redis_command_with_state(
                args.into_iter().map(|arg| arg.as_bytes().to_vec()).collect(),
                1,
                state,
                &mut |command| {
                    let response = engine.execute(crate::ExecuteRequest {
                        shard_id: 1,
                        command,
                    });
                    if response.status.ok {
                        Ok(response.response)
                    } else {
                        Err(response.status.message)
                    }
                },
            )
        };

        assert_eq!(run(&mut state, vec!["RPUSH", "l", "b"]), RespValue::Integer(1));
        assert_eq!(run(&mut state, vec!["RPUSH", "l", "c", "d"]), RespValue::Integer(3));
        assert_eq!(run(&mut state, vec!["LPUSH", "l", "a"]), RespValue::Integer(4));
        assert_eq!(run(&mut state, vec!["LLEN", "l"]), RespValue::Integer(4));
        assert_eq!(run(&mut state, vec!["TYPE", "l"]), RespValue::SimpleString("list".to_string()));

        let full = RespValue::Array(vec![
            RespValue::Bulk(Some(b"a".to_vec())),
            RespValue::Bulk(Some(b"b".to_vec())),
            RespValue::Bulk(Some(b"c".to_vec())),
            RespValue::Bulk(Some(b"d".to_vec())),
        ]);
        assert_eq!(run(&mut state, vec!["LRANGE", "l", "0", "-1"]), full);
        assert_eq!(
            run(&mut state, vec!["LRANGE", "l", "-2", "-1"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"c".to_vec())),
                RespValue::Bulk(Some(b"d".to_vec())),
            ]),
            "negative indices count from the tail"
        );
        assert_eq!(
            run(&mut state, vec!["LRANGE", "l", "5", "9"]),
            RespValue::Array(Vec::new()),
            "an out-of-window range answers empty, never an error"
        );

        assert_eq!(run(&mut state, vec!["LPOP", "l"]), RespValue::Bulk(Some(b"a".to_vec())));
        assert_eq!(run(&mut state, vec!["RPOP", "l"]), RespValue::Bulk(Some(b"d".to_vec())));
        assert_eq!(
            run(&mut state, vec!["LPOP", "l", "5"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"b".to_vec())),
                RespValue::Bulk(Some(b"c".to_vec())),
            ]),
            "a COUNT larger than the list drains it and answers what there was"
        );
        assert_eq!(run(&mut state, vec!["LPOP", "l"]), RespValue::Bulk(None));
        assert_eq!(run(&mut state, vec!["LLEN", "l"]), RespValue::Integer(0));
        assert_eq!(run(&mut state, vec!["LLEN", "missing"]), RespValue::Integer(0));

        // Interleaved pushes after a drain keep working -- the sequence space is not consumed.
        assert_eq!(run(&mut state, vec!["LPUSH", "l", "z"]), RespValue::Integer(1));
        assert_eq!(run(&mut state, vec!["LPOP", "l"]), RespValue::Bulk(Some(b"z".to_vec())));
    }

    /// Sorted sets behave like the native ones: ZADD answers only NEW members, a re-score
    /// moves ordering without growing the set, ranges walk (score, member) order with
    /// negative indices and WITHSCORES, score windows honor -inf/+inf and the exclusive
    /// paren, and TYPE says zset.
    #[test]
    fn sorted_set_commands_match_native() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut state = RedisCommandState::default();
        let mut run = |state: &mut RedisCommandState, args: Vec<&str>| {
            execute_redis_command_with_state(
                args.into_iter().map(|arg| arg.as_bytes().to_vec()).collect(),
                1,
                state,
                &mut |command| {
                    let response = engine.execute(crate::ExecuteRequest {
                        shard_id: 1,
                        command,
                    });
                    if response.status.ok {
                        Ok(response.response)
                    } else {
                        Err(response.status.message)
                    }
                },
            )
        };
        let bulk = |text: &str| RespValue::Bulk(Some(text.as_bytes().to_vec()));

        assert_eq!(run(&mut state, vec!["ZADD", "z", "2", "b", "1", "a"]), RespValue::Integer(2));
        assert_eq!(run(&mut state, vec!["ZADD", "z", "3", "c", "5", "a"]), RespValue::Integer(1),
            "re-scoring a is not an add");
        assert_eq!(run(&mut state, vec!["ZCARD", "z"]), RespValue::Integer(3));
        assert_eq!(run(&mut state, vec!["ZSCORE", "z", "a"]), bulk("5"));
        assert_eq!(run(&mut state, vec!["ZSCORE", "z", "missing"]), RespValue::Bulk(None));
        assert_eq!(run(&mut state, vec!["TYPE", "z"]), RespValue::SimpleString("zset".to_string()));

        assert_eq!(
            run(&mut state, vec!["ZRANGE", "z", "0", "-1"]),
            RespValue::Array(vec![bulk("b"), bulk("c"), bulk("a")]),
            "order follows the moved score"
        );
        assert_eq!(
            run(&mut state, vec!["ZRANGE", "z", "0", "1", "WITHSCORES"]),
            RespValue::Array(vec![bulk("b"), bulk("2"), bulk("c"), bulk("3")]),
        );
        assert_eq!(
            run(&mut state, vec!["ZREVRANGE", "z", "0", "0"]),
            RespValue::Array(vec![bulk("a")]),
        );
        assert_eq!(
            run(&mut state, vec!["ZRANGEBYSCORE", "z", "-inf", "+inf"]),
            RespValue::Array(vec![bulk("b"), bulk("c"), bulk("a")]),
        );
        assert_eq!(
            run(&mut state, vec!["ZRANGEBYSCORE", "z", "(2", "5"]),
            RespValue::Array(vec![bulk("c"), bulk("a")]),
            "the paren excludes the bound"
        );
        assert_eq!(
            run(&mut state, vec!["ZREVRANGEBYSCORE", "z", "+inf", "3", "WITHSCORES"]),
            RespValue::Array(vec![bulk("a"), bulk("5"), bulk("c"), bulk("3")]),
            "rev-by-score takes (max, min) and answers descending"
        );

        assert_eq!(run(&mut state, vec!["ZREM", "z", "b", "missing"]), RespValue::Integer(1));
        assert_eq!(run(&mut state, vec!["ZCARD", "z"]), RespValue::Integer(2));

        // Negative scores order below positives (the sign-flip bias at work).
        assert_eq!(run(&mut state, vec!["ZADD", "z", "-1.5", "n"]), RespValue::Integer(1));
        assert_eq!(
            run(&mut state, vec!["ZRANGE", "z", "0", "0", "WITHSCORES"]),
            RespValue::Array(vec![bulk("n"), bulk("-1.5")]),
        );
    }

    /// The rest of the sorted-set surface: ZINCRBY creates-then-moves atomically, ZCOUNT
    /// honors the window syntax, ZPOPMIN/ZPOPMAX drain in order with scores attached, and
    /// ZRANK/ZREVRANK answer positions or nil.
    #[test]
    fn sorted_set_completion_commands_match_native() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut state = RedisCommandState::default();
        let mut run = |state: &mut RedisCommandState, args: Vec<&str>| {
            execute_redis_command_with_state(
                args.into_iter().map(|arg| arg.as_bytes().to_vec()).collect(),
                1,
                state,
                &mut |command| {
                    let response = engine.execute(crate::ExecuteRequest {
                        shard_id: 1,
                        command,
                    });
                    if response.status.ok {
                        Ok(response.response)
                    } else {
                        Err(response.status.message)
                    }
                },
            )
        };
        let bulk = |text: &str| RespValue::Bulk(Some(text.as_bytes().to_vec()));

        // ZINCRBY on a missing member starts from 0; on a present one it moves the score.
        assert_eq!(run(&mut state, vec!["ZINCRBY", "z", "3", "a"]), bulk("3"));
        assert_eq!(run(&mut state, vec!["ZINCRBY", "z", "-1.5", "a"]), bulk("1.5"));
        assert_eq!(run(&mut state, vec!["ZCARD", "z"]), RespValue::Integer(1));

        assert_eq!(run(&mut state, vec!["ZADD", "z", "5", "b", "7", "c"]), RespValue::Integer(2));
        assert_eq!(run(&mut state, vec!["ZCOUNT", "z", "-inf", "+inf"]), RespValue::Integer(3));
        assert_eq!(run(&mut state, vec!["ZCOUNT", "z", "(1.5", "5"]), RespValue::Integer(1),
            "the paren excludes a's exact score");

        assert_eq!(run(&mut state, vec!["ZRANK", "z", "a"]), RespValue::Integer(0));
        assert_eq!(run(&mut state, vec!["ZREVRANK", "z", "a"]), RespValue::Integer(2));
        assert_eq!(run(&mut state, vec!["ZRANK", "z", "missing"]), RespValue::Bulk(None));

        assert_eq!(
            run(&mut state, vec!["ZPOPMIN", "z"]),
            RespValue::Array(vec![bulk("a"), bulk("1.5")]),
        );
        assert_eq!(
            run(&mut state, vec!["ZPOPMAX", "z", "5"]),
            RespValue::Array(vec![bulk("c"), bulk("7"), bulk("b"), bulk("5")]),
            "a COUNT larger than the set drains it high-to-low"
        );
        assert_eq!(run(&mut state, vec!["ZCARD", "z"]), RespValue::Integer(0));
        assert_eq!(run(&mut state, vec!["ZPOPMIN", "z"]), RespValue::Array(Vec::new()));
    }

    /// The token-bucket verbs over RESP: TAKE admits until the bucket runs dry and then
    /// answers denied with a retry-after, PEEK answers the same shape without consuming.
    #[test]
    fn bucket_verbs_admit_then_deny_with_retry_after() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut state = RedisCommandState::default();
        let mut run = |state: &mut RedisCommandState, args: Vec<&str>| {
            execute_redis_command_with_state(
                args.into_iter().map(|arg| arg.as_bytes().to_vec()).collect(),
                1,
                state,
                &mut |command| {
                    let response = engine.execute(crate::ExecuteRequest {
                        shard_id: 1,
                        command,
                    });
                    if response.status.ok {
                        Ok(response.response)
                    } else {
                        Err(response.status.message)
                    }
                },
            )
        };
        let fields = |value: RespValue| -> Vec<String> {
            match value {
                RespValue::Array(items) => items
                    .into_iter()
                    .map(|item| match item {
                        RespValue::Bulk(Some(bytes)) => String::from_utf8(bytes).expect("utf8"),
                        other => panic!("unexpected item: {other:?}"),
                    })
                    .collect(),
                other => panic!("unexpected response: {other:?}"),
            }
        };

        // Capacity 2, no refill within the test: two takes admit, the third denies.
        let first = fields(run(&mut state, vec!["BUCKETTAKE", "q", "1", "2", "0.001"]));
        assert_eq!("1", first[0]);
        let second = fields(run(&mut state, vec!["BUCKETTAKE", "q", "1", "2", "0.001"]));
        assert_eq!("1", second[0]);
        let third = fields(run(&mut state, vec!["BUCKETTAKE", "q", "1", "2", "0.001"]));
        assert_eq!("0", third[0], "the drained bucket must deny");
        assert!(third[2].parse::<u64>().expect("retry ms") > 0, "denied answers a retry-after");

        // PEEK reports without consuming. The retry-after may drift by the milliseconds the
        // wall clock moved between calls, so equality holds for the outcome and the level,
        // and the retry-after only within a tolerance.
        let peek_one = fields(run(&mut state, vec!["BUCKETPEEK", "q", "1", "2", "0.001"]));
        let peek_two = fields(run(&mut state, vec!["BUCKETPEEK", "q", "1", "2", "0.001"]));
        assert_eq!(peek_one[0], peek_two[0], "peek must not change the outcome");
        assert_eq!(peek_one[1], peek_two[1], "peek must not consume");
        let retry_one = peek_one[2].parse::<i64>().expect("retry ms");
        let retry_two = peek_two[2].parse::<i64>().expect("retry ms");
        assert!((retry_one - retry_two).abs() <= 50, "retry-after moved {retry_one} -> {retry_two}");
    }

    /// The windowed seen-set: first sight answers 0 and marks, a repeat inside the window
    /// answers 1 without re-marking, the window expires entries, SEENCARD counts, and DEL
    /// clears the whole set.
    #[test]
    fn seen_set_marks_dedupes_expires_and_clears() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut state = RedisCommandState::default();
        let mut run = |state: &mut RedisCommandState, args: Vec<&str>| {
            execute_redis_command_with_state(
                args.into_iter().map(|arg| arg.as_bytes().to_vec()).collect(),
                1,
                state,
                &mut |command| {
                    let response = engine.execute(crate::ExecuteRequest {
                        shard_id: 1,
                        command,
                    });
                    if response.status.ok {
                        Ok(response.response)
                    } else {
                        Err(response.status.message)
                    }
                },
            )
        };

        assert_eq!(run(&mut state, vec!["SEENCHECK", "idem", "a", "60000"]), RespValue::Integer(0),
            "first sight is not a duplicate");
        assert_eq!(run(&mut state, vec!["SEENCHECK", "idem", "a", "60000"]), RespValue::Integer(1),
            "a repeat inside the window is");
        assert_eq!(run(&mut state, vec!["SEENCHECK", "idem", "b", "60000"]), RespValue::Integer(0));
        assert_eq!(run(&mut state, vec!["SEENCARD", "idem"]), RespValue::Integer(2));

        // A 1ms window expires across a 25ms sleep: the member reads as new again, and the
        // bounded front-sweep has removed the stale entries from the count.
        assert_eq!(run(&mut state, vec!["SEENCHECK", "gone", "x", "1"]), RespValue::Integer(0));
        std::thread::sleep(std::time::Duration::from_millis(25));
        assert_eq!(run(&mut state, vec!["SEENCHECK", "gone", "x", "1"]), RespValue::Integer(0),
            "an expired member is new again");
        assert_eq!(run(&mut state, vec!["SEENCARD", "gone"]), RespValue::Integer(1),
            "the sweep dropped the expired entry, keeping only the fresh mark");

        assert_eq!(run(&mut state, vec!["DEL", "idem"]), RespValue::Integer(1));
        assert_eq!(run(&mut state, vec!["SEENCARD", "idem"]), RespValue::Integer(0));
        assert_eq!(run(&mut state, vec!["SEENCHECK", "idem", "a", "60000"]), RespValue::Integer(0),
            "a cleared set forgets");
    }

    #[test]
    fn set_zero_expiry_match_native() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut state = RedisCommandState::default();
        let run = |state: &mut RedisCommandState, args: Vec<&str>| {
            execute_redis_command_with_state(
                args.into_iter().map(|arg| arg.as_bytes().to_vec()).collect(),
                1,
                state,
                |command| {
                    let response = engine.execute(ExecuteRequest {
                        shard_id: 1,
                        command,
                    });
                    if response.status.ok {
                        Ok(response.response)
                    } else {
                        Err(response.status.message)
                    }
                },
            )
        };

        // Non-positive expiry is rejected on SETEX / PSETEX / SET EX (would otherwise write
        // an already-expired key). Matches real Redis.
        assert!(matches!(
            run(&mut state, vec!["SETEX", "a", "0", "v"]),
            RespValue::Error(_)
        ));
        assert!(matches!(
            run(&mut state, vec!["PSETEX", "b", "0", "v"]),
            RespValue::Error(_)
        ));
        assert!(matches!(
            run(&mut state, vec!["SET", "c", "v", "EX", "0"]),
            RespValue::Error(_)
        ));
        assert_eq!(run(&mut state, vec!["GET", "a"]), RespValue::Bulk(None));
        // A positive expiry still works.
        assert_eq!(
            run(&mut state, vec!["SETEX", "d", "100", "v"]),
            RespValue::SimpleString("OK".to_string())
        );
    }

    /// `GETEX key PERSIST` must make the key permanent.
    ///
    /// NON-VACUITY. Every half is asserted SEPARATELY, and each half states its denominator
    /// first: the deadline was really set (PTTL is a positive number, not -1 and not -2), the
    /// key really exists at the moment PERSIST is asked for, and only then is the removal of
    /// the deadline claimed. A single combined assertion would pass with the key already gone.
    ///
    /// WHY IT MATTERS. `PERSIST` is the caller saying "keep this forever". A spelling that
    /// accepts the request and leaves the deadline armed loses the value later, silently, with
    /// no error anywhere -- the worst shape a storage defect can take.
    #[test]
    fn getex_persist_makes_the_key_permanent() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut state = RedisCommandState::default();
        let run = |state: &mut RedisCommandState, args: Vec<&str>| {
            execute_redis_command_with_state(
                args.into_iter().map(|arg| arg.as_bytes().to_vec()).collect(),
                1,
                state,
                |command| {
                    let response = engine.execute(ExecuteRequest {
                        shard_id: 1,
                        command,
                    });
                    if response.status.ok {
                        Ok(response.response)
                    } else {
                        Err(response.status.message)
                    }
                },
            )
        };
        let pttl = |state: &mut RedisCommandState, key: &str| match run(state, vec!["PTTL", key]) {
            RespValue::Integer(value) => value,
            other => panic!("PTTL {key} answered {other:?}"),
        };

        // DENOMINATOR 1: the key exists and carries a real deadline. Both halves separately.
        assert_eq!(
            run(&mut state, vec!["SETEX", "persist:key", "600", "v"]),
            RespValue::SimpleString("OK".to_string()),
        );
        assert_eq!(
            run(&mut state, vec!["GET", "persist:key"]),
            RespValue::Bulk(Some(b"v".to_vec())),
            "the key must exist before PERSIST is asked for anything",
        );
        let armed = pttl(&mut state, "persist:key");
        assert!(
            armed > 0,
            "the deadline must be ARMED before this test can say anything about removing it, \
             PTTL read {armed} (-1 is no deadline, -2 is no key)",
        );

        // THE CLAIM: GETEX ... PERSIST returns the value AND disarms the deadline.
        assert_eq!(
            run(&mut state, vec!["GETEX", "persist:key", "PERSIST"]),
            RespValue::Bulk(Some(b"v".to_vec())),
            "GETEX still answers with the value",
        );
        assert_eq!(
            pttl(&mut state, "persist:key"),
            -1,
            "after PERSIST the key must exist with NO deadline (-1), not still be counting down",
        );

        // POSITIVE CONTROL: the same spelling with a duration still ARMS a deadline, so the
        // assertion above cannot be passing because GETEX stopped doing anything at all.
        assert_eq!(
            run(&mut state, vec!["GETEX", "persist:key", "EX", "600"]),
            RespValue::Bulk(Some(b"v".to_vec())),
        );
        let rearmed = pttl(&mut state, "persist:key");
        assert!(
            rearmed > 0,
            "GETEX EX must still arm a deadline, PTTL read {rearmed}",
        );

        // And PERSIST on a key that has no deadline is a no-op, not an error or a deletion.
        assert_eq!(
            run(&mut state, vec!["SET", "persist:plain", "v"]),
            RespValue::SimpleString("OK".to_string()),
        );
        assert_eq!(pttl(&mut state, "persist:plain"), -1);
        assert_eq!(
            run(&mut state, vec!["GETEX", "persist:plain", "PERSIST"]),
            RespValue::Bulk(Some(b"v".to_vec())),
        );
        assert_eq!(pttl(&mut state, "persist:plain"), -1);
        assert_eq!(
            run(&mut state, vec!["GET", "persist:plain"]),
            RespValue::Bulk(Some(b"v".to_vec())),
            "a no-op PERSIST must not remove the key",
        );
    }

    #[test]
    fn resp_parser_reads_array_command() {
        let mut input = BufReader::new(&b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"[..]);
        assert_eq!(
            read_command(&mut input).unwrap(),
            Some(vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()])
        );
    }

    #[test]
    // shared-corpus: redis_engine_product_command_flow;
    fn redis_core_api_extensions_use_engine_and_state() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut state = RedisCommandState::default();
        let run = |state: &mut RedisCommandState, args: Vec<&str>| {
            execute_redis_command_with_state(
                args.into_iter()
                    .map(|arg| arg.as_bytes().to_vec())
                    .collect(),
                1,
                state,
                |command| {
                    let response = engine.execute(ExecuteRequest {
                        shard_id: 1,
                        command,
                    });
                    if response.status.ok {
                        Ok(response.response)
                    } else {
                        Err(response.status.message)
                    }
                },
            )
        };

        assert_eq!(run(&mut state, vec!["DBSIZE"]), RespValue::Integer(0));
        assert_eq!(
            run(&mut state, vec!["SET", "redis:core:string", "value"]),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            run(&mut state, vec!["GETRANGE", "redis:core:string", "0", "4"]),
            RespValue::Bulk(Some(b"value".to_vec()))
        );
        assert_eq!(
            run(
                &mut state,
                vec!["SETRANGE", "redis:core:string", "5", "-tail"]
            ),
            RespValue::Integer(10)
        );
        assert_eq!(
            run(&mut state, vec!["GETRANGE", "redis:core:string", "0", "-1"]),
            RespValue::Bulk(Some(b"value-tail".to_vec()))
        );
        assert_eq!(
            run(
                &mut state,
                vec!["GETEX", "redis:core:string", "PX", "60000"]
            ),
            RespValue::Bulk(Some(b"value-tail".to_vec()))
        );
        assert_eq!(
            run(&mut state, vec!["INCRBYFLOAT", "redis:core:float", "1.5"]),
            RespValue::Bulk(Some(b"1.5".to_vec()))
        );
        assert_eq!(
            run(&mut state, vec!["TYPE", "redis:core:string"]),
            RespValue::SimpleString("string".to_string())
        );
        assert_eq!(
            run(&mut state, vec!["HSETNX", "redis:core:hash", "field", "v1"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(&mut state, vec!["HSETNX", "redis:core:hash", "field", "v2"]),
            RespValue::Integer(0)
        );
        assert_eq!(
            run(&mut state, vec!["HEXISTS", "redis:core:hash", "field"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(&mut state, vec!["HEXISTS", "redis:core:hash", "missing"]),
            RespValue::Integer(0)
        );
        assert_eq!(
            run(&mut state, vec!["HKEYS", "redis:core:hash"]),
            RespValue::Array(vec![RespValue::Bulk(Some(b"field".to_vec()))])
        );
        assert_eq!(
            run(&mut state, vec!["HVALS", "redis:core:hash"]),
            RespValue::Array(vec![RespValue::Bulk(Some(b"v1".to_vec()))])
        );
        assert_eq!(
            run(&mut state, vec!["HSTRLEN", "redis:core:hash", "field"]),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(
                &mut state,
                vec!["HINCRBYFLOAT", "redis:core:hash", "float", "1.5"]
            ),
            RespValue::Bulk(Some(b"1.5".to_vec()))
        );
        assert_eq!(
            run(&mut state, vec!["HSCAN", "redis:core:hash", "0"]),
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"0".to_vec())),
                RespValue::Array(vec![
                    RespValue::Bulk(Some(b"field".to_vec())),
                    RespValue::Bulk(Some(b"v1".to_vec())),
                    RespValue::Bulk(Some(b"float".to_vec())),
                    RespValue::Bulk(Some(b"1.5".to_vec())),
                ]),
            ])
        );
        assert_eq!(
            run(&mut state, vec!["TYPE", "redis:core:hash"]),
            RespValue::SimpleString("hash".to_string())
        );
        assert_eq!(
            run(&mut state, vec!["SADD", "redis:core:set", "a", "b"]),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(&mut state, vec!["SADD", "redis:core:other-set", "b", "c"]),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(
                &mut state,
                vec!["SDIFF", "redis:core:set", "redis:core:other-set"]

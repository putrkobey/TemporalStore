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

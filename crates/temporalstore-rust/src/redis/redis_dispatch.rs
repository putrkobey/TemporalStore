// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Redis command dispatcher (execute_redis_command_with_state), extracted from redis.rs.

use super::*;
use crate::types::{Command, CommandResponse, ControlStateFamily, StringSetCondition};

pub fn execute_redis_command_with_state(
    args: Vec<Vec<u8>>,
    shard_id: ShardId,
    state: &mut RedisCommandState,
    mut execute: impl FnMut(Command) -> Result<CommandResponse, String>,
) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR empty command".to_string());
    }
    let command = upper(&args[0]);
    match command.as_str() {
        "AUTH" if args.len() == 2 => {
            let configured = state
                .config
                .get("requirepass")
                .map(String::as_str)
                .unwrap_or_default();
            if configured.is_empty() || configured.as_bytes() == args[1].as_slice() {
                state.authenticated = true;
                RespValue::SimpleString("OK".to_string())
            } else {
                RespValue::Error("ERR invalid password".to_string())
            }
        }
        "PING" => RespValue::SimpleString(
            args.get(1)
                .map(|value| String::from_utf8_lossy(value).to_string())
                .unwrap_or_else(|| "PONG".to_string()),
        ),
        "ECHO" if args.len() == 2 => RespValue::Bulk(Some(args[1].clone())),
        "SELECT" if args.len() == 2 => match parse_u64(&args[1], "db") {
            Ok(0) => RespValue::SimpleString("OK".to_string()),
            Ok(_) => RespValue::Error("ERR DB index is out of range".to_string()),
            Err(err) => RespValue::Error(err),
        },
        "BGSAVE" if args.len() == 1 || args.len() == 2 => {
            RespValue::SimpleString("Background saving started".to_string())
        }
        "COMMAND" => redis_command_response(&args),
        "CONFIG" if args.len() >= 2 => redis_config_response(&args, state),
        "SLAVEOF" if args.len() == 3 => {
            let host = string_arg(&args[1]);
            let port = string_arg(&args[2]);
            if host.eq_ignore_ascii_case("no") && port.eq_ignore_ascii_case("one") {
                state.master = None;
            } else {
                state.master = Some((host, port));
            }
            RespValue::SimpleString("OK".to_string())
        }
        "INFO" if args.len() == 1 || args.len() == 2 => {
            let section = args
                .get(1)
                .map(|value| string_arg(value))
                .unwrap_or_else(|| "default".to_string());
            RespValue::Bulk(Some(
                redis_info(section.as_str(), shard_id, state).into_bytes(),
            ))
        }
        "PARTITION" if args.len() >= 2 => redis_partition_response(&args, shard_id, state),
        "PSLOTHASHKEY" if args.len() == 2 => {
            RespValue::Integer(bucket_id_for_key(String::from_utf8_lossy(&args[1]).as_ref()) as i64)
        }
        "PCLUSTERKEYSLOT" if args.len() == 2 => {
            RespValue::Integer(bucket_id_for_key(String::from_utf8_lossy(&args[1]).as_ref()) as i64)
        }
        "PCLUSTERHASH" if args.len() == 2 => {
            RespValue::Integer(stable_key_hash(String::from_utf8_lossy(&args[1]).as_ref()) as i64)
        }
        // Not `state.keyspace.len()`: the mirror holds names, and a name whose deadline has
        // passed is not a key. See `live_matching_keys`.
        "DBSIZE" if args.len() == 1 => {
            RespValue::Integer(live_matching_keys("*", state, &mut execute).len() as i64)
        }
        "TYPE" if args.len() == 2 => redis_type_response(&args[1], &mut execute),
        "GET" if args.len() == 2 => bytes_response(execute(Command::StringGet {
            key: string_arg(&args[1]),
        })),
        "MGET" if args.len() >= 2 => {
            let mut values = Vec::new();
            for key in args.iter().skip(1) {
                match execute(Command::StringGet {
                    key: string_arg(key),
                }) {
                    Ok(CommandResponse::Bytes { value }) => values.push(RespValue::Bulk(value)),
                    Ok(_) => return RespValue::Error("ERR invalid mget response".to_string()),
                    Err(err) => return RespValue::Error(format!("ERR {err}")),
                }
            }
            RespValue::Array(values)
        }
        "GETDEL" if args.len() == 2 => {
            let key = string_arg(&args[1]);
            match execute(Command::StringGet { key: key.clone() }) {
                Ok(CommandResponse::Bytes { value }) => {
                    if value.is_some() {
                        if let Err(err) = execute(Command::CommonDelete { key }) {
                            return RespValue::Error(format!("ERR {err}"));
                        }
                        state.keyspace.remove(&string_arg(&args[1]));
                    }
                    RespValue::Bulk(value)
                }
                Ok(_) => RespValue::Error("ERR invalid getdel response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        // GETSET IS `SET key value GET`, AND CARRIES SET SEMANTICS WITH IT. The caller hands
        // over the whole new value, so the deadline that belonged to the value being replaced
        // goes with it. `StringSet` never touches the expiry index -- it drops an already-
        // lapsed record and then writes the page -- so it left the OLD countdown running under
        // the NEW value, and the key vanished at a time the caller had overwritten away. The
        // conditional write is the one that clears the deadline, and it carries the old value
        // back in the same command, so the separate `StringGet` that used to read it (and
        // could observe a different record than the one being replaced) is gone too.
        "GETSET" if args.len() == 3 => {
            let key = string_arg(&args[1]);
            match execute(Command::StringSetConditional {
                key: key.clone(),
                value: args[2].clone(),
                ttl_ms: None,
                condition: StringSetCondition::Always,
                return_old: true,
                keep_ttl: false,
            }) {
                Ok(CommandResponse::Bytes { value }) => {
                    state.keyspace.insert(key);
                    RespValue::Bulk(value)
                }
                Ok(_) => RespValue::Error("ERR invalid getset response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "SET" if args.len() >= 3 => {
            let key = string_arg(&args[1]);
            let value = args[2].clone();
            let options = match parse_set_options(&args[3..]) {
                Ok(options) => options,
                Err(err) => return RespValue::Error(err),
            };
            // NOTE: the redis bridge rejects TTL+NX/XX ("not supported ... yet"), but that
            // is a stopgap limitation, not a semantic -- real Redis supports SET k v NX EX 10
            // and Rust deliberately does too (see the redis_string_hash_set_and_feature test).
            // We keep the more-capable Rust behavior rather than match a shim's TODO.
            match execute(Command::StringSetConditional {
                key: key.clone(),
                value,
                ttl_ms: options.ttl_ms,
                condition: options.condition,
                return_old: options.return_old,
                keep_ttl: options.keep_ttl,
            }) {
                Ok(CommandResponse::Bytes { value }) => {
                    if options.condition == StringSetCondition::Always || value.is_some() {
                        state.keyspace.insert(key);
                    }
                    RespValue::Bulk(value)
                }
                Ok(CommandResponse::Integer { value: 1 }) => {
                    state.keyspace.insert(key);
                    RespValue::SimpleString("OK".to_string())
                }
                Ok(CommandResponse::Integer { value: 0 }) => RespValue::Bulk(None),
                Ok(_) => RespValue::Error("ERR invalid set response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "SETNX" if args.len() == 3 => match execute(Command::StringSetConditional {
            key: string_arg(&args[1]),
            value: args[2].clone(),
            ttl_ms: None,
            condition: StringSetCondition::IfNotExists,
            return_old: false,
            keep_ttl: false,
        }) {
            Ok(CommandResponse::Integer { value }) => {
                if value > 0 {
                    state.keyspace.insert(string_arg(&args[1]));
                }
                RespValue::Integer(value)
            }
            Ok(CommandResponse::Bytes { value: None }) => RespValue::Integer(0),
            Ok(_) => RespValue::Error("ERR invalid setnx response".to_string()),
            Err(err) => RespValue::Error(format!("ERR {err}")),
        },
        // MSET IS A BATCH OF SET, SO EACH PAIR CLEARS ITS OWN DEADLINE. Same rule, same
        // reason as GETSET above: the caller supplies the whole value for each key.
        "MSET" if args.len() >= 3 && args.len() % 2 == 1 => {
            for pair in args[1..].chunks(2) {
                if let Err(err) = execute(Command::StringSetConditional {
                    key: string_arg(&pair[0]),
                    value: pair[1].clone(),
                    ttl_ms: None,
                    condition: StringSetCondition::Always,
                    return_old: false,
                    keep_ttl: false,
                }) {
                    return RespValue::Error(format!("ERR {err}"));
                }
                state.keyspace.insert(string_arg(&pair[0]));
            }
            RespValue::SimpleString("OK".to_string())
        }
        "MSETNX" if args.len() >= 3 && args.len() % 2 == 1 => {
            for pair in args[1..].chunks(2) {
                match execute(Command::CommonExists {
                    key: string_arg(&pair[0]),
                }) {
                    Ok(CommandResponse::Integer { value }) if value > 0 => {
                        return RespValue::Integer(0);
                    }
                    Ok(CommandResponse::Integer { .. }) => {}
                    Ok(_) => return RespValue::Error("ERR invalid exists response".to_string()),
                    Err(err) => return RespValue::Error(format!("ERR {err}")),
                }
            }
            // The same write as MSET, though MSETNX can never observe the difference: it
            // only reaches here when NONE of its keys exist, and a key that does not exist
            // has no deadline to discard. It is spelled the same so a reader does not have
            // to re-derive that, and so the rule has one spelling rather than two.
            for pair in args[1..].chunks(2) {
                if let Err(err) = execute(Command::StringSetConditional {
                    key: string_arg(&pair[0]),
                    value: pair[1].clone(),
                    ttl_ms: None,
                    condition: StringSetCondition::Always,
                    return_old: false,
                    keep_ttl: false,
                }) {
                    return RespValue::Error(format!("ERR {err}"));
                }
                state.keyspace.insert(string_arg(&pair[0]));
            }
            RespValue::Integer(1)
        }
        "GETEX" if args.len() >= 2 => {
            let key = string_arg(&args[1]);
            match execute(Command::StringGet { key: key.clone() }) {
                Ok(CommandResponse::Bytes { value }) => {
                    if value.is_some() {
                        let deadline = match parse_getex_ttl_ms(&args[2..]) {
                            Ok(value) => value,
                            Err(err) => return RespValue::Error(err),
                        };
                        // PERSIST is its own outcome, and it is the one this used to lose:
                        // it shared a `None` with "no option words" and so cleared nothing.
                        match deadline {
                            GetExDeadline::Unchanged => {}
                            GetExDeadline::Persist => {
                                if let Err(err) = execute(Command::CommonPersist { key }) {
                                    return RespValue::Error(format!("ERR {err}"));
                                }
                            }
                            GetExDeadline::Arm(ttl_ms) => {
                                if let Err(err) = execute(Command::CommonExpire { key, ttl_ms }) {
                                    return RespValue::Error(format!("ERR {err}"));
                                }
                            }
                            // The value was already read above and is still returned; what
                            // the caller asked for is a key that is gone by the time they
                            // get it, so it goes now rather than in a millisecond.
                            GetExDeadline::Discard => {
                                if let Err(err) = execute(Command::CommonDelete { key: key.clone() })
                                {
                                    return RespValue::Error(format!("ERR {err}"));
                                }
                                state.keyspace.remove(&key);
                            }
                        }
                    }
                    RespValue::Bulk(value)
                }
                Ok(_) => RespValue::Error("ERR invalid getex response".to_string()),
                Err(err) => RespValue::Error(format!("ERR {err}")),
            }
        }
        "SETEX" if args.len() == 4 => match parse_u64(&args[2], "seconds") {
            // rejects a non-positive expiry ("invalid expire time"); Rust otherwise
            // stored a key with deadline now+0 that vanishes on the next access.
            Ok(0) => RespValue::Error("ERR invalid expire time in setex".to_string()),
            Ok(seconds) => match execute(Command::StringSetEx {
                key: string_arg(&args[1]),
                value: args[3].clone(),
                ttl_ms: seconds.saturating_mul(1000),
            }) {
                Ok(_) => {
                    state.keyspace.insert(string_arg(&args[1]));
                    RespValue::SimpleString("OK".to_string())
                }
                Err(err) => RespValue::Error(format!("ERR {err}")),
            },
            Err(err) => RespValue::Error(err),
        },
        "PSETEX" if args.len() == 4 => match parse_u64(&args[2], "milliseconds") {
            Ok(0) => RespValue::Error("ERR invalid expire time in psetex".to_string()),
            Ok(ttl_ms) => match execute(Command::StringSetEx {
                key: string_arg(&args[1]),
                value: args[3].clone(),
                ttl_ms,
            }) {
                Ok(_) => {
                    state.keyspace.insert(string_arg(&args[1]));
                    RespValue::SimpleString("OK".to_string())
                }
                Err(err) => RespValue::Error(format!("ERR {err}")),
            },
            Err(err) => RespValue::Error(err),
        },
        "EXISTS" if args.len() >= 2 => {
            let mut count = 0;
            for key in args.iter().skip(1) {
                match execute(Command::CommonExists {
                    key: string_arg(key),
                }) {
                    Ok(CommandResponse::Integer { value }) => count += value,
                    Ok(_) => return RespValue::Error("ERR invalid exists response".to_string()),
                    Err(err) => return RespValue::Error(format!("ERR {err}")),
                }
            }
            RespValue::Integer(count)
        }
        "DEL" if args.len() >= 2 => {
            let mut removed = 0;
            for key_arg in args.iter().skip(1) {
                let key = string_arg(key_arg);
                match execute(Command::CommonExists { key: key.clone() }) {
                    Ok(CommandResponse::Integer { value }) => {
                        if value > 0 {
                            if let Err(err) = execute(Command::CommonDelete { key: key.clone() }) {
                                return RespValue::Error(format!("ERR {err}"));
                            }
                            state.keyspace.remove(&key);
                            removed += 1;
                        }
                    }
                    Ok(_) => return RespValue::Error("ERR invalid exists response".to_string()),
                    Err(err) => return RespValue::Error(format!("ERR {err}")),
                }
            }
            RespValue::Integer(removed)
        }
        "UNLINK" if args.len() >= 2 => {
            let mut removed = 0;
            for key_arg in args.iter().skip(1) {
                let key = string_arg(key_arg);
                match execute(Command::CommonExists { key: key.clone() }) {
                    Ok(CommandResponse::Integer { value }) if value > 0 => {
                        if let Err(err) = execute(Command::CommonDelete { key: key.clone() }) {
                            return RespValue::Error(format!("ERR {err}"));
                        }
                        state.keyspace.remove(&key);
                        removed += 1;
                    }
                    Ok(CommandResponse::Integer { .. }) => {}
                    Ok(_) => return RespValue::Error("ERR invalid exists response".to_string()),
                    Err(err) => return RespValue::Error(format!("ERR {err}")),
                }
            }
            RespValue::Integer(removed)
        }
        "TOUCH" if args.len() >= 2 => {
            let mut touched = 0;
            for key in args.iter().skip(1) {
                match execute(Command::CommonExists {
                    key: string_arg(key),
                }) {
                    Ok(CommandResponse::Integer { value }) => touched += value,

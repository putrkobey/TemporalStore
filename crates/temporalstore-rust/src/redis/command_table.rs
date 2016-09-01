// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Redis supported-command descriptor table, extracted from redis.rs.

use super::*;

pub(crate) fn redis_supported_commands() -> &'static [RedisCommandDescriptor] {
    const READ: &[&str] = &["readonly"];
    const WRITE: &[&str] = &["write"];
    const ADMIN: &[&str] = &["admin"];
    &[
        RedisCommandDescriptor {
            name: "APPEND",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "AUTH",
            arity: 2,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "BGSAVE",
            arity: -1,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "COMMAND",
            arity: -1,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "CONFIG",
            arity: -2,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "BUCKETPEEK",
            arity: 5,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "BUCKETTAKE",
            arity: 5,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "COPY",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "DBSIZE",
            arity: 1,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "DECR",
            arity: 2,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "DECRBY",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "DEL",
            arity: -2,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "ECHO",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "EXISTS",
            arity: -2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "EXPIRE",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "EXPIREAT",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "EXPIRETIME",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "FLUSHALL",
            arity: -1,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "FLUSHDB",
            arity: -1,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "GET",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "GETDEL",
            arity: 2,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "GETEX",
            arity: -2,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "GETSET",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "GETRANGE",
            arity: 4,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HDEL",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "HGET",
            arity: 3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HGETALL",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HEXISTS",
            arity: 3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HINCRBY",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "HINCRBYFLOAT",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "HKEYS",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HLEN",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HMGET",
            arity: -3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HMSET",
            arity: -4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "HSET",
            arity: -4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "HSCAN",
            arity: -3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HSETNX",
            arity: 4,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "HSTRLEN",
            arity: 3,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "HVALS",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "INFO",
            arity: -1,
            flags: ADMIN,
        },
        RedisCommandDescriptor {
            name: "INCR",
            arity: 2,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "INCRBY",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "INCRBYFLOAT",
            arity: 3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "LLEN",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "LPOP",
            arity: -2,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "LPUSH",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "LRANGE",
            arity: 4,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "MGET",
            arity: -2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "MSET",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "MSETNX",
            arity: -3,
            flags: WRITE,
        },
        RedisCommandDescriptor {
            name: "KEYS",
            arity: 2,
            flags: READ,
        },
        RedisCommandDescriptor {
            name: "PARTITION",

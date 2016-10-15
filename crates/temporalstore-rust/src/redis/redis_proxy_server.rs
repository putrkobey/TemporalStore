// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::io::{self, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::types::{ExecuteRequest, ShardId};
use crate::TemporalStoreClient;

use super::{execute_redis_command_with_state, read_command, RedisCommandState};

pub fn serve_redis_proxy(addr: &str, proxy_addr: String, shard_id: ShardId) -> io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    let client = Arc::new(TemporalStoreClient::new(proxy_addr));
    for stream in listener.incoming() {
        let stream = stream?;
        let client = Arc::clone(&client);
        thread::spawn(move || {
            let _ = handle_stream(stream, &client, shard_id);
        });
    }
    Ok(())
}

fn handle_stream(

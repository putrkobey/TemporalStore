// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Idle read timeout for a kept-alive server connection. A persistent client
/// connection parks here between requests; if nothing arrives for this long the
/// server reaps the connection (the client transparently reconnects).
const SERVER_KEEPALIVE_IDLE_MS: u64 = 120_000;
/// Max idle client sockets pooled per destination address, per thread.
const CLIENT_POOL_MAX_PER_ADDR: usize = 8;

use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum HttpError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("bad response: {0}")]
    BadResponse(String),
}

impl HttpError {
    /// Whether this error proves the request never reached the server.
    ///
    /// Only a refused connection proves it: the peer rejected the TCP handshake, so not one
    /// byte of the request was sent. Everything else -- a read timeout above all -- means
    /// the server stopped answering, which is not the same as never having heard the
    /// request. That distinction is what decides whether a write may be sent again.
    ///
    /// Deliberately conservative. A connect timeout is indistinguishable from a read
    /// timeout here (both surface as `TimedOut`), so it is treated as "unknown" and a write
    /// is not repeated after one.
    pub fn request_never_reached_the_server(&self) -> bool {
        matches!(self, HttpError::Io(err) if err.kind() == ErrorKind::ConnectionRefused)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpRequestOptions {
    pub connect_timeout_ms: u64,
    pub io_timeout_ms: u64,
    pub max_retries: usize,
}

impl Default for HttpRequestOptions {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 200,
            io_timeout_ms: 200,
            max_retries: 0,
        }
    }
}

#[derive(Debug)]
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub body: Vec<u8>,
}

/// Parsed HTTP request head (request line + headers), read off the socket
/// *before* the body. A streaming handler inspects this to decide whether to
/// take a request without the body ever being buffered.
#[derive(Debug, Clone)]
pub struct RequestHead {
    pub method: String,
    pub path: String,
    pub content_length: usize,
    pub keep_alive: bool,
    /// True when the request arrived carrying the blob peer-fetch loop-guard
    /// header (`X-Ts-Blob-Peer-Fetch: 0`). Such a request is itself a
    /// cross-peer blob fetch hop and MUST be served local-only — never
    /// re-forwarded to another peer — otherwise peers would fetch from each
    /// other forever. Parsed generically here so the streaming blob handler can
    /// enforce the guard without re-reading the socket headers.
    pub blob_peer_fetch_loop_guard: bool,
    /// The `Authorization: Bearer <token>` credential, when the request carried
    /// one. Parsed generically here so a serving layer that requires a token
    /// (the metaserver's admin surface) can enforce it at the head stage,
    /// before any body byte is read.
    pub bearer_token: Option<String>,
}

/// What a streaming handler did with a request.
pub enum StreamAction {
    /// Fully handled: the streaming handler consumed the entire request body and
    /// wrote the complete response itself. The connection then stays alive (or
    /// closes) per `RequestHead::keep_alive`.
    Handled,
    /// Declined WITHOUT reading any body byte or writing any response; the serve
    /// loop buffers the body and dispatches to the ordinary buffered handler.
    Declined,
}

/// Handle passed to a streaming handler. Reads the request body straight off the
/// socket in caller-sized chunks (no full-body `Vec` buffering) and writes the
/// response straight back to the socket. Body bytes that arrived in the same
/// read as the head are served first, transparently.
pub struct StreamTransfer<'a> {
    stream: &'a mut TcpStream,
    prebuffered: Vec<u8>,
    prebuffered_pos: usize,
    content_length: usize,
    consumed: usize,
    keep_alive: bool,
}

impl<'a> StreamTransfer<'a> {
    fn new(
        stream: &'a mut TcpStream,
        mut prebuffered: Vec<u8>,
        content_length: usize,
        keep_alive: bool,
    ) -> Self {
        // Never treat more than content_length bytes as this request's body
        // (matches the buffered path, which ignores any pipelined trailer).
        if prebuffered.len() > content_length {
            prebuffered.truncate(content_length);
        }
        Self {
            stream,
            prebuffered,
            prebuffered_pos: 0,
            content_length,
            consumed: 0,
            keep_alive,
        }
    }

    /// Declared request-body length in bytes.
    pub fn body_len(&self) -> usize {
        self.content_length
    }

    /// Whether the connection is to be kept alive after this request.
    pub fn keep_alive(&self) -> bool {
        self.keep_alive
    }

    /// Read the next slice of request body into `buf`. Returns 0 at end of body.
    /// Serves any bytes already read while parsing the head, then reads from the
    /// socket, so a giant upload never lands in a single buffer.
    pub fn read_body(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.prebuffered_pos < self.prebuffered.len() {
            let n = buf.len().min(self.prebuffered.len() - self.prebuffered_pos);
            buf[..n]
                .copy_from_slice(&self.prebuffered[self.prebuffered_pos..self.prebuffered_pos + n]);
            self.prebuffered_pos += n;
            self.consumed += n;
            return Ok(n);
        }
        if self.consumed >= self.content_length {
            return Ok(0);
        }
        let want = buf.len().min(self.content_length - self.consumed);
        loop {
            match self.stream.read(&mut buf[..want]) {
                Ok(n) => {
                    self.consumed += n;
                    return Ok(n);
                }
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            }
        }
    }

    /// Drain and discard any unread request body so a kept-alive connection stays
    /// framed after an early (pre-body) error response.
    pub fn drain_body(&mut self) -> std::io::Result<()> {
        let mut scratch = [0u8; 8192];
        loop {
            if self.read_body(&mut scratch)? == 0 {
                return Ok(());
            }
        }
    }

    /// Write the response status line + headers. Call once, before the body.
    pub fn send_head(
        &mut self,
        status: u16,
        content_type: &str,
        content_length: usize,
    ) -> std::io::Result<()> {
        let status_text = if status == 200 { "OK" } else { "ERROR" };
        let connection = if self.keep_alive { "keep-alive" } else { "close" };
        let header = format!(
            "HTTP/1.1 {status} {status_text}\r\nContent-Type: {content_type}\r\nContent-Length: {content_length}\r\nConnection: {connection}\r\n\r\n"
        );
        self.stream.write_all(header.as_bytes())
    }

    /// Write a slice of the response body straight to the socket.
    pub fn write_chunk(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.stream.write_all(buf)
    }

    /// Flush the socket after the response has been written.
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }

    /// Consume the transfer, returning the full request body (any prebuffered
    /// bytes + the rest read from the socket). Used by the buffered fallback when
    /// the streaming handler declines; taking `self` by value releases the socket
    /// borrow so the caller can write the buffered response.
    fn into_buffered_body(self) -> Result<Vec<u8>, HttpError> {
        let mut body = Vec::with_capacity(self.content_length);
        body.extend_from_slice(&self.prebuffered[self.prebuffered_pos..]);
        let mut chunk = [0u8; 8192];
        while body.len() < self.content_length {
            let want = chunk.len().min(self.content_length - body.len());
            match self.stream.read(&mut chunk[..want]) {
                Ok(0) => {
                    return Err(HttpError::BadResponse(
                        "incomplete request body".to_string(),
                    ))
                }
                Ok(n) => body.extend_from_slice(&chunk[..n]),
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err) => return Err(HttpError::Io(err)),
            }
        }
        Ok(body)
    }
}

pub fn serve(
    addr: &str,
    handler: impl Fn(HttpRequest) -> (u16, Vec<u8>) + Send + Sync + 'static,
) -> Result<(), HttpError> {
    // Plain serve: every request goes through the buffered handler (the streaming
    // pre-handler declines everything).
    serve_with_stream_handler(addr, |_head, _transfer| StreamAction::Declined, handler)
}

/// Like [`serve`], but a `stream_handler` gets first refusal on each request with
/// the socket exposed (via [`StreamTransfer`]) so it can read the body and write
/// the response incrementally — no full-body buffering. It returns
/// [`StreamAction::Declined`] (without touching the body) to fall through to the
/// buffered `handler`. Used by the datanode to stream `/blob/<key>` bodies
/// straight between the socket and the object store.
pub fn serve_with_stream_handler<S, H>(
    addr: &str,
    stream_handler: S,
    handler: H,
) -> Result<(), HttpError>
where
    S: Fn(&RequestHead, &mut StreamTransfer) -> StreamAction + Send + Sync + 'static,
    H: Fn(HttpRequest) -> (u16, Vec<u8>) + Send + Sync + 'static,
{
    serve_listener_with_stream_handler(TcpListener::bind(addr)?, stream_handler, handler)
}

/// Like [`serve`], but on a listener the caller has already bound.
///
/// Choosing a port by binding, reading the address back, dropping the listener
/// and handing the address on leaves the port owned by nobody until the server
/// binds it again. On a machine running more than one server process, something
/// else can take it inside that window, and the server then fails on `AddrInUse`
/// long after the address was decided -- far from where the address was chosen,
/// and usually reported as whatever the missing server broke downstream. Handing
/// the bound listener straight over closes the window: the port is never unowned.
pub fn serve_listener(
    listener: TcpListener,
    handler: impl Fn(HttpRequest) -> (u16, Vec<u8>) + Send + Sync + 'static,
) -> Result<(), HttpError> {
    serve_listener_with_stream_handler(
        listener,
        |_head, _transfer| StreamAction::Declined,
        handler,
    )
}

/// [`serve_with_stream_handler`] on an already-bound listener. See [`serve_listener`].
pub fn serve_listener_with_stream_handler<S, H>(
    listener: TcpListener,
    stream_handler: S,
    handler: H,
) -> Result<(), HttpError>
where
    S: Fn(&RequestHead, &mut StreamTransfer) -> StreamAction + Send + Sync + 'static,
    H: Fn(HttpRequest) -> (u16, Vec<u8>) + Send + Sync + 'static,
{
    let stream_handler = Arc::new(stream_handler);
    let handler = Arc::new(handler);
    for stream in listener.incoming() {
        let stream = stream?;
        // Disable Nagle's algorithm: request/response bodies are small and are
        // written in a couple of segments, so Nagle + delayed-ACK otherwise adds
        // a ~40ms stall per round-trip on loopback/LAN.
        let _ = stream.set_nodelay(true);
        let stream_handler = Arc::clone(&stream_handler);
        let handler = Arc::clone(&handler);
        thread::spawn(move || {
            let _ = handle_stream(stream, &*stream_handler, &*handler);
        });
    }
    Ok(())
}

pub fn json_response<T: Serialize>(status: u16, value: &T) -> (u16, Vec<u8>) {
    (
        status,
        serde_json::to_vec(value).unwrap_or_else(|_| b"{\"ok\":false}".to_vec()),
    )
}

pub fn parse_json<T: DeserializeOwned>(body: &[u8]) -> Result<T, HttpError> {
    Ok(serde_json::from_slice(body)?)
}

/// Like `parse_json`, but for a request type that BORROWS from the body.
///
/// The caller keeps the body alive for as long as the parsed value, so serde can point at it
/// instead of copying every string out of it.
pub fn parse_json_borrowed<'a, T: serde::Deserialize<'a>>(body: &'a [u8]) -> Result<T, HttpError> {
    Ok(serde_json::from_slice(body)?)
}

pub fn post_json<Req: Serialize, Res: DeserializeOwned>(
    addr: &str,
    path: &str,
    request: &Req,
) -> Result<Res, HttpError> {
    post_json_with_options(addr, path, request, HttpRequestOptions::default())
}

pub fn post_json_with_options<Req: Serialize, Res: DeserializeOwned>(
    addr: &str,
    path: &str,
    request: &Req,
    options: HttpRequestOptions,
) -> Result<Res, HttpError> {
    post_json_with_options_and_headers(addr, path, request, "", options)
}

pub fn post_json_with_options_and_headers<Req: Serialize, Res: DeserializeOwned>(
    addr: &str,
    path: &str,
    request: &Req,
    extra_headers: &str,
    options: HttpRequestOptions,
) -> Result<Res, HttpError> {
    let body = serde_json::to_vec(request)?;
    // Borrowed when there is nothing to append, which is every caller but meta sync. The
    // sibling of this line in `request_bytes_with_options` was given a `Cow` and this one was
    // not, so the two allocations `format!` costs stayed on the JSON POST path -- the path the
    // proxy forwards execute, batch and the context routes through.
    let headers: std::borrow::Cow<'static, str> = if extra_headers.is_empty() {
        std::borrow::Cow::Borrowed("Content-Type: application/json\r\n")
    } else {
        std::borrow::Cow::Owned(format!("Content-Type: application/json\r\n{extra_headers}"))
    };
    let raw = request_raw_with_options(addr, "POST", path, &body, &headers, options)?;
    Ok(serde_json::from_slice(&raw)?)
}

/// Send a raw request body (e.g. a large binary attachment) and return the raw
/// response body. Used by the blob/attachment path where bodies are not JSON.
pub fn request_bytes_with_options(
    addr: &str,
    method: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
    options: HttpRequestOptions,
) -> Result<Vec<u8>, HttpError> {
    // The header line is only borrowed below, and every caller in this tree sends JSON, so
    // the common case needs no allocation. `format!` here cost two on every request the proxy
    // and the client make -- execute, batch, the context routes, meta sync, the heartbeat.
    let headers: std::borrow::Cow<'static, str> = match content_type {
        "application/json" => std::borrow::Cow::Borrowed("Content-Type: application/json\r\n"),
        other => std::borrow::Cow::Owned(format!("Content-Type: {other}\r\n")),
    };
    request_raw_with_options(addr, method, path, body, &headers, options)
}

/// Send a GET (empty body) and return the raw response body, with caller-supplied
/// extra request headers (each header line must end in `\r\n`). Non-200 responses
/// surface as `HttpError::BadResponse(status_line)`. Used by the cross-peer blob
/// availability path, which sets the `X-Ts-Blob-Peer-Fetch: 0` loop-guard header so
/// the queried peer serves local-only and never re-forwards.
pub fn get_bytes_with_headers(
    addr: &str,
    path: &str,
    extra_headers: &str,
    options: HttpRequestOptions,
) -> Result<Vec<u8>, HttpError> {
    request_raw_with_options(addr, "GET", path, &[], extra_headers, options)
}

pub fn get_json<Res: DeserializeOwned>(addr: &str, path: &str) -> Result<Res, HttpError> {
    get_json_with_options(addr, path, HttpRequestOptions::default())
}

pub fn get_json_with_options<Res: DeserializeOwned>(
    addr: &str,
    path: &str,
    options: HttpRequestOptions,
) -> Result<Res, HttpError> {
    get_json_with_options_and_headers(addr, path, "", options)
}

pub fn get_json_with_options_and_headers<Res: DeserializeOwned>(
    addr: &str,
    path: &str,
    extra_headers: &str,
    options: HttpRequestOptions,
) -> Result<Res, HttpError> {
    let raw = request_raw_with_options(addr, "GET", path, &[], extra_headers, options)?;
    Ok(serde_json::from_slice(&raw)?)

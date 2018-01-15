// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Per-shard read and write rate limits.
//!
//! One token bucket per direction. A bucket is defined by a rate (tokens per second) and a burst
//! (how many tokens may accumulate while idle), and a rate of zero means the direction is not
//! limited at all -- which is the default, so a deployment that sets nothing behaves exactly as it
//! did before this existed.

use std::collections::HashMap;
use std::time::Instant;

use crate::types::ShardId;

/// Which direction a command consumes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuotaKind {
    Read,
    Write,
}

/// A token bucket that never waits: it either has the tokens or it does not.
///
/// Tokens are not counted. What is stored is a point in time that consumption has been charged up
/// to, so the tokens available at any moment are however much time has passed since it, capped at
/// the burst. Charging N tokens moves that point forward by N token-times. Nothing accumulates
/// while idle beyond the cap, and no timer has to run to refill anything.
#[derive(Debug)]
pub(crate) struct TokenBucket {
    rate: u64,
    burst: u64,
    per_token_ns: i128,
    per_burst_ns: i128,
    /// Consumption is charged up to here, as nanoseconds since `origin`. Starts one full burst
    /// BEHIND the origin, which is what makes a new bucket start full: the tokens available are
    /// however much time separates this point from now, and at the origin that is a whole burst.
    /// Signed for that reason -- an unsigned zero would mean a new bucket had nothing in it, and
    /// the first request against it would be refused.
    charged_ns: i128,
    origin: Instant,
}

impl TokenBucket {
    pub(crate) fn new(rate: u64, burst: u64) -> Self {
        // A burst of zero would mean nothing may ever accumulate, so a caller sending at exactly
        // the rate would still be refused on any jitter. One second of credit is the useful
        // default and matches what a rate alone implies.
        let burst = if burst == 0 { rate } else { burst };
        let per_token_ns: i128 = if rate == 0 {
            0
        } else {
            1_000_000_000i128 / rate as i128
        };
        let per_burst_ns = per_token_ns * burst as i128;
        Self {
            rate,
            burst,
            per_token_ns,
            per_burst_ns,
            charged_ns: -per_burst_ns,
            origin: Instant::now(),
        }
    }

    pub(crate) fn rate(&self) -> u64 {
        self.rate
    }

    pub(crate) fn burst(&self) -> u64 {
        self.burst
    }

    fn now_ns(&self) -> i128 {
        self.origin.elapsed().as_nanos() as i128
    }

    /// Take `tokens` if they are there. Returns false without waiting if they are not.
    pub(crate) fn try_consume(&mut self, tokens: u64) -> bool {
        self.try_consume_at(tokens, self.now_ns())
    }

    fn try_consume_at(&mut self, tokens: u64, now_ns: i128) -> bool {
        if self.rate == 0 {
            return true;
        }
        // Credit older than one burst is gone -- that is what the cap means.
        let floor = now_ns - self.per_burst_ns;
        let from = self.charged_ns.max(floor);
        let charged_to = from + tokens as i128 * self.per_token_ns;
        if charged_to > now_ns {
            return false;
        }
        self.charged_ns = charged_to;
        true
    }

    /// How many tokens are available right now. Diagnostics only.
    pub(crate) fn available(&self) -> u64 {
        if self.rate == 0 || self.per_token_ns == 0 {
            return u64::MAX;
        }
        let now_ns = self.now_ns();
        if now_ns <= self.charged_ns {
            return 0;
        }
        let idle = (now_ns - self.charged_ns).min(self.per_burst_ns);
        (idle / self.per_token_ns) as u64

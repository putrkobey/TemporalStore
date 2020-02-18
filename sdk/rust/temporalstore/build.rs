// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// The default build of this SDK is the proxy client: pure Rust speaking the
// TemporalStore proxy protocol over a socket, with nothing native to link.
//
// The optional `direct` feature instead binds the in-process C ABI of a native
// TemporalStore library. This repository does not build that library, so
// enabling `direct` means supplying it yourself and pointing this script at it

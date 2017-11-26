// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! TemporalStoreClient execute + route-resolution + backend-failure methods, split from client.rs.
use super::*;

impl TemporalStoreClient {
    pub fn execute(&self, request: ExecuteRequest) -> Result<ExecuteResponse, HttpError> {
        post_json(&self.inner.options.proxy_addr, "/execute", &request)
    }

    pub fn execute_with_options(
        &self,
        request: ExecuteRequest,
        options: RequestOptions,
    ) -> Result<ExecuteResponse, ClientError> {
        let _trace_id = options.trace_id;
        self.execute_routed(request, false).map_err(Into::into)
    }

    pub fn batch_execute(
        &self,
        request: BatchExecuteRequest,
    ) -> Result<BatchExecuteResponse, HttpError> {
        post_json(&self.inner.options.proxy_addr, "/batch_execute", &request)
    }

    pub fn batch_execute_with_options(
        &self,
        request: BatchExecuteRequest,
        options: RequestOptions,
    ) -> Result<BatchExecuteResponse, ClientError> {
        let _trace_id = options.trace_id;
        let http_options = self.inner.options.http_options();
        if self.inner.options.meta_addr.is_some() {
            let server_addr = self.resolve_route(request.shard_id, false, None)?;
            return post_json_with_options(&server_addr, "/batch_execute", &request, http_options)
                .or_else(|err| {
                    let became_continuous = self.record_backend_failure(
                        &server_addr,
                        self.inner.options.topo_error_retry_interval_ms,
                    );
                    self.inner
                        .stats
                        .lock()
                        .expect("client stats lock poisoned")
                        .record_backend_error(became_continuous);
                    if !self.inner.options.refresh_route_on_backend_error {
                        return Err(err.into());
                    }
                    if !Self::may_send_again(&request.commands, &err) {
                        self.inner
                            .stats
                            .lock()
                            .expect("client stats lock poisoned")
                            .record_write_of_unknown_outcome();
                        let _ = self.resolve_route(request.shard_id, true, None);
                        return Err(ClientError::WriteOutcomeUnknown(err.to_string()));
                    }
                    let refreshed = self.resolve_route(request.shard_id, true, None)?;
                    Ok(post_json_with_options(
                        &refreshed,
                        "/batch_execute",
                        &request,
                        http_options,
                    )?)
                });
        }
        Ok(post_json_with_options(
            &self.inner.options.proxy_addr,
            "/batch_execute",
            &request,
            http_options,
        )?)
    }

    pub fn get_shard(&self, shard_id: u64) -> Result<GetShardResponse, HttpError> {
        get_json(
            &self.inner.options.proxy_addr,
            &format!("/shards/{shard_id}"),
        )
    }

    pub fn refresh_route(&self, shard_id: ShardId) -> Result<String, ClientError> {
        self.resolve_route(shard_id, true, None)
    }

    pub(super) fn execute_routed(
        &self,
        request: ExecuteRequest,
        force_primary: bool,
    ) -> Result<ExecuteResponse, ClientError> {
        self.execute_routed_with_http(
            request,
            force_primary,
            self.inner.options.http_options(),
            None,
        )
    }

    pub(super) fn execute_routed_with_http(
        &self,
        request: ExecuteRequest,
        force_primary: bool,
        http_options: HttpRequestOptions,
        continuous_failed_time_ms: Option<u64>,
    ) -> Result<ExecuteResponse, ClientError> {
        self.execute_routed_with_http_and_policy(
            request,
            force_primary,
            http_options,
            continuous_failed_time_ms,
            ReplicaReadPolicy::PinPrimary,
            None,
        )
    }

    /// Whether a request that just failed may simply be sent again.
    ///
    /// A read always may -- repeating it cannot change anything. A write may only if the
    /// error proves it never arrived: a refused connection sent no bytes, so nothing was
    /// applied and re-sending is the recovery the caller wants. A read timeout proves
    /// nothing of the sort -- the datanode stopped answering, which is not the same as never
    /// having received the write -- and re-sending there counts `ControlStateIncrement`
    /// twice with nothing downstream able to tell.
    ///
    /// The command layer above already drew this line: it retries a write only when the
    /// backend REFUSED it (`safe_budget_free_write_retry` requires a topology retry), a
    /// definite "not applied". This layer had no such reasoning and re-sent regardless.
    fn may_send_again(commands: &[Command], err: &HttpError) -> bool {
        !commands.iter().any(super::commands::is_write) || err.request_never_reached_the_server()
    }

    /// The fencing token to send with a write bound for `server_addr`, or 0 for none.
    ///
    /// The token describes the PRIMARY's placement, so it only means anything when the write is
    /// actually going to the primary. Sending it to a replica would fence on a version that
    /// replica never claimed to hold, turning every replica-directed write into a mismatch.
    ///
    /// 0 whenever there is nothing to say -- no cached route, a route from a metaserver that
    /// sends no token, or an address that is not the primary -- and 0 keeps the unchecked path,
    /// so this can only ever ADD a check, never remove one.
    fn fencing_token_for(&self, shard_id: ShardId, server_addr: &str) -> u64 {
        self.inner
            .routes
            .read()
            .expect("client route cache lock poisoned")
            .get(&shard_id)
            .filter(|route| route.primary_addr == server_addr)
            .map(|route| route.load_version)
            .unwrap_or(0)
    }

    /// Send one write, fenced when a token is known.
    ///
    /// The checked path answers with a `CheckedExecuteResponse` whose inner response carries the
    /// same status, so unwrapping it here keeps the caller's type and keeps a rejection visible:
    /// `load_version_mismatch` is already classified retryable, and a retry re-resolves the route
    /// rather than repeating the write to a node that has been fenced out.
    fn post_execute(
        server_addr: &str,
        request: &ExecuteRequest,
        load_version: u64,
        http_options: HttpRequestOptions,
    ) -> Result<ExecuteResponse, HttpError> {
        if load_version == 0 {
            return post_json_with_options(server_addr, "/execute", request, http_options);
        }
        let checked = crate::control::CheckedExecuteRequest {
            shard_id: request.shard_id,
            load_version,
            command: request.command.clone(),
        };
        let response: crate::control::CheckedExecuteResponse =
            post_json_with_options(server_addr, "/execute_checked", &checked, http_options)?;
        Ok(response.response)
    }

    pub(super) fn execute_routed_with_http_and_policy(
        &self,
        request: ExecuteRequest,
        force_primary: bool,
        http_options: HttpRequestOptions,
        continuous_failed_time_ms: Option<u64>,
        replica_read_policy: ReplicaReadPolicy,
        preferred_location: Option<&str>,
    ) -> Result<ExecuteResponse, ClientError> {
        if self.inner.options.meta_addr.is_some() {
            let policy = if force_primary {
                ReplicaReadPolicy::PinPrimary
            } else {
                replica_read_policy
            };
            let server_addr = self.resolve_route_with_policy(
                request.shard_id,
                false,
                continuous_failed_time_ms,
                policy,
                preferred_location,
            )?;
            let load_version = self.fencing_token_for(request.shard_id, &server_addr);
            return Self::post_execute(&server_addr, &request, load_version, http_options)
                .or_else(|err| {
                    let became_continuous = self.record_backend_failure(
                        &server_addr,
                        continuous_failed_time_ms
                            .unwrap_or(self.inner.options.topo_error_retry_interval_ms),
                    );
                    self.inner
                        .stats
                        .lock()
                        .expect("client stats lock poisoned")
                        .record_backend_error(became_continuous);
                    if !self.inner.options.refresh_route_on_backend_error {
                        return Err(err.into());
                    }
                    if !Self::may_send_again(std::slice::from_ref(&request.command), &err) {
                        self.inner
                            .stats
                            .lock()
                            .expect("client stats lock poisoned")
                            .record_write_of_unknown_outcome();
                        // Drop the stale route so the NEXT request re-resolves, but do not
                        // send this write a second time -- its outcome is unknown, not known
                        // to be "never happened".
                        let _ = self.resolve_route_with_policy(
                            request.shard_id,
                            true,
                            continuous_failed_time_ms,
                            policy,
                            preferred_location,
                        );
                        return Err(ClientError::WriteOutcomeUnknown(err.to_string()));
                    }
                    let refreshed = self.resolve_route_with_policy(
                        request.shard_id,
                        true,
                        continuous_failed_time_ms,
                        policy,
                        preferred_location,
                    )?;
                    let refreshed_load_version =
                        self.fencing_token_for(request.shard_id, &refreshed);
                    let response = Self::post_execute(
                        &refreshed,
                        &request,
                        refreshed_load_version,
                        http_options,
                    )?;
                    self.record_backend_success(&refreshed);
                    self.inner
                        .stats
                        .lock()
                        .expect("client stats lock poisoned")
                        .record_backend_success();
                    Ok(response)
                });
        }

        let _ = force_primary;
        Ok(post_json_with_options(
            &self.inner.options.proxy_addr,
            "/execute",
            &request,
            http_options,
        )?)
    }

    pub(super) fn batch_execute_with_http(
        &self,
        request: BatchExecuteRequest,
        http_options: HttpRequestOptions,
        continuous_failed_time_ms: Option<u64>,
    ) -> Result<BatchExecuteResponse, ClientError> {
        self.batch_execute_with_http_and_policy(
            request,
            http_options,
            continuous_failed_time_ms,
            ReplicaReadPolicy::PinPrimary,
            None,
        )
    }

    /// A batch sent under a chosen replica read policy.
    ///
    /// The route is resolved with the policy on every attempt, including the refreshes after a
    /// backend failure -- a retry that silently fell back to the primary would make the policy
    /// hold only until the first error.
    pub(super) fn batch_execute_with_http_and_policy(
        &self,
        request: BatchExecuteRequest,
        http_options: HttpRequestOptions,
        continuous_failed_time_ms: Option<u64>,
        replica_read_policy: ReplicaReadPolicy,
        preferred_location: Option<&str>,
    ) -> Result<BatchExecuteResponse, ClientError> {
        if self.inner.options.meta_addr.is_some() {
            let server_addr =
                self.resolve_route_with_policy(
                request.shard_id,
                false,
                continuous_failed_time_ms,
                replica_read_policy,
                preferred_location,
            )?;
            return post_json_with_options(&server_addr, "/batch_execute", &request, http_options)
                .or_else(|err| {
                    let became_continuous = self.record_backend_failure(
                        &server_addr,
                        continuous_failed_time_ms
                            .unwrap_or(self.inner.options.topo_error_retry_interval_ms),
                    );
                    self.inner
                        .stats
                        .lock()
                        .expect("client stats lock poisoned")
                        .record_backend_error(became_continuous);
                    if !self.inner.options.refresh_route_on_backend_error {
                        return Err(err.into());
                    }
                    if !Self::may_send_again(&request.commands, &err) {
                        self.inner
                            .stats
                            .lock()
                            .expect("client stats lock poisoned")
                            .record_write_of_unknown_outcome();
                        let _ = self.resolve_route_with_policy(
                            request.shard_id,
                            true,
                            continuous_failed_time_ms,
                            replica_read_policy,
                            preferred_location,
                        );
                        return Err(ClientError::WriteOutcomeUnknown(err.to_string()));
                    }
                    let refreshed = self.resolve_route_with_policy(
                        request.shard_id,
                        true,
                        continuous_failed_time_ms,
                        replica_read_policy,
                        preferred_location,
                    )?;
                    let response = post_json_with_options(
                        &refreshed,
                        "/batch_execute",
                        &request,
                        http_options,
                    )?;
                    self.record_backend_success(&refreshed);
                    self.inner
                        .stats
                        .lock()

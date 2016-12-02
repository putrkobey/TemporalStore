// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Worker loop + task dispatch execution, extracted from data_node.rs.

use super::*;
use std::sync::Arc;

pub(super) fn worker_loop(inner: Arc<DataNodeRuntimeInner>) {
    loop {
        let task = {
            let mut queue = inner.queue.lock().expect("runtime queue lock poisoned");
            loop {
                if let Some(task) = queue.pop_ready() {
                    break task;
                }
                queue = inner
                    .queue_signal
                    .wait(queue)
                    .expect("runtime queue lock poisoned");
            }
        };
        let shard_id = task.request.shard_id();
        let output = if Instant::now() > task.deadline {
            inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .timed_out_total += 1;
            task_timeout_output(task.kind)
        } else if take_canceled(&inner, task.job_id) {
            inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .canceled_total += 1;
            task_canceled_output(&task, "data node task canceled before execution")
        } else {
            let output = execute_task(&inner, &task);
            if task_output_status(&output).code == "job_canceled"
                && take_canceled(&inner, task.job_id)
            {
                inner
                    .stats
                    .lock()
                    .expect("runtime stats lock poisoned")
                    .canceled_total += 1;
            } else {
                let _ = take_canceled(&inner, task.job_id);
            }
            output
        };
        {
            let mut queue = inner.queue.lock().expect("runtime queue lock poisoned");
            queue.finish_shard(shard_id);
            inner.queue_signal.notify_all();
        }
        let finished = DataNodeTaskStatus {
            job_id: task.job_id,
            kind: task.kind,
            status: task_output_status(&output),
            output: Some(output),
            submitted_at_ms: task.submitted_at_ms,
            finished_at_ms: Some(now_ms()),
        };
        {
            let mut jobs = inner
                .jobs
                .lock()
                .expect("data node jobs lock poisoned");
            jobs.insert(task.job_id, finished);
            // A finished status carries its whole output, and a dump output carries the
            // serialized index: 2.4 MB for 20k records, 7.3 MB for 60k, growing with the
            // store. Nothing removed these, so every dump a node ever ran stayed resident.
            // Unfinished jobs are always kept - the queue holds far more than the retention
            // bound, and a queued job must stay observable until it reports.
            let keep = max_retained_finished_jobs();
            let finished_count = jobs.values().filter(|job| job.finished_at_ms.is_some()).count();
            if finished_count > keep {
                let mut finished_ids: Vec<u64> = jobs
                    .values()
                    .filter(|job| job.finished_at_ms.is_some())
                    .map(|job| job.job_id)
                    .collect();
                finished_ids.sort_unstable();
                for job_id in finished_ids.into_iter().take(finished_count - keep) {
                    jobs.remove(&job_id);
                }
            }
        }
        inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned")
            .completed_total += 1;
    }
}

/// TS_MAX_RETAINED_FINISHED_JOBS: how many completed job statuses stay queryable.
///
/// Each carries its full output, and a dump output carries the serialized index, so an
/// unbounded map meant a node retained one index copy per dump for its whole life.
fn max_retained_finished_jobs() -> usize {
    std::env::var("TS_MAX_RETAINED_FINISHED_JOBS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(64)
}

pub(super) fn execute_task(inner: &DataNodeRuntimeInner, task: &QueuedTask) -> DataNodeTaskOutput {
    let cancellation = TaskCancellation {
        inner,
        job_id: task.job_id,
    };
    match &task.request {
        TaskRequest::Load(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(
                    task,
                    "data node load canceled before lifecycle start",
                );
            }
            DataNodeTaskOutput::Load(load_shard_with_inner(inner, request.clone()))
        }
        TaskRequest::Reload(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(
                    task,
                    "data node reload canceled before lifecycle start",
                );
            }
            DataNodeTaskOutput::Reload(reload_shard_with_inner(inner, request.clone()))
        }
        TaskRequest::Unload(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(
                    task,
                    "data node unload canceled before lifecycle start",
                );
            }
            DataNodeTaskOutput::Unload(unload_shard_with_inner(inner, request.clone(), false))
        }
        TaskRequest::Execute(request) => {
            if let Err(status) = validate_foreground_write_allowed_inner(
                inner,
                request.shard_id,
                std::slice::from_ref(&request.command),
            ) {
                return DataNodeTaskOutput::Execute(ExecuteResponse {
                    status,

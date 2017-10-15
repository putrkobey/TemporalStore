// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

pub fn parse_context_resource(request: ContextResourceParseRequest) -> ContextResourceParseReport {
    let resource_type =
        infer_context_resource_type(&request.raw_uri, request.resource_type.as_deref());
    let import_kind = context_resource_import_kind(&request.raw_uri, &resource_type);
    let owner_scope = if request.owner_scope.trim().is_empty() {
        "user".to_string()
    } else {
        request.owner_scope.trim().to_string()
    };
    let scope = context_scope_descriptor(&owner_scope);
    let parser_name = if request.parser_name.trim().is_empty() {
        default_resource_parser_name()
    } else {
        request.parser_name.trim().to_string()
    };
    let max_chunk_chars = request.max_chunk_chars.max(1);
    let overlap_chars = request.overlap_chars.min(max_chunk_chars.saturating_sub(1));
    let payload_size_bytes = request.text.as_bytes().len();
    let max_inline_bytes = default_context_resource_max_inline_bytes();
    let inline_payload = payload_size_bytes <= max_inline_bytes;
    let external_object_uri = if inline_payload {
        String::new()
    } else {
        context_resource_object_store_uri(&request.raw_uri, payload_size_bytes)
    };
    let units = context_resource_units(&request.text, &resource_type, &request.raw_uri);
    let mut chunks = Vec::new();
    let mut parser_warnings = Vec::new();

    for (unit_index, mut unit) in units.into_iter().enumerate() {
        let text = unit.remove("text").unwrap_or_default();
        let heading_path = unit
            .get("heading_path")
            .map(|path| {
                path.split('/')
                    .filter(|part| !part.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let parent_source_ref = unit
            .get("parent_heading_slug")
            .filter(|slug| !slug.is_empty())
            .map(|slug| format!("{}#heading={slug}", request.raw_uri));
        for (split_index, piece) in
            split_context_resource_text(&text, max_chunk_chars, overlap_chars)
                .into_iter()
                .enumerate()
        {
            if piece.trim().is_empty() {
                continue;
            }
            let chunk_index = chunks.len();
            unit.insert("resource_type".to_string(), resource_type.clone());
            unit.insert(
                "import_kind".to_string(),
                context_resource_import_kind_name(import_kind).to_string(),
            );
            unit.insert("parser_name".to_string(), parser_name.clone());
            unit.insert(
                "parser_version".to_string(),
                default_resource_parser_version(),
            );
            unit.insert("owner_scope".to_string(), owner_scope.clone());
            unit.insert(
                "scope_layer".to_string(),
                context_scope_layer_name(&scope.layer).to_string(),
            );
            unit.insert("scope_owner_id".to_string(), scope.owner_id.clone());
            unit.insert(
                "shared_graph_scope".to_string(),
                scope.shared_graph_scope.clone(),
            );
            if !scope.producer_agent_id.is_empty() {
                unit.insert(
                    "producer_agent_id".to_string(),
                    scope.producer_agent_id.clone(),
                );
            }
            unit.insert("chunk_index".to_string(), chunk_index.to_string());
            unit.insert("unit_index".to_string(), unit_index.to_string());
            unit.insert("split_index".to_string(), split_index.to_string());
            unit.insert("raw_uri".to_string(), request.raw_uri.clone());
            unit.insert(
                "uri_scheme".to_string(),
                context_resource_uri_scheme(&request.raw_uri),
            );
            unit.insert(
                "resource_title".to_string(),
                context_resource_title(&request.raw_uri),
            );
            if let Some(extension) = context_resource_extension(&request.raw_uri) {
                unit.insert("source_extension".to_string(), extension);
            }
            unit.insert(
                "payload_size_bytes".to_string(),
                payload_size_bytes.to_string(),
            );
            unit.insert("max_inline_bytes".to_string(), max_inline_bytes.to_string());
            unit.insert("inline_payload".to_string(), inline_payload.to_string());
            if !external_object_uri.is_empty() {
                unit.insert(
                    "external_object_uri".to_string(),
                    external_object_uri.clone(),
                );
                unit.insert("storage_backend".to_string(), "objectstore".to_string());
                unit.insert(
                    "storage_value_mode".to_string(),
                    "object_ref_json".to_string(),
                );
            } else {
                unit.insert("storage_backend".to_string(), "temporalstore".to_string());
                unit.insert(
                    "storage_value_mode".to_string(),
                    "raw_body_utf8".to_string(),
                );
            }
            let source_ref = context_resource_source_ref(&request.raw_uri, &unit);
            unit.insert("source_ref".to_string(), source_ref.clone());
            let content_hash = stable_hash64(&format!("resource_content:{source_ref}:{piece}"));
            unit.insert("content_hash".to_string(), content_hash.to_string());
            let linked_refs = extract_markdown_link_refs(&piece);
            if !linked_refs.is_empty() {
                unit.insert("linked_refs".to_string(), linked_refs.join(","));
            }
            let chunk_kind = unit
                .get("code_language")
                .map(|_| "code")
                .unwrap_or("text")
                .to_string();
            unit.insert("chunk_kind".to_string(), chunk_kind);
            if piece.len() > max_chunk_chars {
                parser_warnings.push(format!(
                    "chunk {} exceeded max_chunk_chars after boundary adjustment",
                    chunk_index
                ));
            }
            let chunk_hash = request
                .chunk_hash_base
                .map(|base| base.saturating_add(chunk_index as u64))
                .unwrap_or_else(|| stable_hash64(&format!("resource_chunk:{source_ref}:{piece}")));
            let embedding_ref_hash = stable_hash64(&format!(
                "resource_chunk_embedding:{}:{chunk_hash}:{}",
                request.raw_uri, "mock-embedding-v1"
            ));
            chunks.push(ContextParsedResourceChunk {
                chunk_hash,
                content_hash,
                embedding_ref_hash,
                source_ref,
                parent_source_ref: parent_source_ref.clone(),
                heading_path: heading_path.clone(),
                token_estimate: estimate_tokens(&piece),
                text: piece,
                metadata: unit.clone(),
            });
        }
    }

    let total_tokens = chunks.iter().fold(0_u32, |total, chunk| {
        total.saturating_add(chunk.token_estimate)
    });
    let content_hash = stable_hash64(&format!(
        "resource_lifecycle:{}:{}:{}",
        request.raw_uri, request.version, request.text
    ));
    let watched = request.watch_interval_minutes > 0
        || matches!(import_kind, ContextResourceImportKind::WatchedResource);
    let version = if request.version.trim().is_empty() {
        format!("{content_hash:x}")
    } else {
        request.version.trim().to_string()
    };
    let lifecycle = ContextResourceLifecycleRecord {
        raw_uri: request.raw_uri.clone(),
        target_uri: context_resource_target_uri(&request.raw_uri),
        owner_scope,
        scope,
        parser_name,
        parser_version: default_resource_parser_version(),
        resource_type: resource_type.clone(),
        import_kind,
        action: if watched {
            ContextResourceLifecycleAction::Watch
        } else {
            ContextResourceLifecycleAction::Add
        },
        version,
        content_hash,
        stale: false,
        invalidates_version: String::new(),
        watched,
        watch_interval_minutes: request.watch_interval_minutes,
        next_refresh_after_ms: request.watch_interval_minutes.saturating_mul(60_000),
        deleted: false,

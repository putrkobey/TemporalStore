// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Context model-provider calls (summaries/embeddings, OpenAI-compatible), split from context_workflow.rs.
use super::*;

#[derive(Debug, Clone)]
pub(crate) struct ContextExtractSummaries {
    pub(crate) status: Status,
    pub(crate) provider: ContextModelProviderConfig,
    pub(crate) l0: String,
    pub(crate) l1: String,
    pub(crate) l2_ref: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct OpenAiChatCompletionRequest<'a> {
    model: &'a str,
    messages: Vec<OpenAiChatMessage<'a>>,
    temperature: f32,
}

#[derive(Debug, Serialize)]
pub(crate) struct OpenAiChatMessage<'a> {
    role: &'a str,
    content: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct OpenAiEmbeddingRequest<'a> {
    model: &'a str,
    input: Vec<&'a str>,
}

/// When set (`MATRIXARK_REQUIRE_MODEL_SUMMARIES=1`), context summaries must come from a
/// real model: mock/deterministic providers are rejected loudly and, if the model omits
/// l0/l1, the model's own output is used rather than the title/body string heuristic.
/// Off by default so CI/dev without credentials still runs deterministically.
pub(crate) fn context_require_model_summaries() -> bool {
    // `1` and `true` were the only words that reached this; `yes` and `on` read as off, and so
    // did a value with a stray space. An operator who asks for a real model and silently gets
    // the heuristic has no way to tell from the outside.
    crate::env_flag::env_bool("MATRIXARK_REQUIRE_MODEL_SUMMARIES", false)
}

/// When set (`MATRIXARK_REQUIRE_MODEL_EMBEDDINGS=1`), embeddings must come from a real
/// model: a mock/deterministic provider is rejected loudly instead of emitting hash vectors.
pub(crate) fn context_require_model_embeddings() -> bool {
    // Same shape as its neighbour above, and the same silent answer: asking for a real model
    // with `on` left the mock provider emitting hash vectors.
    crate::env_flag::env_bool("MATRIXARK_REQUIRE_MODEL_EMBEDDINGS", false)
}

pub(crate) fn context_summaries_for_extract(
    provider: &ContextModelProviderConfig,
    request: &ContextExtractRequest,
) -> Result<ContextExtractSummaries, Status> {
    let provider = normalize_provider(provider.clone());
    if provider.mock_mode || provider.provider_kind == ContextProviderKind::Mock {
        if context_require_model_summaries() {
            return Err(Status::error(
                "model_required_but_provider_is_mock",
                "MATRIXARK_REQUIRE_MODEL_SUMMARIES is set but the context provider is mock/deterministic; configure a real OpenAI-compatible (OSS) provider with mock_mode=false",
            ));
        }
        return Ok(mock_context_summaries(provider, request));
    }
    match provider.provider_kind {
        ContextProviderKind::OpenAiCompatible => {
            let content = call_openai_compatible_context_provider(&provider, request)?;
            let (l0, l1) = parse_provider_summary_content(&content, request);
            let l2_ref = format!(
                "tsctx://tenant/{}/model/{}/source/{}",
                request.tenant_hash, provider.provider_name, request.source_id
            );
            Ok(ContextExtractSummaries {
                status: Status::ok(),
                provider,
                l0,
                l1,
                l2_ref,
            })
        }
        ContextProviderKind::Mock => Ok(mock_context_summaries(provider, request)),
    }
}

/// The embedding provider a deployment has configured, read from the environment.
///
/// The engine could already call a real embedding endpoint, but only a caller holding a
/// `ContextModelProviderConfig` could reach it -- and the proxy had none, which is why a query
/// vector had to be computed elsewhere and sent in.
///
/// It reads the variables the rest of the deployment already sets rather than a second set of its
/// own: `MATRIXARK_EMBEDDING_API_BASE`, `MATRIXARK_EMBEDDING_MODEL`, and
/// `MATRIXARK_EMBEDDING_API_KEY_ENV` for the NAME of the variable holding a key, so a key is never
/// read from a value that might be logged.
///
/// With no base URL the provider stays deterministic, exactly as it was: a deployment that has not
/// configured an encoder does not silently acquire one, and one that requires a real model still
/// gets the error `MATRIXARK_REQUIRE_MODEL_EMBEDDINGS` exists to raise.
pub fn context_provider_from_env() -> ContextModelProviderConfig {
    // Two names for one endpoint, because two paths grew their own: the serving side and the
    // Python side read `MATRIXARK_EMBEDDING_API_BASE`, the embed drainer read
    // `MATRIXARK_EMBED_BASE_URL`. Both are honoured here so there is one constructor rather than
    // two spellings of the same deployment, and the newer name wins when both are set.
    let base_url = std::env::var("MATRIXARK_EMBEDDING_API_BASE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("MATRIXARK_EMBED_BASE_URL")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .unwrap_or_default()
        .trim()
        .trim_end_matches('/')
        .to_string();
    let embedding_model = std::env::var("MATRIXARK_EMBEDDING_MODEL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let api_key_env = std::env::var("MATRIXARK_EMBEDDING_API_KEY_ENV")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("MATRIXARK_EMBED_API_KEY_ENV")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .unwrap_or_default()
        .trim()
        .to_string();

    let mut provider = ContextModelProviderConfig {
        base_url: base_url.clone(),
        api_key_env,
        // An endpoint that answers without a key is the local, self-hosted case, and it is the
        // configuration this is most often pointed at.
        mock_mode: base_url.is_empty(),
        ..ContextModelProviderConfig::default()
    };
    if !base_url.is_empty() {
        provider.provider_kind = ContextProviderKind::OpenAiCompatible;
    }
    if let Some(model) = embedding_model {
        provider.embedding_model = model;
    }
    provider
}

/// Backfill helper: compute REAL model embeddings for a batch of already-stored
/// context node/event texts, reusing the exact provider path the live extract
/// path uses -- request batching, `MATRIXARK_REQUIRE_MODEL_EMBEDDINGS`
/// enforcement, and response shape/finiteness validation. Returns one vector per
/// input text, in the same order.
///
/// This exists so an out-of-band embed-backfill (the `context_embed_backfill`
/// bin) can attach semantic vectors to `raw_first` bulk stores WITHOUT rerunning
/// summary extraction or changing live-ingest behavior.
pub fn context_backfill_embeddings(
    provider: &ContextModelProviderConfig,
    texts: &[&str],
) -> Result<Vec<Vec<f32>>, Status> {
    if texts.is_empty() {
        return Ok(Vec::new());
    }
    let inputs: Vec<(&str, u64, u32, &str)> = texts
        .iter()
        .map(|text| ("node_l0", 0u64, 1u32, *text))
        .collect();
    let (vectors, _report) = context_embeddings_for_extract(provider, &inputs)?;
    Ok(vectors)
}

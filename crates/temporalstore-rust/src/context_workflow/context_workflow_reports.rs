// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Context workflow provider/state/manage report + policy-validation fns, split from context_workflow.rs.
use super::*;

pub fn default_context_model_providers() -> Vec<ContextModelProviderConfig> {
    vec![
        ContextModelProviderConfig::default(),
        ContextModelProviderConfig {
            provider_name: "openai-compatible".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:11434/v1".to_string(),
            api_key_env: "OPENAI_API_KEY".to_string(),
            model: "local-or-commercial-chat-model".to_string(),
            embedding_model: "local-or-commercial-embedding-model".to_string(),
            vlm_model: "local-or-commercial-vlm-model".to_string(),
            timeout_ms: 30_000,
            max_retries: 2,
            fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
            mock_mode: false,
        },
        ContextModelProviderConfig {
            provider_name: "reference-open-source-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:11434/v1".to_string(),
            api_key_env: "MATRIXARK_MODEL_API_KEY".to_string(),
            model: "qwen2.5:7b-instruct".to_string(),
            embedding_model: "nomic-embed-text".to_string(),
            vlm_model: "qwen2.5vl:7b".to_string(),
            timeout_ms: 30_000,
            max_retries: 2,
            fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
            mock_mode: false,
        },
        ContextModelProviderConfig {
            provider_name: "reference-gpt-4o-mini-reader".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "https://api.openai.com/v1".to_string(),
            api_key_env: "OPENAI_API_KEY".to_string(),
            model: "gpt-4o-mini".to_string(),
            embedding_model: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
            vlm_model: "none".to_string(),
            timeout_ms: 30_000,
            max_retries: 2,
            fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
            mock_mode: false,
        },
        ContextModelProviderConfig {
            provider_name: "matrixark-native-oss-context".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:8000/v1".to_string(),
            api_key_env: "MATRIXARK_MODEL_API_KEY".to_string(),
            model: "google/flan-t5-small".to_string(),
            embedding_model: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
            vlm_model: "none".to_string(),
            timeout_ms: 30_000,
            max_retries: 2,
            fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
            mock_mode: false,
        },
        ContextModelProviderConfig {
            provider_name: "reference-open-source-gpt-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:8000/v1".to_string(),
            api_key_env: "MATRIXARK_MODEL_API_KEY".to_string(),
            model: "lmsys/vicuna-7b-v1.5".to_string(),
            embedding_model: "BAAI/bge-m3".to_string(),
            vlm_model: "Vision-CAIR/MiniGPT-4".to_string(),
            timeout_ms: 30_000,
            max_retries: 2,
            fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
            mock_mode: false,
        },
    ]
}

pub fn reference_open_source_model_profiles() -> Vec<ContextReferenceModelProfile> {
    vec![
        ContextReferenceModelProfile {
            profile_name: "reference-qwen2_5_vl-local".to_string(),
            provider_name: "reference-open-source-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:11434/v1".to_string(),
            chat_model: "qwen2.5:7b-instruct".to_string(),
            vlm_model: "qwen2.5vl:7b".to_string(),
            embedding_model: "nomic-embed-text".to_string(),
            capabilities: vec![
                "vlm_image_content_understanding".to_string(),
                "chat_context_extraction".to_string(),
                "embedding_vectorization".to_string(),
                "semantic_retrieval".to_string(),
            ],
            notes: "Recommended local reference-style profile for Ollama or another OpenAI-compatible local gateway."
                .to_string(),
        },
        ContextReferenceModelProfile {
            profile_name: "reference-llava-local".to_string(),
            provider_name: "reference-llava-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:11434/v1".to_string(),
            chat_model: "llama3.1:8b-instruct".to_string(),
            vlm_model: "llava:7b".to_string(),
            embedding_model: "nomic-embed-text".to_string(),
            capabilities: vec![
                "vlm_image_content_understanding".to_string(),
                "chat_context_extraction".to_string(),
                "embedding_vectorization".to_string(),
                "semantic_retrieval".to_string(),
            ],
            notes: "Fallback local profile for LLaVA-compatible OpenAI gateway deployments."
                .to_string(),
        },
        ContextReferenceModelProfile {
            profile_name: "reference-internvl-vllm".to_string(),
            provider_name: "reference-internvl-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:8000/v1".to_string(),
            chat_model: "Qwen/Qwen2.5-7B-Instruct".to_string(),
            vlm_model: "OpenGVLab/InternVL2_5-8B".to_string(),
            embedding_model: "BAAI/bge-m3".to_string(),
            capabilities: vec![
                "vlm_image_content_understanding".to_string(),
                "chat_context_extraction".to_string(),
                "embedding_vectorization".to_string(),
                "semantic_retrieval".to_string(),
            ],
            notes: "reference-style vLLM or OpenAI-compatible gateway profile for GPU deployments."
                .to_string(),
        },
        ContextReferenceModelProfile {
            profile_name: "reference-gpt-4o-mini-reader".to_string(),
            provider_name: "reference-gpt-4o-mini-reader".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "https://api.openai.com/v1".to_string(),
            chat_model: "gpt-4o-mini".to_string(),
            vlm_model: "none".to_string(),
            embedding_model: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
            capabilities: vec![
                "reference_reader_parity".to_string(),
                "chat_context_extraction".to_string(),
                "semantic_retrieval".to_string(),

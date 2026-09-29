//! Bounded client retrieval contract for immutable export artifacts.

use super::{ArtifactId, ContentHash, RunId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const MAX_ARTIFACT_CHUNK_BYTES: u32 = 128 * 1024;
pub const ARTIFACT_QUERY_TOOL: &str = "artifact_query";

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactQuery {
    List {
        run_id: RunId,
    },
    Read {
        artifact_id: ArtifactId,
        expected_sha256: ContentHash,
        offset: u64,
        limit: u32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArtifactReadArguments {
    pub action: ArtifactReadAction,
    pub artifact_id: ArtifactId,
    pub expected_sha256: ContentHash,
    pub offset: u64,
    pub limit: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactReadAction {
    Read,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRetrieval {
    pub tool_name: String,
    pub read_arguments: ArtifactReadArguments,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArtifactDescriptor {
    pub artifact_id: ArtifactId,
    pub run_id: RunId,
    pub file_name: String,
    pub media_type: String,
    pub bytes: u64,
    pub sha256: ContentHash,
    pub uncompressed_sha256: Option<ContentHash>,
    pub uncompressed_bytes: Option<u64>,
    pub complete: bool,
    pub retrieval: ArtifactRetrieval,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ArtifactEncoding {
    Hex,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArtifactChunk {
    pub artifact: ArtifactDescriptor,
    pub offset: u64,
    pub raw_bytes: u32,
    pub encoding: ArtifactEncoding,
    pub data_hex: String,
    pub chunk_sha256: ContentHash,
    pub next_offset: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactQueryResult {
    Catalog {
        run_id: RunId,
        artifacts: Vec<ArtifactDescriptor>,
        returned_count: u64,
    },
    Chunk {
        chunk: Box<ArtifactChunk>,
    },
}

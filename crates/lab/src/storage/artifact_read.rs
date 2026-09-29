//! Bounded reads of immutable, catalogued export artifacts.

use super::{Store, io_error, reject_symlink_chain, validate_relative_path};
use crate::contracts::{
    ARTIFACT_QUERY_TOOL, ArtifactDescriptor, ArtifactId, ArtifactReadAction, ArtifactReadArguments,
    ArtifactRef, ArtifactRetrieval, ContentHash, LabError, MAX_ARTIFACT_CHUNK_BYTES, RunId,
};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const MAX_ARTIFACT_CATALOG_ENTRIES: usize = 128;

#[derive(Debug)]
pub struct ArtifactChunkRead {
    pub artifact: ArtifactDescriptor,
    pub offset: u64,
    pub data: Vec<u8>,
    pub chunk_sha256: ContentHash,
    pub next_offset: Option<u64>,
}

impl Store {
    /// Return public metadata for every immutable artifact of one run.
    ///
    /// # Errors
    /// Rejects corrupt catalog identities, unsafe paths, or SQLite failure.
    pub fn artifact_catalog(&self, run_id: &RunId) -> Result<Vec<ArtifactDescriptor>, LabError> {
        if self.load_run_header(run_id)?.is_none() {
            return Err(LabError::InvalidConfig("unknown run".into()));
        }
        let artifacts = self.list_artifacts(run_id)?;
        if artifacts.len() > MAX_ARTIFACT_CATALOG_ENTRIES {
            return Err(LabError::ResourceLimit(format!(
                "artifact catalog exceeds {MAX_ARTIFACT_CATALOG_ENTRIES} entries"
            )));
        }
        artifacts
            .into_iter()
            .map(|artifact| authenticated_descriptor(&artifact))
            .collect()
    }

    /// Read one bounded raw chunk after authenticating the immutable catalog entry.
    ///
    /// # Errors
    /// Rejects unknown artifacts, hash pins, bounds, unsafe paths, changed lengths, or I/O failure.
    pub fn read_artifact_chunk(
        &self,
        artifact_id: &ArtifactId,
        expected_sha256: &ContentHash,
        offset: u64,
        limit: u32,
    ) -> Result<ArtifactChunkRead, LabError> {
        validate_chunk_limit(limit)?;
        let artifact = self
            .load_artifact(artifact_id)?
            .ok_or_else(|| LabError::InvalidConfig("unknown artifact".into()))?;
        validate_hash_pin(&artifact, expected_sha256)?;
        let descriptor = authenticated_descriptor(&artifact)?;
        let path = self.raw_objects().root.join(&artifact.relative_path);
        read_chunk(
            &self.raw_objects().root,
            &path,
            &artifact,
            descriptor,
            offset,
            limit,
        )
    }
}

fn authenticated_descriptor(artifact: &ArtifactRef) -> Result<ArtifactDescriptor, LabError> {
    validate_relative_path(&artifact.relative_path)?;
    if !artifact.relative_path.starts_with("exports/") || !artifact.complete {
        return Err(LabError::DataCorrupt(
            "artifact is not a complete export".into(),
        ));
    }
    let expected_id =
        ArtifactId::for_file(&artifact.run_id, &artifact.relative_path, &artifact.sha256);
    if artifact.id != expected_id {
        return Err(LabError::DataCorrupt(
            "artifact catalog identity mismatch".into(),
        ));
    }
    let file_name = Path::new(&artifact.relative_path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| LabError::DataCorrupt("artifact has no valid file name".into()))?
        .to_owned();
    Ok(ArtifactDescriptor {
        artifact_id: artifact.id.clone(),
        run_id: artifact.run_id.clone(),
        file_name,
        media_type: artifact.media_type.clone(),
        bytes: artifact.bytes,
        sha256: artifact.sha256.clone(),
        uncompressed_sha256: artifact.uncompressed_sha256.clone(),
        uncompressed_bytes: artifact.uncompressed_bytes,
        complete: artifact.complete,
        retrieval: ArtifactRetrieval {
            tool_name: ARTIFACT_QUERY_TOOL.into(),
            read_arguments: ArtifactReadArguments {
                action: ArtifactReadAction::Read,
                artifact_id: artifact.id.clone(),
                expected_sha256: artifact.sha256.clone(),
                offset: 0,
                limit: MAX_ARTIFACT_CHUNK_BYTES,
            },
        },
    })
}

fn validate_hash_pin(
    artifact: &ArtifactRef,
    expected_sha256: &ContentHash,
) -> Result<(), LabError> {
    if &artifact.sha256 == expected_sha256 {
        Ok(())
    } else {
        Err(LabError::InputHashMismatch(
            "artifact SHA-256 pin differs from catalog".into(),
        ))
    }
}

fn read_chunk(
    root: &Path,
    path: &Path,
    artifact: &ArtifactRef,
    descriptor: ArtifactDescriptor,
    offset: u64,
    limit: u32,
) -> Result<ArtifactChunkRead, LabError> {
    reject_symlink_chain(root, path)?;
    let path_metadata = std::fs::symlink_metadata(path).map_err(io_error("inspect artifact"))?;
    if !path_metadata.file_type().is_file() || path_metadata.len() != artifact.bytes {
        return Err(LabError::DataCorrupt(
            "catalogued artifact is not a regular file of the expected length".into(),
        ));
    }
    let mut file = File::open(path).map_err(io_error("open artifact"))?;
    let metadata = file.metadata().map_err(io_error("inspect artifact"))?;
    if !metadata.is_file() || metadata.len() != artifact.bytes {
        return Err(LabError::DataCorrupt(
            "catalogued artifact length changed".into(),
        ));
    }
    if offset > artifact.bytes {
        return Err(LabError::InvalidConfig(
            "artifact offset exceeds file length".into(),
        ));
    }
    let remaining = artifact.bytes - offset;
    let chunk_bytes = remaining.min(u64::from(limit));
    let chunk_len = usize::try_from(chunk_bytes)
        .map_err(|_| LabError::ResourceLimit("artifact chunk exceeds platform capacity".into()))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(io_error("seek artifact"))?;
    let mut data = vec![0_u8; chunk_len];
    if let Err(error) = file.read_exact(&mut data) {
        return if error.kind() == std::io::ErrorKind::UnexpectedEof {
            Err(LabError::DataCorrupt(
                "artifact changed during chunk read".into(),
            ))
        } else {
            Err(io_error("read artifact chunk")(error))
        };
    }
    let end = offset
        .checked_add(chunk_bytes)
        .ok_or_else(|| LabError::ResourceLimit("artifact offset overflow".into()))?;
    Ok(ArtifactChunkRead {
        artifact: descriptor,
        offset,
        chunk_sha256: ContentHash::of_bytes(&data),
        data,
        next_offset: (end < artifact.bytes).then_some(end),
    })
}

fn validate_chunk_limit(limit: u32) -> Result<(), LabError> {
    if (1..=MAX_ARTIFACT_CHUNK_BYTES).contains(&limit) {
        Ok(())
    } else {
        Err(LabError::ResourceLimit(format!(
            "artifact chunk limit must be in 1..={MAX_ARTIFACT_CHUNK_BYTES}"
        )))
    }
}

#[cfg(test)]
mod tests;

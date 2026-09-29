use super::*;
use crate::contracts::{ArtifactRef, RunId};

#[test]
fn bounded_chunks_reassemble_exactly_and_reject_invalid_identity_path_hash_and_bounds() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "spot-lab-artifact-read-{}-{nonce}",
        std::process::id()
    ));
    let relative_path = "exports/test-run/review.json";
    let path = root.join(relative_path);
    std::fs::create_dir_all(path.parent().expect("artifact parent")).expect("create export");
    let body = b"immutable-artifact-body-with-several-chunks";
    std::fs::write(&path, body).expect("write artifact");
    let run_id = RunId::new("run-artifact-read").expect("run id");
    let sha256 = ContentHash::of_bytes(body);
    let artifact_ref = artifact(&run_id, relative_path, body, sha256.clone());
    let descriptor = authenticated_descriptor(&artifact_ref).expect("authenticate artifact");

    let mut reconstructed = Vec::new();
    let mut offset = 0;
    loop {
        let chunk = read_chunk(&root, &path, &artifact_ref, descriptor.clone(), offset, 7)
            .expect("read bounded chunk");
        assert_eq!(chunk.chunk_sha256, ContentHash::of_bytes(&chunk.data));
        reconstructed.extend_from_slice(&chunk.data);
        let Some(next) = chunk.next_offset else {
            break;
        };
        assert!(next > offset);
        offset = next;
    }
    assert_eq!(reconstructed, body);
    assert_eq!(ContentHash::of_bytes(&reconstructed), sha256);

    assert!(matches!(
        validate_chunk_limit(0),
        Err(LabError::ResourceLimit(_))
    ));
    assert!(matches!(
        read_chunk(
            &root,
            &path,
            &artifact_ref,
            descriptor.clone(),
            u64::try_from(body.len()).expect("body length") + 1,
            1,
        ),
        Err(LabError::InvalidConfig(_))
    ));
    let mut wrong_id = artifact_ref.clone();
    wrong_id.id = ArtifactId::new("artifact-wrong").expect("wrong id");
    assert!(matches!(
        authenticated_descriptor(&wrong_id),
        Err(LabError::DataCorrupt(_))
    ));
    assert!(matches!(
        validate_hash_pin(&artifact_ref, &ContentHash::of_bytes(b"wrong artifact")),
        Err(LabError::InputHashMismatch(_))
    ));
    let escaped = artifact(&run_id, "../review.json", body, ContentHash::of_bytes(body));
    assert!(matches!(
        authenticated_descriptor(&escaped),
        Err(LabError::InvalidConfig(_))
    ));

    std::fs::remove_dir_all(root).expect("remove artifact root");
}

fn artifact(run_id: &RunId, relative_path: &str, body: &[u8], sha256: ContentHash) -> ArtifactRef {
    ArtifactRef {
        id: ArtifactId::from_seed(&format!("{run_id}:{relative_path}:{sha256}")),
        run_id: run_id.clone(),
        relative_path: relative_path.into(),
        media_type: "application/json".into(),
        bytes: u64::try_from(body.len()).expect("artifact bytes"),
        sha256,
        uncompressed_sha256: None,
        uncompressed_bytes: None,
        complete: true,
    }
}

#![cfg(feature = "replication")]
#[path = "support/file_backend.rs"]
mod support;
use sqlite_session::replication::{
    Backend,
    files::{Availability, Config, Objects},
};
use std::{
    io::{self, Read},
    sync::{Arc, Barrier, atomic::Ordering},
    time::Duration,
};
use support::FileBackend;

fn config(path: &std::path::Path) -> Config {
    Config::new(path.to_owned(), [7; 32], "127.0.0.1:0".parse().unwrap())
}
fn content(size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| (i.wrapping_mul(73) ^ (i >> 8)) as u8)
        .collect()
}

#[test]
fn empty_and_chunk_boundary_files_survive_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FileBackend::open(&tmp.path().join("spool"), 1, 2));
    let cfg = config(&tmp.path().join("objects"));
    let objects = Objects::open(cfg.clone(), backend.clone()).unwrap();
    for size in [0, 1, 65535, 65536, 65537, 131072, 131073] {
        let name = size.to_string();
        let bytes = content(size);
        let meta = objects.upload(&name, bytes.as_slice()).unwrap();
        assert_eq!(meta.bytes, size as u64);
        let mut out = Vec::new();
        objects.read(&name, &mut out).unwrap();
        assert_eq!(out, bytes);
    }
    drop(objects);
    backend.store.close().unwrap();
    drop(backend);
    let reopened = Arc::new(FileBackend::open(&tmp.path().join("spool"), 1, 2));
    let objects = Objects::open(cfg, reopened.clone()).unwrap();
    for meta in objects.list().unwrap() {
        let mut out = Vec::new();
        objects.read(&meta.name, &mut out).unwrap();
        assert_eq!(out, content(meta.bytes as usize));
    }
    reopened.store.close().unwrap();
}

#[test]
fn corrupt_truncated_appended_and_malformed_objects_fail_closed() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FileBackend::open(&tmp.path().join("spool"), 1, 2));
    let objects = Objects::open(config(&tmp.path().join("files")), backend.clone()).unwrap();
    let meta = objects.upload("file", content(65537).as_slice()).unwrap();
    let path = objects.path(meta.digest, meta.generation);
    let good = std::fs::read(&path).unwrap();
    for len in [0, 8, 39, 40, 44, 100, good.len() - 1] {
        std::fs::write(&path, &good[..len]).unwrap();
        assert!(objects.read("file", io::sink()).is_err(), "length {len}");
    }
    for index in [0, 8, 16, 32, 50, good.len() - 1] {
        let mut bad = good.clone();
        bad[index] ^= 1;
        std::fs::write(&path, bad).unwrap();
        assert!(objects.read("file", io::sink()).is_err(), "byte {index}");
    }
    let mut bad = good.clone();
    bad.extend(b"trailing");
    std::fs::write(&path, bad).unwrap();
    assert!(objects.read("file", io::sink()).is_err());
    let mut bad = good.clone();
    bad[40..44].copy_from_slice(&(65536u32 + 17).to_be_bytes());
    std::fs::write(&path, bad).unwrap();
    assert!(objects.read("file", io::sink()).is_err());
    std::fs::write(&path, good).unwrap();
    objects.read("file", io::sink()).unwrap();
    backend.store.close().unwrap();
}

struct BrokenReader {
    emitted: bool,
}
impl Read for BrokenReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.emitted {
            return Err(io::Error::other("injected source failure"));
        }
        self.emitted = true;
        buf[..20].fill(9);
        Ok(20)
    }
}

#[test]
fn upload_read_and_publish_failures_preserve_existing_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FileBackend::open(&tmp.path().join("spool"), 1, 2));
    let mut cfg = config(&tmp.path().join("files"));
    cfg.max_file_bytes = 65536;
    let objects = Objects::open(cfg, backend.clone()).unwrap();
    let original = objects.upload("file", b"original".as_slice()).unwrap();
    assert!(
        objects
            .upload("new", BrokenReader { emitted: false })
            .is_err()
    );
    assert!(objects.metadata("new").unwrap().is_none());
    assert!(objects.upload("file", content(65537).as_slice()).is_err());
    assert_eq!(objects.metadata("file").unwrap(), Some(original.clone()));
    backend.fail_commit.store(1, Ordering::SeqCst);
    assert!(objects.upload("file", b"replacement".as_slice()).is_err());
    assert_eq!(objects.metadata("file").unwrap(), Some(original));
    let mut out = Vec::new();
    objects.read("file", &mut out).unwrap();
    assert_eq!(out, b"original");
    assert!(
        !std::fs::read_dir(tmp.path().join("files"))
            .unwrap()
            .any(|e| e
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".upload-"))
    );
    assert_eq!(objects.collect(Duration::ZERO).unwrap(), 1);
    backend.store.close().unwrap();
}

#[test]
fn collection_keeps_shared_live_objects_and_tombstones_after_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("spool");
    let cfg = config(&tmp.path().join("files"));
    let backend = Arc::new(FileBackend::open(&path, 1, 2));
    let objects = Objects::open(cfg.clone(), backend.clone()).unwrap();
    let meta = objects.upload("a", b"shared".as_slice()).unwrap();
    objects.upload("b", b"shared".as_slice()).unwrap();
    objects.delete("a").unwrap();
    assert_eq!(objects.collect(Duration::ZERO).unwrap(), 0);
    assert!(objects.path(meta.digest, 0).exists());
    objects.read("b", io::sink()).unwrap();
    objects.delete("b").unwrap();
    assert_eq!(objects.collect(Duration::ZERO).unwrap(), 1);
    assert!(objects.read("b", io::sink()).is_err());
    drop(objects);
    backend.store.close().unwrap();
    drop(backend);
    let backend = Arc::new(FileBackend::open(&path, 1, 2));
    let objects = Objects::open(cfg, backend.clone()).unwrap();
    assert!(objects.list().unwrap().is_empty());
    assert!(objects.metadata("b").unwrap().unwrap().deleted);
    backend.store.close().unwrap();
}

#[test]
fn interrupted_rotation_resumes_and_retirement_checks_missing_objects() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("spool");
    let mut cfg = config(&tmp.path().join("files"));
    let backend = Arc::new(FileBackend::open(&path, 1, 2));
    let old = Objects::open(cfg.clone(), backend.clone()).unwrap();
    old.upload("a", content(65537).as_slice()).unwrap();
    old.upload("b", b"second".as_slice()).unwrap();
    drop(old);
    cfg.previous_key = Some(cfg.key.clone());
    cfg.key = [8; 32].into();
    cfg.generation = 1;
    let objects = Objects::open(cfg.clone(), backend.clone()).unwrap();
    backend.fail_commit.store(1, Ordering::SeqCst);
    assert!(objects.rotate_keys(1).is_err());
    assert!(
        !objects
            .key_rotation_status()
            .unwrap()
            .previous_key_unused_locally
    );
    drop(objects);
    backend.store.close().unwrap();
    drop(backend);
    let backend = Arc::new(FileBackend::open(&path, 1, 2));
    let objects = Objects::open(cfg.clone(), backend.clone()).unwrap();
    assert_eq!(objects.rotate_keys(1).unwrap(), 1);
    assert_eq!(objects.rotate_keys(10).unwrap(), 1);
    objects.collect(Duration::ZERO).unwrap();
    assert!(
        objects
            .key_rotation_status()
            .unwrap()
            .previous_key_unused_locally
    );
    objects.read("a", io::sink()).unwrap();
    objects.read("b", io::sink()).unwrap();
    drop(objects);
    cfg.generation = 2;
    cfg.previous_key = Some(cfg.key.clone());
    cfg.key = [9; 32].into();
    let objects = Objects::open(cfg, backend.clone()).unwrap();
    let meta = objects.metadata("a").unwrap().unwrap();
    std::fs::remove_file(objects.path(meta.digest, meta.generation)).unwrap();
    let status = objects.key_rotation_status().unwrap();
    assert_eq!(status.old_generation_metadata_missing_locally, 1);
    assert!(!status.previous_key_unused_locally);
    backend.store.close().unwrap();
}

#[test]
fn resumed_stream_offsets_and_consumer_failure_are_reported() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FileBackend::open(&tmp.path().join("spool"), 1, 2));
    let objects = Objects::open(config(&tmp.path().join("files")), backend.clone()).unwrap();
    let bytes = content(131073);
    objects.upload("file", bytes.as_slice()).unwrap();
    for skip in [0, 1, 65535, 65536, 65537, bytes.len()] {
        let mut out = Vec::new();
        let mut offset = skip as u64;
        let (_, total) = objects
            .stream_plain_chunks("file", skip as u64, 4096, |at, data| {
                assert_eq!(at, offset);
                assert!(data.len() <= 4096);
                offset += data.len() as u64;
                out.extend_from_slice(data);
                Ok(())
            })
            .unwrap();
        assert_eq!(total, bytes.len() as u64);
        assert_eq!(out, bytes[skip..]);
    }
    assert!(
        objects
            .stream_plain_chunks("file", 0, 0, |_, _| Ok(()))
            .is_err()
    );
    assert!(
        objects
            .stream_plain_chunks("file", 0, 4096, |_, _| Err(
                sqlite_session::replication::Error::Storage("sink failed".into())
            ))
            .unwrap_err()
            .to_string()
            .contains("sink failed")
    );
    backend.store.close().unwrap();
}

#[test]
fn upload_rotation_and_collection_race_preserves_all_live_names() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FileBackend::open(&tmp.path().join("spool"), 1, 2));
    let mut cfg = config(&tmp.path().join("files"));
    let old = Objects::open(cfg.clone(), backend.clone()).unwrap();
    for i in 0..8 {
        old.upload(&format!("old-{i}"), content(1024 + i).as_slice())
            .unwrap();
    }
    drop(old);
    cfg.previous_key = Some(cfg.key.clone());
    cfg.key = [8; 32].into();
    cfg.generation = 1;
    let objects = Arc::new(Objects::open(cfg, backend.clone()).unwrap());
    let barrier = Arc::new(Barrier::new(3));
    std::thread::scope(|s| {
        for task in 0..3 {
            let objects = objects.clone();
            let barrier = barrier.clone();
            s.spawn(move || {
                barrier.wait();
                for i in 0..12 {
                    match task {
                        0 => {
                            objects
                                .upload(&format!("new-{i}"), content(i + 20).as_slice())
                                .unwrap();
                        }
                        1 => {
                            objects.rotate_keys(1).unwrap();
                        }
                        _ => {
                            objects.collect(Duration::ZERO).unwrap();
                        }
                    }
                }
            });
        }
    });
    for i in 0..8 {
        let mut out = Vec::new();
        objects.read(&format!("old-{i}"), &mut out).unwrap();
        assert_eq!(out, content(1024 + i));
    }
    for i in 0..12 {
        let mut out = Vec::new();
        objects.read(&format!("new-{i}"), &mut out).unwrap();
        assert_eq!(out, content(i + 20));
    }
    backend.store.close().unwrap();
}

#[test]
fn obsolete_fetched_object_cannot_replace_overwritten_or_deleted_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FileBackend::open(&tmp.path().join("spool"), 1, 2));
    let objects = Objects::open(config(&tmp.path().join("files")), backend.clone()).unwrap();
    let old = objects.upload("file", b"old".as_slice()).unwrap();
    let staged = tmp.path().join("fetched");
    std::fs::copy(objects.path(old.digest, 0), &staged).unwrap();
    let latest = objects.upload("file", b"new".as_slice()).unwrap();
    objects.publish_fetched(&staged, &old).unwrap();
    assert_eq!(objects.metadata("file").unwrap(), Some(latest));
    let mut out = Vec::new();
    objects.read("file", &mut out).unwrap();
    assert_eq!(out, b"new");
    objects.delete("file").unwrap();
    objects.publish_fetched(&staged, &old).unwrap();
    assert_eq!(
        objects.availability("file").unwrap(),
        Availability::Unavailable
    );
    assert!(objects.metadata("file").unwrap().unwrap().deleted);
    assert!(!backend.keys(b"08/files/meta/").unwrap().is_empty());
    backend.store.close().unwrap();
}

#[test]
fn incremental_upload_download_pins_and_shared_limits() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FileBackend::open(&tmp.path().join("spool"), 1, 2));
    let mut cfg = config(&tmp.path().join("objects"));
    cfg.max_staging_bytes = 1 << 20;
    let objects = Arc::new(Objects::open(cfg, backend.clone()).unwrap());
    let old = objects.upload("same", b"old".as_slice()).unwrap();
    let mut upload = objects.begin_upload("same").unwrap();
    let bytes = content(200001);
    for chunk in bytes.chunks(7919) {
        upload.append(chunk).unwrap();
    }
    assert_eq!(objects.metadata("same").unwrap().unwrap(), old);
    objects.collect(Duration::ZERO).unwrap();
    let m = objects.finish_upload(upload).unwrap();
    assert_eq!(m.bytes, bytes.len() as u64);
    let mut download = objects.download("same").unwrap();
    objects.upload("same", b"replacement".as_slice()).unwrap();
    objects.collect(Duration::ZERO).unwrap();
    let mut out = Vec::new();
    loop {
        let (offset, chunk, eof) = download.next(789).unwrap();
        assert_eq!(offset, out.len() as u64);
        out.extend(chunk);
        if eof {
            break;
        }
    }
    assert_eq!(out, bytes);
    drop(download);
    let mut abandoned = objects.begin_upload("aborted").unwrap();
    abandoned.append(b"secret").unwrap();
    drop(abandoned);
    assert!(objects.metadata("aborted").unwrap().is_none());
    let mut large = objects.begin_upload("large").unwrap();
    assert!(large.append(&vec![0; 1 << 20]).is_err());
    drop(large);
    let empty = objects.begin_upload("empty").unwrap();
    objects.finish_upload(empty).unwrap();
    assert!(objects.download("empty").unwrap().next(1).unwrap().2);
    assert!(objects.list_bounded(1).is_err());
    let m = objects.metadata("same").unwrap().unwrap();
    std::fs::remove_file(objects.path(m.digest, m.generation)).unwrap();
    assert!(objects.download("same").is_err());
    backend.store.close().unwrap();
}

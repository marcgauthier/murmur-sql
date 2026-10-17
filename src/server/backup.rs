//! Remote-backup file handling for `MURMUR BACKUP ...` commands.
//!
//! Engine backups are directories, but the wire protocol moves single files.
//! `MURMUR BACKUP SAVE` therefore packs the engine checkpoint directory into
//! one archive file; `MURMUR RESTORE` unpacks it and swaps it into place.
//! Uploads land as hidden `.part` files and are renamed into place by FINISH,
//! so interrupted transfers never look like restorable backups.
//!
//! Archive format (all integers big-endian u64):
//! `MMBK01\n`, then per file: name length, `/`-separated relative name,
//! content length, content bytes. Only regular files are stored; unpacking
//! recreates parent directories.

use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;

/// Suffix given to archives written by `MURMUR BACKUP SAVE`.
pub(crate) const ARCHIVE_SUFFIX: &str = ".murmur-backup";
/// Default `DOWNLOAD` chunk when the client omits LIMIT.
pub(crate) const DOWNLOAD_CHUNK_DEFAULT: u64 = 1 << 20;
/// Refuse to unpack entries beyond these bounds (corrupt length prefixes).
const MAX_NAME_LEN: u64 = 4096;
const MAX_ENTRIES: u64 = 1 << 20;
const MAGIC: &[u8; 7] = b"MMBK01\n";

#[derive(Debug)]
pub(crate) enum BackupError {
    Io(std::io::Error),
    Corrupt(String),
    Invalid(String),
    Exists(String),
    Missing(String),
}

impl BackupError {
    pub(crate) fn user_error(self) -> pgwire::error::PgWireError {
        match self {
            BackupError::Io(e) => crate::server::query::user_error(
                "58000",
                format!("backup storage error: {e}"),
            ),
            BackupError::Corrupt(msg) => {
                crate::server::query::user_error("XX001", format!("corrupt backup file: {msg}"))
            }
            BackupError::Invalid(msg) => crate::server::query::user_error("22023", msg),
            BackupError::Exists(msg) => crate::server::query::user_error("23505", msg),
            BackupError::Missing(msg) => crate::server::query::user_error("58P01", msg),
        }
    }
}

impl From<std::io::Error> for BackupError {
    fn from(error: std::io::Error) -> Self {
        BackupError::Io(error)
    }
}

/// Client-visible backup names: no directories, no hidden files, no quoting
/// hazards. Rejects path separators, parent references, and anything outside
/// a conservative portable charset.
pub(crate) fn validate_name(name: &str) -> Result<(), BackupError> {
    let ok = !name.is_empty()
        && name.len() <= 255
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(BackupError::Invalid(format!(
            "invalid backup name {name:?}: use letters, digits, '.', '_' or '-', no directories"
        )))
    }
}

/// `murmur-YYYYMMDD-HHMMSS.murmur-backup` in UTC.
pub(crate) fn datetime_name(now: SystemTime) -> String {
    format!("murmur-{}{ARCHIVE_SUFFIX}", utc_compact(now))
}

/// `YYYYMMDD-HHMMSS` in UTC for datetime file and directory names.
pub(crate) fn utc_compact(now: SystemTime) -> String {
    let secs = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (year, month, day, hour, minute, second) = split_utc(secs);
    format!("{year:04}{month:02}{day:02}-{hour:02}{minute:02}{second:02}")
}

/// Create a file for backup data, readable only by the owner on unix. The
/// mode is set at creation and tightened right after, so archives and
/// staging files never depend on the process umask.
pub(crate) fn create_private_file(path: &Path) -> std::io::Result<std::fs::File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        Ok(file)
    }
    #[cfg(not(unix))]
    {
        std::fs::File::create(path)
    }
}

/// `create_dir_all`, then best-effort owner-only (0700) on unix.
pub(crate) fn create_private_dir(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700));
    }
    Ok(())
}

/// Fresh hidden staging directory: `.<tag>-<pid>-<nanos>`, bumped on collision.
pub(crate) fn staging_dir(dir: &Path, tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let base = format!(".{tag}-{}-{nanos}", std::process::id());
    let mut candidate = dir.join(&base);
    for n in 2u32.. {
        if !candidate.exists() {
            return candidate;
        }
        candidate = dir.join(format!("{base}-{n}"));
    }
    unreachable!("u32 namespaces do not exhaust")
}

/// `YYYY-MM-DDTHH:MM:SSZ` in UTC for LIST output.
pub(crate) fn format_utc(now: SystemTime) -> String {
    let secs = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (year, month, day, hour, minute, second) = split_utc(secs);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Split unix seconds into UTC calendar fields (proleptic Gregorian).
fn split_utc(secs: u64) -> (i64, u32, u32, u32, u32, u32) {
    let days = (secs / 86_400) as i64;
    let clock = secs % 86_400;
    // Howard Hinnant's days-to-civil algorithm.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    if month <= 2 {
        year += 1;
    }
    (
        year,
        month,
        day,
        (clock / 3_600) as u32,
        ((clock % 3_600) / 60) as u32,
        (clock % 60) as u32,
    )
}

/// Pick `dir/base`, adding `-2`, `-3`, ... before the suffix on collision.
pub(crate) fn unique_path(dir: &Path, base: &str) -> PathBuf {
    let candidate = dir.join(base);
    if !candidate.exists() {
        return candidate;
    }
    let (stem, suffix) = match base.rsplit_once('.') {
        Some((stem, suffix)) => (stem, format!(".{suffix}")),
        None => (base, String::new()),
    };
    for n in 2u32.. {
        let candidate = dir.join(format!("{stem}-{n}{suffix}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    unreachable!("u32 namespaces do not exhaust")
}

pub(crate) struct BackupEntry {
    pub name: String,
    pub size: u64,
    pub modified: String,
}

/// List restorable files: regular, non-hidden files sorted by name. A
/// missing directory lists as empty; nothing is created by a read.
pub(crate) fn list_entries(dir: &Path) -> Result<Vec<BackupEntry>, BackupError> {
    let mut entries = Vec::new();
    let read = match std::fs::read_dir(dir) {
        Ok(read) => read,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(entries),
        Err(e) => return Err(BackupError::Io(e)),
    };
    for entry in read {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        let meta = entry.metadata()?;
        if !meta.is_file() {
            continue;
        }
        entries.push(BackupEntry {
            name,
            size: meta.len(),
            modified: format_utc(meta.modified().unwrap_or(SystemTime::UNIX_EPOCH)),
        });
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(entries)
}

/// Resolve a validated client name to a regular, non-hidden file.
pub(crate) fn resolve_file(dir: &Path, name: &str) -> Result<PathBuf, BackupError> {
    validate_name(name)?;
    let path = dir.join(name);
    let meta = std::fs::metadata(&path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            BackupError::Missing(format!("no backup file {name:?}"))
        } else {
            BackupError::Io(e)
        }
    })?;
    if !meta.is_file() {
        return Err(BackupError::Missing(format!("no backup file {name:?}")));
    }
    Ok(path)
}

pub(crate) struct Chunk {
    pub total: u64,
    pub data: Vec<u8>,
}

/// Read `limit` bytes at `offset` from a backup file.
pub(crate) fn read_chunk(
    dir: &Path,
    name: &str,
    offset: u64,
    limit: u64,
) -> Result<Chunk, BackupError> {
    use std::io::{Seek, SeekFrom};
    let path = resolve_file(dir, name)?;
    let mut file = std::fs::File::open(&path)?;
    let total = file.metadata()?.len();
    let mut data = Vec::new();
    if offset < total {
        file.seek(SeekFrom::Start(offset))?;
        let mut capped = file.take(limit.min(total - offset));
        capped.read_to_end(&mut data)?;
    }
    Ok(Chunk { total, data })
}

fn part_path(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!(".{name}.part"))
}

/// Start (or restart) an upload, truncating any partial state.
pub(crate) fn begin_upload(dir: &Path, name: &str) -> Result<(), BackupError> {
    validate_name(name)?;
    create_private_dir(dir)?;
    create_private_file(&part_path(dir, name))?;
    Ok(())
}

/// Decode one base64 chunk and append it to the in-progress upload.
pub(crate) fn append_upload(dir: &Path, name: &str, chunk: &str) -> Result<u64, BackupError> {
    use base64::Engine;
    validate_name(name)?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(chunk.trim())
        .map_err(|_| BackupError::Invalid("upload chunk is not valid base64".to_string()))?;
    let path = part_path(dir, name);
    let mut part = std::fs::OpenOptions::new().append(true).open(&path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            BackupError::Invalid(format!("no upload in progress for {name:?}; send BEGIN first"))
        } else {
            BackupError::Io(e)
        }
    })?;
    part.write_all(&bytes)?;
    Ok(part.metadata()?.len())
}

/// Atomically publish a finished upload. Refuses to overwrite an existing file.
pub(crate) fn finish_upload(dir: &Path, name: &str) -> Result<u64, BackupError> {
    validate_name(name)?;
    let part = part_path(dir, name);
    let size = std::fs::metadata(&part)
        .map(|m| m.len())
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                BackupError::Invalid(format!("no upload in progress for {name:?}; send BEGIN first"))
            } else {
                BackupError::Io(e)
            }
        })?;
    let dest = dir.join(name);
    if dest.exists() {
        return Err(BackupError::Exists(format!(
            "backup file {name:?} already exists"
        )));
    }
    std::fs::rename(&part, &dest)?;
    Ok(size)
}

/// Discard an in-progress upload; missing partial state is fine.
pub(crate) fn abort_upload(dir: &Path, name: &str) -> Result<(), BackupError> {
    validate_name(name)?;
    match std::fs::remove_file(part_path(dir, name)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(BackupError::Io(e)),
    }
}

/// Pack every regular file under `src` into one archive file at `dest`.
/// Returns the archive size. Streams file contents; memory stays flat.
pub(crate) fn pack_dir(src: &Path, dest: &Path) -> Result<u64, BackupError> {
    let mut files = Vec::new();
    collect_files(src, src, &mut files)?;
    files.sort();
    if let Some(parent) = dest.parent() {
        create_private_dir(parent)?;
    }
    let mut out = std::io::BufWriter::new(create_private_file(dest)?);
    out.write_all(MAGIC)?;
    for relative in &files {
        let name = relative.to_string_lossy().replace('\\', "/");
        let content = std::fs::File::open(src.join(relative))?;
        let len = content.metadata()?.len();
        out.write_all(&(name.len() as u64).to_be_bytes())?;
        out.write_all(name.as_bytes())?;
        out.write_all(&len.to_be_bytes())?;
        let mut content = std::io::BufReader::new(content);
        std::io::copy(&mut content, &mut out)?;
    }
    out.flush()?;
    Ok(std::fs::metadata(dest)?.len())
}

fn collect_files(
    root: &Path,
    dir: &Path,
    files: &mut Vec<PathBuf>,
) -> Result<(), BackupError> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        // Follow symlinks: same-filesystem engine checkpoints use hardlinks,
        // which already read as regular files.
        let meta = std::fs::metadata(&path)?;
        if meta.is_dir() {
            collect_files(root, &path, files)?;
        } else if meta.is_file() {
            files.push(path.strip_prefix(root).unwrap_or(&path).to_path_buf());
        }
    }
    Ok(())
}

/// Unpack an archive into `dest`, creating it. Callers pass fresh staging
/// directories. Refuses path escapes, truncated entries, and absurd entry
/// counts from corrupt length prefixes.
pub(crate) fn unpack_archive(file: &Path, dest: &Path) -> Result<(), BackupError> {
    let mut input = std::io::BufReader::new(std::fs::File::open(file)?);
    let mut magic = [0u8; 7];
    input.read_exact(&mut magic).map_err(|_| {
        BackupError::Corrupt("archive is truncated or not a Murmur backup".to_string())
    })?;
    if &magic != MAGIC {
        return Err(BackupError::Corrupt(
            "not a Murmur backup archive (bad magic)".to_string(),
        ));
    }
    create_private_dir(dest)?;
    for _ in 0..MAX_ENTRIES {
        let name_len = match read_u64(&mut input)? {
            Some(len) => len,
            None => return Ok(()),
        };
        if name_len == 0 || name_len > MAX_NAME_LEN {
            return Err(BackupError::Corrupt("bad entry name length".to_string()));
        }
        let mut name = vec![0u8; name_len as usize];
        input.read_exact(&mut name).map_err(|_| {
            BackupError::Corrupt("truncated entry name".to_string())
        })?;
        let relative = checked_relative(&name)?;
        let content_len = read_u64(&mut input)?.ok_or_else(|| {
            BackupError::Corrupt("truncated entry length".to_string())
        })?;
        let path = dest.join(&relative);
        if let Some(parent) = path.parent() {
            create_private_dir(parent)?;
        }
        let mut out = create_private_file(&path)?;
        let mut capped = input.by_ref().take(content_len);
        let copied = std::io::copy(&mut capped, &mut out)?;
        if copied != content_len {
            return Err(BackupError::Corrupt(format!(
                "truncated entry {}",
                relative.display()
            )));
        }
        out.sync_all()?;
    }
    Err(BackupError::Corrupt("too many entries".to_string()))
}

/// Read one u64, or `Ok(None)` on clean EOF before any byte.
fn read_u64(input: &mut impl Read) -> Result<Option<u64>, BackupError> {
    let mut buf = [0u8; 8];
    match input.read_exact(&mut buf) {
        Ok(()) => Ok(Some(u64::from_be_bytes(buf))),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            if buf == [0u8; 8] {
                Ok(None)
            } else {
                Err(BackupError::Corrupt("truncated length prefix".to_string()))
            }
        }
        Err(e) => Err(BackupError::Io(e)),
    }
}

/// Reject absolute paths, parent escapes, and non-UTF8 entry names.
fn checked_relative(raw: &[u8]) -> Result<PathBuf, BackupError> {
    let name = std::str::from_utf8(raw)
        .map_err(|_| BackupError::Corrupt("entry name is not UTF-8".to_string()))?;
    if name.is_empty() {
        return Err(BackupError::Corrupt("empty entry name".to_string()));
    }
    let mut clean = PathBuf::new();
    for component in Path::new(name).components() {
        match component {
            Component::Normal(part) => clean.push(part),
            _ => {
                return Err(BackupError::Corrupt(format!(
                    "entry escapes archive: {name:?}"
                )));
            }
        }
    }
    if clean.as_os_str().is_empty() {
        return Err(BackupError::Corrupt("empty entry name".to_string()));
    }
    Ok(clean)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_dates_cover_epoch_day_and_leap_day() {
        assert_eq!(format_utc(SystemTime::UNIX_EPOCH), "1970-01-01T00:00:00Z");
        let day = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(86_400 + 3_661);
        assert_eq!(format_utc(day), "1970-01-02T01:01:01Z");
        // 2000-02-29T00:00:00Z.
        let leap = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(951_782_400);
        assert_eq!(format_utc(leap), "2000-02-29T00:00:00Z");
        assert_eq!(
            datetime_name(leap),
            format!("murmur-20000229-000000{ARCHIVE_SUFFIX}")
        );
    }

    #[test]
    fn names_reject_traversal_and_hidden_files() {
        assert!(validate_name("murmur-20261016-120000.murmur-backup").is_ok());
        assert!(validate_name("a1._-b").is_ok());
        for bad in ["", ".hidden", ".a.part", "../x", "a/b", "a\\b", "..", "a b", "a'b", "x".repeat(256).as_str()] {
            assert!(validate_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn unique_names_gain_numeric_suffixes() {
        let dir = std::env::temp_dir().join(format!("murmur-backup-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let first = unique_path(&dir, "murmur-20261016-120000.murmur-backup");
        assert_eq!(first.file_name().unwrap(), "murmur-20261016-120000.murmur-backup");
        std::fs::write(&first, b"x").unwrap();
        let second = unique_path(&dir, "murmur-20261016-120000.murmur-backup");
        assert_eq!(second.file_name().unwrap(), "murmur-20261016-120000-2.murmur-backup");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn pack_unpack_roundtrip_preserves_tree() {
        let dir = std::env::temp_dir().join(format!("murmur-pack-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let src = dir.join("src");
        std::fs::create_dir_all(src.join("sub")).unwrap();
        std::fs::write(src.join("a.txt"), b"alpha").unwrap();
        std::fs::write(src.join("sub").join("b.bin"), &[0u8, 1, 2, 3]).unwrap();
        let archive = dir.join("test.murmur-backup");
        let size = pack_dir(&src, &archive).unwrap();
        assert!(size > 9);
        let dest = dir.join("dest");
        unpack_archive(&archive, &dest).unwrap();
        assert_eq!(std::fs::read(dest.join("a.txt")).unwrap(), b"alpha");
        assert_eq!(std::fs::read(dest.join("sub").join("b.bin")).unwrap(), [0u8, 1, 2, 3]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn unpack_rejects_bad_magic_truncation_and_escapes() {
        let dir = std::env::temp_dir().join(format!("murmur-corrupt-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let bad = dir.join("bad.murmur-backup");
        std::fs::write(&bad, b"not an archive at all........").unwrap();
        assert!(matches!(
            unpack_archive(&bad, &dir.join("o1")),
            Err(BackupError::Corrupt(_))
        ));
        // Truncated after a valid name header.
        let mut truncated = MAGIC.to_vec();
        truncated.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 3, b'a', b'b']);
        std::fs::write(&bad, &truncated).unwrap();
        assert!(matches!(
            unpack_archive(&bad, &dir.join("o2")),
            Err(BackupError::Corrupt(_))
        ));
        // Parent escape.
        let mut escape = MAGIC.to_vec();
        escape.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 6, b'.', b'.', b'/', b'.', b'.', b'/', b'x']);
        escape.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1, b'z']);
        std::fs::write(&bad, &escape).unwrap();
        assert!(matches!(
            unpack_archive(&bad, &dir.join("o3")),
            Err(BackupError::Corrupt(_))
        ));
        assert!(!dir.join("x").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn backup_files_and_staging_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("murmur-perm-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        // SAVE path: the packed archive is 0600.
        let src = dir.join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.txt"), b"alpha").unwrap();
        let backups = dir.join("backups");
        let archive = backups.join("test.murmur-backup");
        pack_dir(&src, &archive).unwrap();
        assert_eq!(std::fs::metadata(&archive).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(std::fs::metadata(&backups).unwrap().permissions().mode() & 0o777, 0o700);
        // UPLOAD path: the finished file is 0600.
        begin_upload(&backups, "up.murmur-backup").unwrap();
        use base64::Engine;
        let chunk = base64::engine::general_purpose::STANDARD.encode(b"hello");
        append_upload(&backups, "up.murmur-backup", &chunk).unwrap();
        finish_upload(&backups, "up.murmur-backup").unwrap();
        assert_eq!(
            std::fs::metadata(backups.join("up.murmur-backup")).unwrap().permissions().mode() & 0o777,
            0o600
        );
        // RESTORE path: unpacked staging is 0600 files under 0700 dirs.
        let dest = dir.join("staging");
        unpack_archive(&archive, &dest).unwrap();
        assert_eq!(std::fs::metadata(&dest).unwrap().permissions().mode() & 0o777, 0o700);
        assert_eq!(std::fs::metadata(dest.join("a.txt")).unwrap().permissions().mode() & 0o777, 0o600);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn upload_lifecycle_supports_chunks_and_refuses_overwrite() {
        use base64::Engine;
        let dir = std::env::temp_dir().join(format!("murmur-upload-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        begin_upload(&dir, "up.murmur-backup").unwrap();
        assert!(list_entries(&dir).unwrap().is_empty());
        let chunk = base64::engine::general_purpose::STANDARD.encode(b"hello ");
        append_upload(&dir, "up.murmur-backup", &chunk).unwrap();
        let chunk = base64::engine::general_purpose::STANDARD.encode(b"world");
        assert_eq!(append_upload(&dir, "up.murmur-backup", &chunk).unwrap(), 11);
        assert!(append_upload(&dir, "up.murmur-backup", "!!!").is_err());
        assert!(append_upload(&dir, "missing.murmur-backup", &chunk).is_err());
        assert_eq!(finish_upload(&dir, "up.murmur-backup").unwrap(), 11);
        assert_eq!(std::fs::read(dir.join("up.murmur-backup")).unwrap(), b"hello world");
        // A second file under the same name is refused, not overwritten.
        begin_upload(&dir, "up.murmur-backup").unwrap();
        append_upload(&dir, "up.murmur-backup", &chunk).unwrap();
        assert!(matches!(
            finish_upload(&dir, "up.murmur-backup"),
            Err(BackupError::Exists(_))
        ));
        abort_upload(&dir, "up.murmur-backup").unwrap();
        abort_upload(&dir, "up.murmur-backup").unwrap();
        let chunk = read_chunk(&dir, "up.murmur-backup", 6, 100).unwrap();
        assert_eq!(chunk.total, 11);
        assert_eq!(chunk.data, b"world");
        let past = read_chunk(&dir, "up.murmur-backup", 99, 10).unwrap();
        assert!(past.data.is_empty());
        assert!(read_chunk(&dir, "absent.murmur-backup", 0, 10).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

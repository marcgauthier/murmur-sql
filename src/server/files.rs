//! PostgreSQL file commands. Durable metadata and payloads remain outside SQLite.
#[cfg(feature = "replication")]
use super::query::ColInfo;
use super::{commands::CommandOutput, query::user_error};
use murmur::Value;
use pgwire::{api::Type, error::PgWireResult};
use std::collections::VecDeque;

fn syntax() -> pgwire::error::PgWireError {
    user_error("42601", "invalid MURMUR FILE command".into())
}
#[derive(Clone, Debug)]
enum Arg {
    Literal(String),
    Param(usize),
}
#[derive(Clone, Debug)]
pub(crate) struct Request {
    pub op: String,
    args: Vec<(Arg, Type)>,
    pub limit: usize,
}
impl Request {
    pub fn types(&self) -> PgWireResult<Vec<Type>> {
        let mut types = Vec::new();
        for (arg, ty) in &self.args {
            if let Arg::Param(i) = arg {
                if *i > 64 {
                    return Err(syntax());
                }
                types.resize(types.len().max(*i), Type::UNKNOWN);
                if types[*i - 1] != Type::UNKNOWN && types[*i - 1] != *ty {
                    return Err(user_error("42P08", "inconsistent parameter types".into()));
                }
                types[*i - 1] = ty.clone();
            }
        }
        if types.contains(&Type::UNKNOWN) {
            return Err(syntax());
        }
        Ok(types)
    }
    fn values(&self, params: &[Value]) -> PgWireResult<Vec<Value>> {
        self.args
            .iter()
            .map(|(arg, ty)| match arg {
                Arg::Param(i) => params
                    .get(i - 1)
                    .cloned()
                    .filter(|v| {
                        matches!(
                            (v, ty),
                            (Value::Text(_), &Type::TEXT) | (Value::Blob(_), &Type::BYTEA)
                        )
                    })
                    .ok_or_else(|| {
                        user_error(
                            "22023",
                            "file arguments must be non-null and have the declared type".into(),
                        )
                    }),
                Arg::Literal(s) if *ty == Type::BYTEA => {
                    let hex = s.strip_prefix("\\x").ok_or_else(syntax)?;
                    if !hex.len().is_multiple_of(2) || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                        return Err(syntax());
                    }
                    Ok(Value::Blob(
                        hex.as_bytes()
                            .chunks_exact(2)
                            .map(|p| {
                                u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap()
                            })
                            .collect(),
                    ))
                }
                Arg::Literal(s) => Ok(Value::Text(s.clone())),
            })
            .collect()
    }
    pub fn columns(&self) -> Vec<(&'static str, Type)> {
        match self.op.as_str() {
            "LIST" | "SEARCH" => vec![("cursor", Type::TEXT)],
            "NEXT" | "UPLOAD FINISH" => metadata_columns(),
            "UPLOAD BEGIN" => vec![("upload", Type::TEXT)],
            "DOWNLOAD" => vec![
                ("download", Type::TEXT),
                ("name", Type::TEXT),
                ("size", Type::INT8),
            ],
            "DOWNLOAD NEXT" => vec![
                ("offset", Type::INT8),
                ("total", Type::INT8),
                ("chunk", Type::BYTEA),
                ("eof", Type::BOOL),
            ],
            _ => vec![],
        }
    }
}
fn metadata_columns() -> Vec<(&'static str, Type)> {
    vec![
        ("name", Type::TEXT),
        ("size", Type::INT8),
        ("modified_ms", Type::INT8),
        ("sha256", Type::TEXT),
        ("local", Type::BOOL),
    ]
}
/// Tokenize without substituting parameter values into SQL text.
pub(crate) fn parse(command: &str) -> PgWireResult<Request> {
    let mut chars = command.trim().trim_end_matches(';').chars().peekable();
    let mut words = VecDeque::new();
    while let Some(c) = chars.next() {
        if c.is_whitespace() {
            continue;
        }
        if c == '\'' {
            let mut s = String::new();
            loop {
                match chars.next() {
                    Some('\'') if chars.peek() == Some(&'\'') => {
                        chars.next();
                        s.push('\'');
                    }
                    Some('\'') => break,
                    Some(c) => s.push(c),
                    None => return Err(syntax()),
                }
            }
            words.push_back((s, true));
        } else {
            let mut s = c.to_string();
            while chars.peek().is_some_and(|c| !c.is_whitespace()) {
                s.push(chars.next().unwrap());
            }
            words.push_back((s, false));
        }
    }
    let word = |words: &mut VecDeque<(String, bool)>| -> PgWireResult<String> {
        match words.pop_front() {
            Some((s, false)) => Ok(s.to_ascii_uppercase()),
            _ => Err(syntax()),
        }
    };
    if word(&mut words)? != "FILE" {
        return Err(syntax());
    }
    let mut op = word(&mut words)?;
    if op == "UPLOAD" {
        op.push(' ');
        op.push_str(&word(&mut words)?);
    } else if op == "DOWNLOAD"
        && words
            .front()
            .is_some_and(|(s, q)| !q && s.eq_ignore_ascii_case("NEXT"))
    {
        words.pop_front();
        op.push_str(" NEXT");
    }
    let count = match op.as_str() {
        "LIST" => 0,
        "SEARCH" | "NEXT" | "CLOSE" | "UPLOAD BEGIN" | "UPLOAD FINISH" | "UPLOAD ABORT"
        | "DOWNLOAD" | "DOWNLOAD NEXT" => 1,
        "UPLOAD APPEND" => 2,
        _ => return Err(syntax()),
    };
    let mut args = Vec::new();
    for index in 0..count {
        let (s, quoted) = words.pop_front().ok_or_else(syntax)?;
        let arg = if quoted {
            Arg::Literal(s)
        } else {
            let n = s
                .strip_prefix('$')
                .and_then(|s| s.parse::<usize>().ok())
                .filter(|n| *n > 0)
                .ok_or_else(syntax)?;
            Arg::Param(n)
        };
        args.push((arg, if index == 1 { Type::BYTEA } else { Type::TEXT }));
    }
    let mut limit = if op == "NEXT" { 256 } else { 64 << 10 };
    if !words.is_empty() {
        if op != "NEXT" && op != "DOWNLOAD NEXT" {
            return Err(syntax());
        }
        if word(&mut words)? != "LIMIT" {
            return Err(syntax());
        }
        let (n, q) = words.pop_front().ok_or_else(syntax)?;
        if q {
            return Err(syntax());
        }
        limit = n.parse().map_err(|_| syntax())?;
    }
    if !words.is_empty() {
        return Err(syntax());
    }
    let max = if op == "NEXT" { 1000 } else { 1 << 20 };
    if limit == 0 || limit > max {
        return Err(user_error(
            "22023",
            "file page/chunk limit is out of range".into(),
        ));
    }
    let req = Request { op, args, limit };
    req.types()?;
    Ok(req)
}
pub(crate) fn is_file(command: &str) -> bool {
    command
        .split_whitespace()
        .next()
        .is_some_and(|s| s.eq_ignore_ascii_case("FILE"))
}
#[cfg(feature = "replication")]
fn rows(req: &Request, rows: Vec<Vec<Value>>) -> CommandOutput {
    CommandOutput::Rows {
        columns: req
            .columns()
            .into_iter()
            .map(|(name, ty)| ColInfo {
                name: name.into(),
                decltype: Some(
                    match ty {
                        Type::INT8 => "INTEGER",
                        Type::BYTEA => "BLOB",
                        Type::BOOL => "MURMUR_BOOL",
                        _ => "TEXT",
                    }
                    .into(),
                ),
            })
            .collect(),
        rows,
    }
}

#[cfg(not(feature = "replication"))]
pub(crate) fn execute(
    _state: &super::ServerState,
    _addr: std::net::SocketAddr,
    _user: &str,
    req: Request,
    params: &[Value],
) -> PgWireResult<CommandOutput> {
    req.values(params)?;
    let _ = req.limit;
    Err(user_error(
        "0A000",
        "file storage requires the replication feature".into(),
    ))
}

#[cfg(feature = "replication")]
mod enabled {
    use super::*;
    use murmur::replication::files::{Download, Objects, Upload};
    use std::{
        collections::HashMap,
        net::SocketAddr,
        sync::Arc,
        time::{Duration, Instant},
    };
    const SESSION_BYTES: usize = 16 << 20;
    const GLOBAL_BYTES: usize = 128 << 20;
    const IDLE: Duration = Duration::from_secs(300);
    enum Handle {
        List {
            rows: VecDeque<Vec<Value>>,
            bytes: usize,
        },
        Upload {
            objects: Arc<Objects>,
            upload: Upload,
        },
        Download(Download),
    }
    struct Entry {
        addr: SocketAddr,
        user: String,
        touched: Instant,
        handle: Handle,
    }
    #[derive(Default)]
    pub(crate) struct Handles {
        closing: bool,
        entries: HashMap<String, Entry>,
    }
    impl Handles {
        pub fn expire(&mut self) {
            self.entries.retain(|_, e| e.touched.elapsed() < IDLE);
        }
        pub fn disconnect(&mut self, addr: SocketAddr) {
            self.entries.retain(|_, e| e.addr != addr);
        }
        pub fn clear(&mut self) {
            self.entries.clear();
        }
        pub fn shutdown(&mut self) {
            self.closing = true;
            self.clear();
        }
        fn usage(&self, addr: Option<SocketAddr>) -> usize {
            self.entries
                .values()
                .filter(|e| addr.is_none_or(|a| a == e.addr))
                .map(|e| match &e.handle {
                    Handle::List { bytes, .. } => *bytes,
                    _ => 0,
                })
                .sum()
        }
        fn insert(&mut self, addr: SocketAddr, user: &str, handle: Handle) -> PgWireResult<String> {
            if self.entries.len() >= 1024
                || self.entries.values().filter(|e| e.addr == addr).count() >= 16
            {
                return Err(user_error("53400", "too many file handles".into()));
            }
            let id = format!("{:032x}", rand::random::<u128>());
            self.entries.insert(
                id.clone(),
                Entry {
                    addr,
                    user: user.into(),
                    touched: Instant::now(),
                    handle,
                },
            );
            Ok(id)
        }
        fn get(&mut self, id: &str, addr: SocketAddr, user: &str) -> PgWireResult<&mut Handle> {
            let e = self
                .entries
                .get_mut(id)
                .filter(|e| e.addr == addr && e.user == user)
                .ok_or_else(|| {
                    user_error(
                        "34000",
                        "file handle is unknown, expired, or belongs to another connection".into(),
                    )
                })?;
            e.touched = Instant::now();
            Ok(&mut e.handle)
        }
    }
    fn storage(e: murmur::replication::Error) -> pgwire::error::PgWireError {
        let code = match &e {
            murmur::replication::Error::Unavailable(_) => "55000",
            murmur::replication::Error::Overloaded => "53400",
            murmur::replication::Error::Invalid(_) => "22023",
            _ => "58030",
        };
        user_error(code, e.to_string())
    }
    fn metadata(
        objects: &Objects,
        m: &murmur::replication::files::Metadata,
    ) -> PgWireResult<Vec<Value>> {
        Ok(vec![
            Value::Text(m.name.clone()),
            Value::Integer(
                i64::try_from(m.bytes)
                    .map_err(|_| user_error("22003", "file size overflow".into()))?,
            ),
            Value::Integer(
                i64::try_from(m.millis)
                    .map_err(|_| user_error("22003", "file timestamp overflow".into()))?,
            ),
            Value::Text(murmur::replication::hex(&m.digest)),
            Value::Integer(objects.path(m.digest, m.generation).is_file() as i64),
        ])
    }
    // Linear-space wildcard matching, supporting Unicode characters and escaped literals.
    fn like(name: &str, pattern: &str) -> PgWireResult<bool> {
        let mut tokens = Vec::new();
        let mut p = pattern.chars();
        while let Some(c) = p.next() {
            tokens.push(match c {
                '\\' => (
                    p.next()
                        .ok_or_else(|| user_error("22025", "LIKE pattern ends in escape".into()))?,
                    false,
                ),
                '%' | '_' => (c, true),
                _ => (c, false),
            });
        }
        let mut prev = vec![false; tokens.len() + 1];
        prev[0] = true;
        for (i, t) in tokens.iter().enumerate() {
            prev[i + 1] = prev[i] && *t == ('%', true);
        }
        for c in name.chars() {
            let mut next = vec![false; prev.len()];
            for (i, (t, w)) in tokens.iter().enumerate() {
                next[i + 1] = if *w && *t == '%' {
                    next[i] || prev[i + 1]
                } else {
                    prev[i] && (*w && *t == '_' || *t == c)
                };
            }
            prev = next;
        }
        Ok(prev[tokens.len()])
    }
    pub(crate) fn execute(
        state: &super::super::ServerState,
        addr: SocketAddr,
        user: &str,
        req: Request,
        params: &[Value],
    ) -> PgWireResult<CommandOutput> {
        let args = req.values(params)?;
        let text = |i: usize| match &args[i] {
            Value::Text(s) => s.as_str(),
            _ => unreachable!(),
        };
        let epoch = state.file_epoch.load(std::sync::atomic::Ordering::SeqCst);
        let db = state.require_db()?;
        let objects = db
            .replication()
            .and_then(|r| r.files())
            .ok_or_else(|| user_error("55000", "file storage is not configured at open".into()))?;
        let mut handles = state.files.lock().unwrap();
        handles.expire();
        if handles.closing {
            return Err(user_error("08006", "server is shutting down".into()));
        }
        if epoch != state.file_epoch.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(user_error(
                "55000",
                "database restored; retry file command".into(),
            ));
        }
        match req.op.as_str() {
            "LIST" | "SEARCH" => {
                let budget = (SESSION_BYTES.saturating_sub(handles.usage(Some(addr))))
                    .min(GLOBAL_BYTES.saturating_sub(handles.usage(None)));
                let mut output = VecDeque::new();
                let mut bytes = 0usize;
                // Validate even when the store contains no files.
                if req.op == "SEARCH" {
                    like("", text(0))?;
                }
                for m in objects.list_bounded(budget).map_err(storage)? {
                    if req.op == "SEARCH" && !like(&m.name, text(0))? {
                        continue;
                    }
                    bytes = bytes.saturating_add(m.name.len() + 256);
                    output.push_back(metadata(&objects, &m)?);
                }
                let id = handles.insert(
                    addr,
                    user,
                    Handle::List {
                        rows: output,
                        bytes,
                    },
                )?;
                Ok(rows(&req, vec![vec![Value::Text(id)]]))
            }
            "NEXT" => {
                let Handle::List { rows: items, .. } = handles.get(text(0), addr, user)? else {
                    return Err(syntax());
                };
                let n = req.limit.min(items.len());
                let output = items.drain(..n).collect();
                Ok(rows(&req, output))
            }
            "CLOSE" => {
                handles.get(text(0), addr, user)?;
                handles.entries.remove(text(0));
                Ok(CommandOutput::Tag("CLOSE"))
            }
            "UPLOAD BEGIN" => {
                let upload = objects.begin_upload(text(0)).map_err(storage)?;
                let id = handles.insert(addr, user, Handle::Upload { objects, upload })?;
                Ok(rows(&req, vec![vec![Value::Text(id)]]))
            }
            "UPLOAD APPEND" => {
                let Value::Blob(chunk) = &args[1] else {
                    unreachable!()
                };
                if chunk.len() > 1 << 20 {
                    return Err(user_error(
                        "22023",
                        "upload chunks may not exceed 1 MiB".into(),
                    ));
                }
                let Handle::Upload { upload, .. } = handles.get(text(0), addr, user)? else {
                    return Err(syntax());
                };
                if let Err(e) = upload.append(chunk) {
                    handles.entries.remove(text(0));
                    return Err(storage(e));
                }
                Ok(CommandOutput::Tag("UPLOAD"))
            }
            "UPLOAD FINISH" => {
                if !matches!(handles.get(text(0), addr, user)?, Handle::Upload { .. }) {
                    return Err(syntax());
                }
                let Entry {
                    handle: Handle::Upload { objects, upload },
                    ..
                } = handles.entries.remove(text(0)).unwrap()
                else {
                    unreachable!()
                };
                let m = objects.finish_upload(upload).map_err(storage)?;
                Ok(rows(&req, vec![metadata(&objects, &m)?]))
            }
            "UPLOAD ABORT" => {
                if !matches!(handles.get(text(0), addr, user)?, Handle::Upload { .. }) {
                    return Err(syntax());
                }
                handles.entries.remove(text(0));
                Ok(CommandOutput::Tag("UPLOAD"))
            }
            "DOWNLOAD" => {
                let download = objects.download(text(0)).map_err(storage)?;
                let name = download.meta.name.clone();
                let size = i64::try_from(download.meta.bytes)
                    .map_err(|_| user_error("22003", "file size overflow".into()))?;
                let id = handles.insert(addr, user, Handle::Download(download))?;
                Ok(rows(
                    &req,
                    vec![vec![
                        Value::Text(id),
                        Value::Text(name),
                        Value::Integer(size),
                    ]],
                ))
            }
            "DOWNLOAD NEXT" => {
                let Handle::Download(download) = handles.get(text(0), addr, user)? else {
                    return Err(syntax());
                };
                let total = download.meta.bytes;
                match download.next(req.limit) {
                    Ok((offset, chunk, eof)) => {
                        let offset = i64::try_from(offset)
                            .map_err(|_| user_error("22003", "file offset overflow".into()))?;
                        let total = i64::try_from(total)
                            .map_err(|_| user_error("22003", "file size overflow".into()))?;
                        Ok(rows(
                            &req,
                            vec![vec![
                                Value::Integer(offset),
                                Value::Integer(total),
                                Value::Blob(chunk),
                                Value::Integer(eof as i64),
                            ]],
                        ))
                    }
                    Err(e) => {
                        handles.entries.remove(text(0));
                        Err(storage(e))
                    }
                }
            }
            _ => Err(syntax()),
        }
    }
    #[cfg(test)]
    mod tests {
        use super::*;
        #[test]
        fn handle_expiration_disconnect_and_budgets() {
            let addr = "127.0.0.1:9".parse().unwrap();
            let mut h = Handles::default();
            let id = h
                .insert(
                    addr,
                    "a",
                    Handle::List {
                        rows: VecDeque::new(),
                        bytes: 123,
                    },
                )
                .unwrap();
            assert_eq!(h.usage(None), 123);
            assert!(h.get(&id, addr, "b").is_err());
            h.entries.get_mut(&id).unwrap().touched = Instant::now() - IDLE;
            h.expire();
            assert!(h.get(&id, addr, "a").is_err());
            let id = h
                .insert(
                    addr,
                    "a",
                    Handle::List {
                        rows: VecDeque::new(),
                        bytes: 0,
                    },
                )
                .unwrap();
            h.disconnect(addr);
            assert!(h.get(&id, addr, "a").is_err());
        }
        #[test]
        fn patterns() {
            for (n, p, want) in [
                ("a.pdf", "%.pdf", true),
                ("A.pdf", "a%", false),
                ("a_b", "a\\_b", true),
                ("a%b", "a\\%b", true),
                ("é", "_", true),
                ("", "%", true),
                ("axxb", "a%b", true),
            ] {
                assert_eq!(like(n, p).unwrap(), want);
            }
            assert!(like("", "x\\").is_err());
        }
    }
}
#[cfg(feature = "replication")]
pub(crate) use enabled::{Handles, execute};

#[cfg(test)]
mod parser_tests {
    use super::*;
    #[test]
    fn typed_arguments_and_quoted_literals() {
        let r = parse("FILE UPLOAD APPEND $1 $2").unwrap();
        assert_eq!(r.types().unwrap(), vec![Type::TEXT, Type::BYTEA]);
        let r = parse("FILE SEARCH 'a''b$1%'").unwrap();
        assert!(r.types().unwrap().is_empty());
        assert_eq!(r.values(&[]).unwrap(), vec![Value::Text("a'b$1%".into())]);
        for s in [
            "FILE SEARCH $0",
            "FILE SEARCH $2",
            "FILE SEARCH $99999999999999999999999999",
            "FILE UPLOAD APPEND $1 $1",
            "FILE NEXT 'x' LIMIT 0",
            "FILE NEXT 'x' LIMIT 1001",
            "FILE LIST x",
            "FILE SEARCH 'unclosed",
        ] {
            assert!(parse(s).is_err(), "{s}");
        }
    }
}

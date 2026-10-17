//! SQL execution over the Postgres wire protocol, speaking SQLite dialect.
//!
//! Routing per statement:
//!
//! - `MURMUR ...` commands are intercepted (see `commands.rs`).
//! - DDL (`CREATE`/`ALTER`/`DROP`/`REINDEX`) runs through `Database::migrate`
//!   so schema tracking stays authoritative; the write path would deny it.
//! - `BEGIN`/`COMMIT`/`ROLLBACK` (and `START TRANSACTION`/`END`) are accepted
//!   no-ops: every statement already runs in its own atomic transaction.
//! - `SELECT version()` and `SHOW server_version` answer from server constants.
//! - Everything else runs in one engine transaction (autocommit). Statements
//!   returning columns produce row sets; others produce command tags.
//!
//! Parameters use `$N` (rewritten to SQLite `?N`). Multi-statement strings are
//! only accepted on the simple-query path, one response per statement.

use crate::model::Value;
use crate::server::ServerState;
use async_trait::async_trait;
use futures::stream;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use std::sync::Arc;

/// Typed clients decode Execute rows against the Describe response, so the
/// column types returned by describe and execute must agree. Untyped
/// expressions (no SQLite decltype) are probed with a `LIMIT 1` query at
/// describe time and the result is stashed for execute.
pub struct QueryBackend {
    state: Arc<ServerState>,
    describe_cache: Arc<std::sync::Mutex<DescribeCache>>,
}

type CacheKey = (std::net::SocketAddr, u8, String);

#[derive(Default)]
struct DescribeCache {
    map: std::collections::HashMap<CacheKey, Vec<Type>>,
}

impl DescribeCache {
    /// Stash describe types, bounding memory by dropping everything on
    /// overflow (describes are cheap to redo and clients cache them).
    fn store(&mut self, key: CacheKey, types: Vec<Type>) {
        if self.map.len() >= 4096 {
            self.map.clear();
        }
        self.map.insert(key, types);
    }

    fn fetch(&self, key: &CacheKey, columns: usize) -> Option<Vec<Type>> {
        self.map
            .get(key)
            .filter(|types| types.len() == columns)
            .cloned()
    }
}

impl QueryBackend {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self {
            state,
            describe_cache: Arc::new(std::sync::Mutex::new(DescribeCache::default())),
        }
    }

    fn stash(&self, key: CacheKey, types: &[Type]) {
        self.describe_cache
            .lock()
            .unwrap()
            .store(key, types.to_vec());
    }

    fn stashed(
        &self,
        addr: std::net::SocketAddr,
        portal: &Portal<String>,
        columns: usize,
    ) -> Option<Vec<Type>> {
        let cache = self.describe_cache.lock().unwrap();
        cache
            .fetch(&(addr, b'P', portal.name.clone()), columns)
            .or_else(|| cache.fetch(&(addr, b'S', portal.statement.id.clone()), columns))
    }

    fn addr<C: ClientInfo>(client: &C) -> std::net::SocketAddr {
        client.socket_addr()
    }

    fn user<C: ClientInfo>(client: &C) -> String {
        pgwire::api::auth::LoginInfo::from_client_info(client)
            .user()
            .unwrap_or_default()
            .to_owned()
    }

    /// Locked connections may only send `MURMUR UNLOCK ...`.
    fn check_unlocked(
        &self,
        addr: std::net::SocketAddr,
        user: &str,
        sql: &str,
    ) -> PgWireResult<()> {
        if is_unlock_command(sql) || !self.state.is_locked(addr, user) {
            return Ok(());
        }
        Err(user_error(
            "28000",
            "connection is locked; send MURMUR UNLOCK KEY '<key>' first".to_string(),
        ))
    }
}

/// First two words are `MURMUR UNLOCK` (case-insensitive).
fn is_unlock_command(sql: &str) -> bool {
    let mut words = strip_leading_trivia(sql).split_whitespace();
    matches!(
        (words.next(), words.next()),
        (Some(first), Some(second))
            if first.eq_ignore_ascii_case("MURMUR") && second.eq_ignore_ascii_case("UNLOCK")
    )
}

#[async_trait]
impl SimpleQueryHandler for QueryBackend {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let addr = Self::addr(_client);
        let user = Self::user(_client);
        let statements = split_statements(query);
        if statements.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }
        let config = self.state.config_snapshot();
        let mut out = Vec::with_capacity(statements.len());
        for sql in statements {
            self.check_unlocked(addr, &user, &sql)?;
            let state = self.state.clone();
            let config = config.clone();
            let user = user.clone();
            let output = tokio::task::spawn_blocking(move || {
                execute_one(&state, &config, addr, &user, &sql, &[])
            })
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))??;
            out.push(response_for(output, &Format::UnifiedText, None)?);
        }
        Ok(out)
    }
}

#[async_trait]
impl ExtendedQueryHandler for QueryBackend {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser::new())
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // Full result sets are buffered; `_max_rows` cursors are a follow-up.
        let addr = Self::addr(_client);
        let user = Self::user(_client);
        let sql = portal.statement.statement.clone();
        self.check_unlocked(addr, &user, &sql)?;
        let statements = split_statements(&sql);
        if statements.len() > 1 {
            return Err(user_error(
                "42601",
                "extended protocol takes one statement at a time".to_string(),
            ));
        }
        if statements.is_empty() {
            return Ok(Response::EmptyQuery);
        }
        let params = extract_params(portal, &sql)?;
        let config = self.state.config_snapshot();
        let state = self.state.clone();
        let output = tokio::task::spawn_blocking(move || {
            execute_one(&state, &config, addr, &user, &sql, &params)
        })
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))??;
        let stashed = match &output {
            StmtOutput::Rows { columns, .. } => self.stashed(addr, portal, columns.len()),
            _ => None,
        };
        response_for(output, &portal.result_column_format, stashed.as_deref())
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let addr = Self::addr(_client);
        let user = Self::user(_client);
        self.check_unlocked(addr, &user, &stmt.statement)?;
        let supplied: Vec<Type> = stmt
            .parameter_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::UNKNOWN))
            .collect();
        let (param_types, fields, types) = describe_parts(
            &self.state,
            &stmt.statement,
            supplied,
            &Format::UnifiedBinary,
        )
        .await?;
        self.stash((addr, b'S', stmt.id.clone()), &types);
        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let addr = Self::addr(_client);
        let user = Self::user(_client);
        self.check_unlocked(addr, &user, &portal.statement.statement)?;
        let supplied: Vec<Type> = portal
            .statement
            .parameter_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::UNKNOWN))
            .collect();
        let (_, fields, types) = describe_parts(
            &self.state,
            &portal.statement.statement,
            supplied,
            &portal.result_column_format,
        )
        .await?;
        self.stash((addr, b'P', portal.name.clone()), &types);
        Ok(DescribePortalResponse::new(fields))
    }
}

async fn describe_parts(
    state: &ServerState,
    sql: &str,
    mut param_types: Vec<Type>,
    format: &Format,
) -> PgWireResult<(Vec<Type>, Vec<FieldInfo>, Vec<Type>)> {
    if split_statements(sql).is_empty() {
        return Ok((param_types, vec![], vec![]));
    }
    match route_statement(sql) {
        Route::Ddl(_) | Route::Txn(_) => Ok((param_types, vec![], vec![])),
        Route::Murmur(command) => {
            if super::files::is_file(&command) {
                let req = super::files::parse(&command)?;
                let inferred = req.types()?;
                for (i, ty) in param_types.iter().enumerate() {
                    if *ty != Type::UNKNOWN && inferred.get(i) != Some(ty) {
                        return Err(user_error("42804", "incorrect file parameter type".into()));
                    }
                }
                let columns = req.columns();
                let types = columns.iter().map(|(_, ty)| ty.clone()).collect();
                let fields = columns
                    .into_iter()
                    .enumerate()
                    .map(|(i, (name, ty))| {
                        FieldInfo::new(name.into(), None, None, ty, format.format_for(i))
                    })
                    .collect();
                return Ok((inferred, fields, types));
            }
            // Row-returning commands must describe their real columns; the
            // client decodes Execute rows against the Describe response.
            match crate::server::commands::describe_columns(&command) {
                Some(columns) => {
                    let types = columns.iter().map(|(_, ty)| ty.clone()).collect();
                    let fields = columns
                        .into_iter()
                        .enumerate()
                        .map(|(idx, (name, ty))| {
                            FieldInfo::new(name.to_owned(), None, None, ty, format.format_for(idx))
                        })
                        .collect();
                    Ok((param_types, fields, types))
                }
                None => Ok((param_types, vec![], vec![])),
            }
        }
        Route::Version | Route::ShowVersion => {
            let fields = vec![FieldInfo::new(
                "version".to_owned(),
                None,
                None,
                Type::TEXT,
                format.format_for(0),
            )];
            Ok((param_types, fields, vec![Type::TEXT]))
        }
        Route::Sql => {
            let (rewritten, max_param) = rewrite_placeholders(sql)?;
            while param_types.len() < max_param {
                param_types.push(Type::UNKNOWN);
            }
            let db = state.require_db()?;
            // Fill client-unspecified parameter types from context so drivers
            // can encode bound values. Typed clients make inference a no-op;
            // skip the schema fetch and SQL parse then.
            let need_infer = param_types.iter().take(max_param).any(|ty| *ty == Type::UNKNOWN);
            let probe_params: Vec<StoredParam> = param_types
                .iter()
                .take(max_param)
                .map(dummy_param)
                .collect();
            let (columns, sample, schema) = tokio::task::spawn_blocking(
                move || -> std::result::Result<
                    (
                        Vec<ColInfo>,
                        Option<Vec<Value>>,
                        Option<crate::model::Schema>,
                    ),
                    crate::Error,
                > {
                    let schema = if need_infer { Some(db.schema()?) } else { None };
                    // Describe only prepares the statement and reads column
                    // metadata (plus an optional probe SELECT). SELECTs take
                    // the read path; anything else keeps a write transaction
                    // because SQLite authorizes writes at prepare time.
                    let readonly = first_word(&rewritten).eq_ignore_ascii_case("SELECT");
                    let run = move |sql: &crate::Sql| {
                        let stmt = sql.prepare(&rewritten)?;
                        let columns = statement_columns(&stmt);
                        // Untyped columns need a sample row so describe and
                        // execute agree; probe failures fall back to TEXT.
                        let sample = if columns.iter().any(|c| c.decltype.is_none()) {
                            probe_sample(sql, &rewritten, &probe_params)
                        } else {
                            None
                        };
                        Ok((columns, sample))
                    };
                    let (columns, sample) = if readonly {
                        db.read(run)?
                    } else {
                        db.transaction(run)?
                    };
                    Ok((columns, sample, schema))
                },
            )
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?
            .map_err(engine_error)?;
            if need_infer {
                let inferred = crate::server::infer::infer_param_types(
                    &schema.unwrap_or_default(),
                    sql,
                    max_param,
                );
                for (idx, ty) in param_types.iter_mut().enumerate() {
                    if *ty == Type::UNKNOWN {
                        if let Some(inferred_ty) = inferred.get(idx).and_then(Clone::clone) {
                            *ty = inferred_ty;
                        }
                    }
                }
            }
            let types = column_types(&columns, sample.as_deref());
            let out = columns
                .iter()
                .zip(types.iter())
                .enumerate()
                .map(|(idx, (col, ty))| {
                    FieldInfo::new(
                        col.name.clone(),
                        None,
                        None,
                        ty.clone(),
                        format.format_for(idx),
                    )
                })
                .collect();
            Ok((param_types, out, types))
        }
    }
}

/// Derive one wire type per column from SQLite decltypes, filling untyped
/// columns from a sample row (probe at describe, first rows at execute).
// sqlite-vec declares hidden distance/k columns without numeric affinities.
// Origin metadata handles aliases and empty results without guessing by label.
fn statement_columns(stmt: &rusqlite::Statement<'_>) -> Vec<ColInfo> {
    stmt.columns().iter().zip(stmt.columns_with_metadata()).map(|(column, origin)| {
        let vector = origin.database_name() == Some("temp")
            && origin.table_name().is_some_and(|name| name.starts_with("murmur_vec_"));
        let ty = if vector {
            match origin.origin_name() {
                Some("distance") => Some("REAL"),
                Some("k" | "rowid") => Some("INTEGER"),
                _ => Some("BLOB"),
            }
        } else { column.decl_type() };
        ColInfo { name: column.name().into(), decltype: ty.map(str::to_owned) }
    }).collect()
}

fn column_types(columns: &[ColInfo], sample: Option<&[Value]>) -> Vec<Type> {
    columns
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            if col.decltype.is_some() {
                return decltype_to_type(col.decltype.as_deref());
            }
            sample
                .and_then(|row| row.get(idx))
                .and_then(value_type)
                .unwrap_or(Type::TEXT)
        })
        .collect()
}

fn value_type(value: &Value) -> Option<Type> {
    match value {
        Value::Null => None,
        Value::Integer(_) => Some(Type::INT8),
        Value::Real(_) => Some(Type::FLOAT8),
        Value::Text(_) => Some(Type::TEXT),
        Value::Blob(_) => Some(Type::BYTEA),
    }
}

/// Dummy probe bindings by declared parameter type. Never fail the describe.
fn dummy_param(ty: &Type) -> StoredParam {
    match *ty {
        Type::INT2 | Type::INT4 | Type::INT8 => StoredParam::Int(0),
        Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC => StoredParam::Float(0.0),
        Type::BOOL => StoredParam::Bool(false),
        Type::BYTEA => StoredParam::Blob(Vec::new()),
        _ => StoredParam::Text(String::new()),
    }
}

/// Shape of the probe query for `rewritten`, or `None` when probing is
/// impossible (DML with RETURNING, EXPLAIN, side-effecting PRAGMAs).
fn probe_sql(rewritten: &str) -> Option<String> {
    let stripped = strip_leading_trivia(rewritten)
        .trim_end_matches(';')
        .trim_end();
    let (first, rest) = split_first_word(stripped);
    match first.to_ascii_uppercase().as_str() {
        "SELECT" | "WITH" | "VALUES" | "TABLE" => {
            Some(format!("SELECT * FROM ({stripped}) LIMIT 1"))
        }
        "PRAGMA" => {
            // Only the side-effect-free inspection pragmas (the authorizer's
            // own allowlist) run directly as probes.
            let name = split_first_word(rest).0.to_ascii_lowercase();
            let name = name.split('(').next().unwrap_or("");
            match name {
                "table_info" | "table_xinfo" | "index_list" | "index_info" => {
                    Some(stripped.to_owned())
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// First probe row, or `None` when probing is impossible or fails.
fn probe_sample(sql: &crate::Sql, rewritten: &str, params: &[StoredParam]) -> Option<Vec<Value>> {
    let probe = probe_sql(rewritten)?;
    let bound: Vec<Box<dyn rusqlite::ToSql + Send>> =
        params.iter().map(StoredParam::to_sql).collect();
    let refs: Vec<&dyn rusqlite::ToSql> = bound
        .iter()
        .map(|b| b.as_ref() as &dyn rusqlite::ToSql)
        .collect();
    let mut stmt = sql.prepare(&probe).ok()?;
    let width = stmt.column_count();
    let mut cursor = if refs.is_empty() {
        stmt.query(()).ok()?
    } else {
        stmt.query(refs.as_slice()).ok()?
    };
    let row = cursor.next().ok()??;
    let mut values = Vec::with_capacity(width);
    for idx in 0..width {
        values.push(Value::read(row.get_ref(idx).ok()?).ok()?);
    }
    Some(values)
}

/// Output of one executed statement, before wire encoding.
enum StmtOutput {
    Rows {
        columns: Vec<ColInfo>,
        rows: Vec<Vec<Value>>,
    },
    Count {
        verb: String,
        count: usize,
    },
    BareTag(String),
}

#[derive(Clone)]
pub(crate) struct ColInfo {
    pub(crate) name: String,
    pub(crate) decltype: Option<String>,
}

/// Execute one statement on a blocking thread.
fn execute_one(
    state: &ServerState,
    config: &crate::config::Config,
    addr: std::net::SocketAddr,
    user: &str,
    sql: &str,
    params: &[RawParam],
) -> PgWireResult<StmtOutput> {
    match route_statement(sql) {
        Route::Murmur(command) => {
            if super::files::is_file(&command) {
                let req = super::files::parse(&command)?;
                let types = req.types()?;
                if params.len() != types.len() {
                    return Err(user_error(
                        "22023",
                        "incorrect number of file parameters".into(),
                    ));
                }
                let values = params
                    .iter()
                    .zip(&types)
                    .enumerate()
                    .map(|(i, (p, ty))| {
                        if p.declared != Type::UNKNOWN && p.declared != *ty {
                            return Err(user_error(
                                "42804",
                                "incorrect file parameter type".into(),
                            ));
                        }
                        match decode_raw(p, ty, i)? {
                            StoredParam::Text(v) => Ok(Value::Text(v)),
                            StoredParam::Blob(v) => Ok(Value::Blob(v)),
                            StoredParam::Null => Ok(Value::Null),
                            _ => Err(user_error("42804", "incorrect file parameter type".into())),
                        }
                    })
                    .collect::<PgWireResult<Vec<_>>>()?;
                return match super::files::execute(state, addr, user, req, &values)? {
                    super::commands::CommandOutput::Tag(tag) => Ok(StmtOutput::BareTag(tag.into())),
                    super::commands::CommandOutput::Rows { columns, rows } => {
                        Ok(StmtOutput::Rows { columns, rows })
                    }
                };
            }
            if !params.is_empty() {
                return Err(user_error(
                    "42601",
                    "MURMUR commands take no parameters".to_string(),
                ));
            }
            match crate::server::commands::execute(state, config, addr, user, &command)? {
                crate::server::commands::CommandOutput::Tag(tag) => {
                    Ok(StmtOutput::BareTag(tag.to_owned()))
                }
                crate::server::commands::CommandOutput::Rows { columns, rows } => {
                    Ok(StmtOutput::Rows { columns, rows })
                }
            }
        }
        Route::Txn(tag) => {
            if !params.is_empty() {
                return Err(user_error(
                    "42601",
                    "transaction words take no parameters".to_string(),
                ));
            }
            Ok(StmtOutput::BareTag(tag.to_owned()))
        }
        Route::Version | Route::ShowVersion => {
            if !params.is_empty() {
                return Err(user_error(
                    "42601",
                    "version queries take no parameters".to_string(),
                ));
            }
            Ok(StmtOutput::Rows {
                columns: vec![ColInfo {
                    name: "version".to_owned(),
                    decltype: Some("TEXT".to_owned()),
                }],
                rows: vec![vec![Value::Text(format!(
                    "PostgreSQL {}",
                    crate::server::PG_SERVER_VERSION
                ))]],
            })
        }
        Route::Ddl(tag) => {
            if !params.is_empty() {
                return Err(user_error("42601", "DDL takes no parameters".to_string()));
            }
            let db = state.require_db()?;
            db.migrate(crate::Migration::new(sql.to_owned()))
                .map_err(engine_error)?;
            Ok(StmtOutput::BareTag(tag))
        }
        Route::Sql => {
            let db = state.require_db()?;
            let (rewritten, max_param) = rewrite_placeholders(sql)?;
            let mut declared: Vec<Type> = params.iter().map(|p| p.declared.clone()).collect();
            // Fill client-unspecified types by inference so values decode
            // against the same types describe reported. Typed clients make
            // this a no-op; skip the schema fetch and SQL parse then.
            if declared.iter().any(|ty| *ty == Type::UNKNOWN) {
                let schema = db.schema().map_err(engine_error)?;
                let inferred = crate::server::infer::infer_param_types(&schema, sql, max_param);
                for (idx, ty) in declared.iter_mut().enumerate() {
                    if *ty == Type::UNKNOWN {
                        if let Some(inferred_ty) = inferred.get(idx).and_then(Clone::clone) {
                            *ty = inferred_ty;
                        }
                    }
                }
            }
            let decoded = params
                .iter()
                .enumerate()
                .map(|(idx, param)| decode_raw(param, &declared[idx], idx))
                .collect::<PgWireResult<Vec<_>>>()?;
            exec_sql(&db, &rewritten, max_param, &decoded)
        }
    }
}

fn exec_sql(
    db: &crate::Database,
    rewritten: &str,
    max_param: usize,
    params: &[StoredParam],
) -> PgWireResult<StmtOutput> {
    let rewritten = rewritten.to_owned();
    if params.len() < max_param {
        return Err(user_error(
            "22023",
            format!("missing value for parameter ${}", params.len() + 1),
        ));
    }
    if params.len() > max_param && max_param > 0 {
        return Err(user_error(
            "22023",
            format!("too many parameters: statement uses {max_param}"),
        ));
    }
    let bound: Vec<Box<dyn rusqlite::ToSql + Send>> =
        params.iter().map(StoredParam::to_sql).collect();
    let verb = first_word(&rewritten).to_ascii_uppercase();
    // Reads skip the write path (scheduler ticket, BEGIN/COMMIT, capture
    // accounting). The authorizer denies every write under read access, so
    // even a smuggled write fails closed here.
    let readonly = verb == "SELECT";
    let run = move |tx: &crate::Sql| {
        let refs: Vec<&dyn rusqlite::ToSql> = bound
            .iter()
            .map(|b| b.as_ref() as &dyn rusqlite::ToSql)
            .collect();
        let mut stmt = tx.prepare(&rewritten)?;
        if stmt.column_count() == 0 {
            let count = if refs.is_empty() {
                stmt.execute(())?
            } else {
                stmt.execute(refs.as_slice())?
            };
            Ok(StmtOutput::Count { verb, count })
        } else {
            let columns = statement_columns(&stmt);
            let mut rows = Vec::new();
            let mut cursor = if refs.is_empty() {
                stmt.query(())?
            } else {
                stmt.query(refs.as_slice())?
            };
            while let Some(row) = cursor.next()? {
                let mut values = Vec::with_capacity(columns.len());
                for idx in 0..columns.len() {
                    values.push(Value::read(row.get_ref(idx)?)?);
                }
                rows.push(values);
            }
            Ok(StmtOutput::Rows { columns, rows })
        }
    };
    if readonly {
        db.read(run).map_err(engine_error)
    } else {
        db.transaction(run).map_err(engine_error)
    }
}

fn response_for(
    output: StmtOutput,
    format: &Format,
    stashed: Option<&[Type]>,
) -> PgWireResult<Response> {
    match output {
        StmtOutput::BareTag(tag) => Ok(Response::Execution(Tag::new(&tag))),
        StmtOutput::Count { verb, count } => {
            let tag = if verb == "INSERT" {
                Tag::new("INSERT").with_oid(0).with_rows(count)
            } else {
                Tag::new(verb.as_str()).with_rows(count)
            };
            Ok(Response::Execution(tag))
        }
        StmtOutput::Rows { columns, rows, .. } => {
            // Prefer describe-time types: the client decodes against them.
            // Without a describe (simple protocol, or Execute without
            // Describe), infer untyped columns from the first non-null value.
            let types: Vec<Type> = match stashed {
                Some(types) if types.len() == columns.len() => types.to_vec(),
                _ => {
                    let sample: Vec<Value> = (0..columns.len())
                        .map(|idx| {
                            rows.iter()
                                .find_map(|row| match &row[idx] {
                                    Value::Null => None,
                                    value => Some(value.clone()),
                                })
                                .unwrap_or(Value::Null)
                        })
                        .collect();
                    column_types(&columns, Some(&sample))
                }
            };
            let header: Arc<Vec<FieldInfo>> = Arc::new(
                columns
                    .iter()
                    .zip(types.iter())
                    .enumerate()
                    .map(|(idx, (col, ty))| {
                        FieldInfo::new(
                            col.name.clone(),
                            None,
                            None,
                            ty.clone(),
                            format.format_for(idx),
                        )
                    })
                    .collect(),
            );
            let mut encoded = Vec::with_capacity(rows.len());
            let mut encoder = DataRowEncoder::new(header.clone());
            for row in &rows {
                for (value, ty) in row.iter().zip(types.iter()) {
                    encode_value(&mut encoder, ty, value)?;
                }
                encoded.push(Ok(encoder.take_row()));
            }
            Ok(Response::Query(QueryResponse::new(
                header,
                stream::iter(encoded),
            )))
        }
    }
}

enum Route {
    Sql,
    Ddl(String),
    Txn(&'static str),
    Murmur(String),
    Version,
    ShowVersion,
}

fn route_statement(sql: &str) -> Route {
    let stripped = strip_leading_trivia(sql);
    let (first, rest) = split_first_word(stripped);
    match first.to_ascii_uppercase().as_str() {
        "MURMUR" => Route::Murmur(rest.to_owned()),
        "CREATE" | "ALTER" | "DROP" | "REINDEX" => Route::Ddl(ddl_tag(&first, rest)),
        "BEGIN" | "START" | "COMMIT" | "END" | "ROLLBACK" => match txn_tag(&first, rest) {
            Some(tag) => Route::Txn(tag),
            None => Route::Sql,
        },
        "SHOW" => {
            if rest
                .trim()
                .trim_end_matches(';')
                .trim()
                .eq_ignore_ascii_case("server_version")
            {
                Route::ShowVersion
            } else {
                Route::Sql
            }
        }
        "SELECT" => {
            if is_select_version(stripped) {
                Route::Version
            } else {
                Route::Sql
            }
        }
        _ => Route::Sql,
    }
}

fn strip_leading_trivia(mut sql: &str) -> &str {
    loop {
        sql = sql.trim_start();
        if let Some(rest) = sql.strip_prefix("--") {
            sql = match rest.find('\n') {
                Some(idx) => &rest[idx + 1..],
                None => return "",
            };
        } else if let Some(rest) = sql.strip_prefix("/*") {
            match rest.find("*/") {
                Some(idx) => sql = &rest[idx + 2..],
                None => return sql,
            }
        } else {
            return sql;
        }
    }
}

fn split_first_word(sql: &str) -> (String, &str) {
    let end = sql
        .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
        .unwrap_or(sql.len());
    (sql[..end].to_owned(), sql[end..].trim_start())
}

fn first_word(sql: &str) -> String {
    split_first_word(strip_leading_trivia(sql)).0
}

fn ddl_tag(first: &str, rest: &str) -> String {
    let second = split_first_word(rest).0.to_ascii_uppercase();
    match second.as_str() {
        "TABLE" | "INDEX" => format!("{} {second}", first.to_ascii_uppercase()),
        _ => first.to_ascii_uppercase(),
    }
}

/// Accept only bare words (plus optional TRANSACTION/WORK); anything else
/// falls through to SQLite for an honest error.
fn txn_tag(first: &str, rest: &str) -> Option<&'static str> {
    let normalized = rest
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_ascii_uppercase();
    let upper = first.to_ascii_uppercase();
    match (upper.as_str(), normalized.as_str()) {
        ("BEGIN", "" | "TRANSACTION" | "WORK") => Some("BEGIN"),
        ("START", "TRANSACTION") => Some("BEGIN"),
        ("COMMIT", "" | "TRANSACTION" | "WORK") | ("END", "" | "TRANSACTION" | "WORK") => {
            Some("COMMIT")
        }
        ("ROLLBACK", "" | "TRANSACTION" | "WORK") => Some("ROLLBACK"),
        _ => None,
    }
}

fn is_select_version(sql: &str) -> bool {
    let compact: String = sql.chars().filter(|c| !c.is_whitespace()).collect();
    let compact = compact.trim_end_matches(';');
    compact.eq_ignore_ascii_case("selectversion()")
}

/// True when the SQL holds no statement: only whitespace and comments.
fn is_trivia_only(sql: &str) -> bool {
    enum State {
        Normal,
        LineComment,
        BlockComment,
    }
    let mut state = State::Normal;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match state {
            State::Normal => match c {
                '-' if chars.peek() == Some(&'-') => {
                    chars.next();
                    state = State::LineComment;
                }
                '/' if chars.peek() == Some(&'*') => {
                    chars.next();
                    state = State::BlockComment;
                }
                // Quote characters are content; only comments, whitespace
                // and bare semicolons are trivia.
                _ if c.is_whitespace() || c == ';' => {}
                _ => return false,
            },
            State::LineComment => {
                if c == '\n' {
                    state = State::Normal;
                }
            }
            State::BlockComment => {
                if c == '*' && chars.peek() == Some(&'/') {
                    chars.next();
                    state = State::Normal;
                }
            }
        }
    }
    true
}

/// Split a simple-query string on `;` outside quotes and comments.
/// Trivia-only segments are dropped like Postgres skips empty statements.
fn split_statements(sql: &str) -> Vec<String> {
    #[derive(PartialEq)]
    enum State {
        Normal,
        LineComment,
        BlockComment,
        Single,
        Double,
        Backtick,
        Bracket,
    }
    let mut state = State::Normal;
    let mut current = String::new();
    let mut out = Vec::new();
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match state {
            State::Normal => match c {
                '-' if chars.peek() == Some(&'-') => {
                    chars.next();
                    current.push_str("--");
                    state = State::LineComment;
                }
                '/' if chars.peek() == Some(&'*') => {
                    chars.next();
                    current.push_str("/*");
                    state = State::BlockComment;
                }
                '\'' => {
                    current.push(c);
                    state = State::Single;
                }
                '"' => {
                    current.push(c);
                    state = State::Double;
                }
                '`' => {
                    current.push(c);
                    state = State::Backtick;
                }
                '[' => {
                    current.push(c);
                    state = State::Bracket;
                }
                ';' => {
                    push_segment(&mut out, &mut current);
                    current = String::new();
                }
                _ => current.push(c),
            },
            State::LineComment => {
                current.push(c);
                if c == '\n' {
                    state = State::Normal;
                }
            }
            State::BlockComment => {
                current.push(c);
                if c == '*' && chars.peek() == Some(&'/') {
                    current.push(chars.next().unwrap());
                    state = State::Normal;
                }
            }
            State::Single => {
                current.push(c);
                if c == '\'' {
                    if chars.peek() == Some(&'\'') {
                        current.push(chars.next().unwrap());
                    } else {
                        state = State::Normal;
                    }
                }
            }
            State::Double => {
                current.push(c);
                if c == '"' {
                    if chars.peek() == Some(&'"') {
                        current.push(chars.next().unwrap());
                    } else {
                        state = State::Normal;
                    }
                }
            }
            State::Backtick => {
                current.push(c);
                if c == '`' {
                    state = State::Normal;
                }
            }
            State::Bracket => {
                current.push(c);
                if c == ']' {
                    state = State::Normal;
                }
            }
        }
    }
    push_segment(&mut out, &mut current);
    out
}

fn push_segment(out: &mut Vec<String>, current: &str) {
    let trimmed = current.trim();
    if !trimmed.is_empty() && !is_trivia_only(trimmed) {
        out.push(trimmed.to_owned());
    }
}

/// Rewrite `$N` placeholders to SQLite `?N`, skipping quoted regions.
/// Returns the rewritten SQL and the highest parameter index seen.
fn rewrite_placeholders(sql: &str) -> PgWireResult<(String, usize)> {
    let mut rewritten = String::with_capacity(sql.len());
    let mut max_param = 0usize;
    let mut chars = sql.chars().peekable();
    // Reuse the splitter states for quote tracking (no splitting here).
    let mut quote: Option<char> = None;
    let mut bracket = false;
    let mut line_comment = false;
    let mut block_comment = false;
    while let Some(c) = chars.next() {
        if line_comment {
            rewritten.push(c);
            if c == '\n' {
                line_comment = false;
            }
            continue;
        }
        if block_comment {
            rewritten.push(c);
            if c == '*' && chars.peek() == Some(&'/') {
                rewritten.push(chars.next().unwrap());
                block_comment = false;
            }
            continue;
        }
        if let Some(q) = quote {
            rewritten.push(c);
            if c == q {
                if chars.peek() == Some(&q) {
                    rewritten.push(chars.next().unwrap());
                } else {
                    quote = None;
                }
            }
            continue;
        }
        if bracket {
            rewritten.push(c);
            if c == ']' {
                bracket = false;
            }
            continue;
        }
        match c {
            '-' if chars.peek() == Some(&'-') => {
                rewritten.push(c);
                rewritten.push(chars.next().unwrap());
                line_comment = true;
            }
            '/' if chars.peek() == Some(&'*') => {
                rewritten.push(c);
                rewritten.push(chars.next().unwrap());
                block_comment = true;
            }
            '\'' | '"' | '`' => {
                quote = Some(c);
                rewritten.push(c);
            }
            '[' => {
                bracket = true;
                rewritten.push(c);
            }
            '$' => {
                let mut digits = String::new();
                while chars.peek().is_some_and(|p| p.is_ascii_digit()) {
                    digits.push(chars.next().unwrap());
                }
                if digits.is_empty() {
                    rewritten.push('$');
                } else {
                    let index: usize = digits.parse().map_err(|_| {
                        user_error("42601", format!("parameter number ${digits} overflows"))
                    })?;
                    if index == 0 {
                        return Err(user_error(
                            "42601",
                            "parameter numbers start at $1".to_string(),
                        ));
                    }
                    max_param = max_param.max(index);
                    rewritten.push('?');
                    rewritten.push_str(&digits);
                }
            }
            _ => rewritten.push(c),
        }
    }
    Ok((rewritten, max_param))
}

#[derive(Debug, Clone)]
enum StoredParam {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl StoredParam {
    fn to_sql(&self) -> Box<dyn rusqlite::ToSql + Send> {
        match self {
            StoredParam::Null => Box::new(rusqlite::types::Null),
            StoredParam::Bool(b) => Box::new(*b),
            StoredParam::Int(i) => Box::new(*i),
            StoredParam::Float(f) => Box::new(*f),
            StoredParam::Text(s) => Box::new(s.clone()),
            StoredParam::Blob(b) => Box::new(b.clone()),
        }
    }
}

/// A bound parameter as received: raw bytes plus the client-declared type.
/// Decoding happens on the blocking thread after inference fills UNKNOWNs.
#[derive(Debug, Clone)]
struct RawParam {
    data: Option<Vec<u8>>,
    binary: bool,
    declared: Type,
}

/// Extract raw portal parameters for the highest `$N` used by `sql`.
fn extract_params(portal: &Portal<String>, sql: &str) -> PgWireResult<Vec<RawParam>> {
    let (_, max_param) = rewrite_placeholders(sql)?;
    let mut out = Vec::with_capacity(max_param);
    for idx in 0..max_param {
        let Some(slot) = portal.parameters.get(idx) else {
            return Err(user_error(
                "22023",
                format!("missing value for parameter ${}", idx + 1),
            ));
        };
        let declared = portal
            .statement
            .parameter_types
            .get(idx)
            .and_then(|t| t.clone())
            .unwrap_or(Type::UNKNOWN);
        out.push(RawParam {
            data: slot.as_ref().map(|bytes| bytes.to_vec()),
            binary: portal.parameter_format.is_binary(idx),
            declared,
        });
    }
    Ok(out)
}

/// Decode one raw parameter against its (possibly inferred) type.
fn decode_raw(param: &RawParam, ty: &Type, idx: usize) -> PgWireResult<StoredParam> {
    let Some(raw) = param.data.as_deref() else {
        return Ok(StoredParam::Null);
    };
    if !param.binary {
        let text = std::str::from_utf8(raw).map_err(|_| {
            user_error(
                "22P02",
                format!("parameter ${} is not valid UTF-8", idx + 1),
            )
        })?;
        return text_param(ty, text, idx);
    }
    binary_param(ty, raw, idx)
}

/// Binary-format decoding with exact length checks.
fn binary_param(ty: &Type, raw: &[u8], idx: usize) -> PgWireResult<StoredParam> {
    let malformed = |what: &str| {
        user_error(
            "22P02",
            format!("malformed binary {what} parameter ${}", idx + 1),
        )
    };
    let be = |len: usize| {
        if raw.len() != len {
            return Err(malformed(ty.name()));
        }
        Ok(raw)
    };
    match *ty {
        Type::BOOL => {
            let raw = be(1)?;
            match raw[0] {
                0 => Ok(StoredParam::Bool(false)),
                1 => Ok(StoredParam::Bool(true)),
                _ => Err(malformed("BOOL")),
            }
        }
        Type::INT2 => Ok(StoredParam::Int(
            i16::from_be_bytes(be(2)?.try_into().unwrap()) as i64,
        )),
        Type::INT4 => Ok(StoredParam::Int(
            i32::from_be_bytes(be(4)?.try_into().unwrap()) as i64,
        )),
        Type::INT8 => Ok(StoredParam::Int(i64::from_be_bytes(
            be(8)?.try_into().unwrap(),
        ))),
        Type::FLOAT4 => Ok(StoredParam::Float(
            f32::from_be_bytes(be(4)?.try_into().unwrap()) as f64,
        )),
        Type::FLOAT8 => Ok(StoredParam::Float(f64::from_be_bytes(
            be(8)?.try_into().unwrap(),
        ))),
        Type::BYTEA => Ok(StoredParam::Blob(raw.to_vec())),
        Type::DATE => Ok(StoredParam::Text(decode_pg_date(raw)?)),
        Type::TIMESTAMP | Type::TIMESTAMPTZ => Ok(StoredParam::Text(decode_pg_timestamp(raw)?)),
        Type::UUID => Ok(StoredParam::Text(decode_pg_uuid(raw)?)),
        Type::NUMERIC => Err(user_error(
            "42846",
            format!(
                "binary parameter ${} of type numeric is not supported; send text",
                idx + 1
            ),
        )),
        _ => {
            let text = std::str::from_utf8(raw).map_err(|_| {
                user_error(
                    "22P02",
                    format!("parameter ${} is not valid UTF-8", idx + 1),
                )
            })?;
            Ok(StoredParam::Text(text.to_owned()))
        }
    }
}

/// Text-format parameters: typed parsing for scalars, passthrough otherwise.
fn text_param(ty: &Type, text: &str, idx: usize) -> PgWireResult<StoredParam> {
    let bad = || {
        user_error(
            "22P02",
            format!("parameter ${} has invalid {} input", idx + 1, ty.name()),
        )
    };
    match *ty {
        Type::BOOL => match text.to_ascii_lowercase().as_str() {
            "t" | "true" | "1" | "yes" | "on" => Ok(StoredParam::Bool(true)),
            "f" | "false" | "0" | "no" | "off" => Ok(StoredParam::Bool(false)),
            _ => Err(bad()),
        },
        Type::INT2 | Type::INT4 | Type::INT8 => text
            .trim()
            .parse::<i64>()
            .map(StoredParam::Int)
            .map_err(|_| bad()),
        Type::FLOAT4 | Type::FLOAT8 => text
            .trim()
            .parse::<f64>()
            .map(StoredParam::Float)
            .map_err(|_| bad()),
        Type::BYTEA => {
            if let Some(hex) = text
                .strip_prefix("\\x")
                .or_else(|| text.strip_prefix("\\X"))
            {
                hex_to_bytes(hex).map(StoredParam::Blob).ok_or_else(bad)
            } else {
                Ok(StoredParam::Blob(text.as_bytes().to_vec()))
            }
        }
        _ => Ok(StoredParam::Text(text.to_owned())),
    }
}

fn hex_to_bytes(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

/// Postgres binary DATE: int32 days since 2000-01-01.
fn decode_pg_date(raw: &[u8]) -> PgWireResult<String> {
    if raw.len() != 4 {
        return Err(user_error(
            "22P02",
            "malformed binary DATE parameter".to_string(),
        ));
    }
    let days = i32::from_be_bytes(raw.try_into().unwrap()) as i64;
    Ok(days_to_iso(10957 + days))
}

/// Postgres binary TIMESTAMP: int64 microseconds since 2000-01-01.
fn decode_pg_timestamp(raw: &[u8]) -> PgWireResult<String> {
    if raw.len() != 8 {
        return Err(user_error(
            "22P02",
            "malformed binary TIMESTAMP parameter".to_string(),
        ));
    }
    let micros = i64::from_be_bytes(raw.try_into().unwrap());
    let days = micros.div_euclid(86_400_000_000);
    let day_micros = micros.rem_euclid(86_400_000_000);
    let date = days_to_iso(10957 + days);
    let secs = day_micros / 1_000_000;
    let micros = day_micros % 1_000_000;
    Ok(format!(
        "{date} {:02}:{:02}:{:02}.{micros:06}",
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    ))
}

fn decode_pg_uuid(raw: &[u8]) -> PgWireResult<String> {
    if raw.len() != 16 {
        return Err(user_error(
            "22P02",
            "malformed binary UUID parameter".to_string(),
        ));
    }
    let hex: String = raw.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    ))
}

/// Days since 1970-01-01 to `YYYY-MM-DD`.
fn days_to_iso(days: i64) -> String {
    // Inverse of days-from-civil.
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    format!("{:04}-{:02}-{:02}", y + (m <= 2) as i64, m, d)
}

pub(crate) fn decltype_to_type(decl: Option<&str>) -> Type {
    if let Some(decl) = decl {
        let upper = decl.to_ascii_uppercase();
        if upper == "MURMUR_BOOL" {
            return Type::BOOL;
        }
        if upper.contains("INT") {
            return Type::INT8;
        }
        if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
            return Type::TEXT;
        }
        if upper.contains("BLOB") {
            return Type::BYTEA;
        }
        if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
            return Type::FLOAT8;
        }
        // NUMERIC, DECIMAL, DATE/TIME, BOOLEAN storage classes have no exact
        // Postgres twin; values are stringified as TEXT.
        return Type::TEXT;
    }
    Type::TEXT
}

/// Encode one value, coercing SQLite's flexible typing into the column type
/// so text and binary formats stay consistent.
fn encode_value(encoder: &mut DataRowEncoder, ty: &Type, value: &Value) -> PgWireResult<()> {
    if matches!(value, Value::Null) {
        return encoder.encode_field(&None::<i8>);
    }
    match *ty {
        Type::BOOL => {
            let b = matches!(value,Value::Integer(n) if *n!=0);
            encoder.encode_field(&b)
        }
        Type::INT8 => {
            let n = match value {
                Value::Integer(n) => *n,
                Value::Real(f) => *f as i64,
                Value::Text(s) => sqlite_atoi(s),
                Value::Blob(_) => return Err(encode_failed(ty, value)),
                Value::Null => unreachable!(),
            };
            encoder.encode_field(&n)
        }
        Type::FLOAT8 => {
            let f = match value {
                Value::Integer(n) => *n as f64,
                Value::Real(f) => *f,
                Value::Text(s) => s.trim().parse::<f64>().unwrap_or(0.0),
                Value::Blob(_) => return Err(encode_failed(ty, value)),
                Value::Null => unreachable!(),
            };
            encoder.encode_field(&f)
        }
        Type::BYTEA => {
            let bytes: Vec<u8> = match value {
                Value::Blob(b) => b.clone(),
                Value::Text(s) => s.as_bytes().to_vec(),
                _ => return Err(encode_failed(ty, value)),
            };
            encoder.encode_field(&bytes)
        }
        _ => {
            let text = match value {
                Value::Integer(n) => n.to_string(),
                Value::Real(f) => format!("{f}"),
                Value::Text(s) => s.clone(),
                Value::Blob(b) => format!(
                    "\\x{}",
                    b.iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>()
                ),
                Value::Null => unreachable!(),
            };
            encoder.encode_field(&text)
        }
    }
}

fn encode_failed(ty: &Type, value: &Value) -> PgWireError {
    user_error(
        "42846",
        format!("cannot encode {} value as {}", value_kind(value), ty.name()),
    )
}

fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "NULL",
        Value::Integer(_) => "integer",
        Value::Real(_) => "real",
        Value::Text(_) => "text",
        Value::Blob(_) => "blob",
    }
}

/// SQLite `CAST(x AS INTEGER)` semantics: leading numeric prefix, else 0.
fn sqlite_atoi(s: &str) -> i64 {
    let s = s.trim_start();
    let (sign, digits) = match s.strip_prefix('-') {
        Some(rest) => (-1i64, rest),
        None => (1i64, s.strip_prefix('+').unwrap_or(s)),
    };
    let mut value: i64 = 0;
    for c in digits.chars() {
        let Some(digit) = c.to_digit(10) else { break };
        value = value.saturating_mul(10).saturating_add(digit as i64);
    }
    value.saturating_mul(sign)
}

/// Build a client-facing error with a Postgres SQLSTATE code.
pub fn user_error(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message,
    )))
}

/// Map engine errors to SQLSTATE codes without leaking internals.
pub fn engine_error(error: crate::Error) -> PgWireError {
    match error {
        crate::Error::Sql(sqlite) => sqlite_error(sqlite),
        crate::Error::WrongKey => user_error("28000", "incorrect key".to_string()),
        crate::Error::Busy => user_error(
            "40001",
            "queue full or merge baseline changed; retry".to_string(),
        ),
        crate::Error::Closed => user_error("08003", "database is closed".to_string()),
        crate::Error::Invalid(message) => user_error("22023", message),
        crate::Error::Corrupt(message) => user_error("XX001", message),
        crate::Error::Limit(message) => user_error("54000", message),
        crate::Error::SchemaMismatch => user_error(
            "55000",
            "database identity or schema history mismatch".to_string(),
        ),
        crate::Error::Storage(message) => user_error("58000", format!("storage error: {message}")),
        crate::Error::Io(error) => user_error("58000", format!("storage error: {error}")),
        crate::Error::CallbackPanicked => user_error(
            "XX000",
            "SQL callback panicked; transaction rolled back".to_string(),
        ),
    }
}

fn sqlite_error(error: rusqlite::Error) -> PgWireError {
    let message = error.to_string();
    let code = match &error {
        rusqlite::Error::SqliteFailure(code, _) => {
            failure_code(code.code, code.extended_code, &message)
        }
        rusqlite::Error::SqlInputError { error: code, .. } => {
            failure_code(code.code, code.extended_code, &message)
        }
        rusqlite::Error::FromSqlConversionFailure(..) => "22P02",
        rusqlite::Error::ToSqlConversionFailure(..) => "22000",
        rusqlite::Error::QueryReturnedNoRows => "02000",
        rusqlite::Error::NulError(_) => "22021",
        _ => "XX000",
    };
    user_error(code, message)
}

fn failure_code(
    primary: rusqlite::ErrorCode,
    extended: std::os::raw::c_int,
    message: &str,
) -> &'static str {
    use rusqlite::ErrorCode::*;
    match primary {
        ConstraintViolation => match extended {
            1555 | 2067 => "23505",
            1299 => "23502",
            787 => "23503",
            275 => "23514",
            _ => "23000",
        },
        AuthorizationForStatementDenied => "42501",
        DatabaseBusy | DatabaseLocked => "55P03",
        ReadOnly => "25006",
        DatabaseCorrupt => "XX001",
        TooBig => "54000",
        SystemIoFailure | CannotOpen | DiskFull | NotADatabase => "58000",
        TypeMismatch => "42804",
        ParameterOutOfRange => "22023",
        _ => {
            let lower = message.to_ascii_lowercase();
            if lower.contains("no such table") || lower.contains("no such index") {
                "42P01"
            } else if lower.contains("no such column") {
                "42703"
            } else if lower.contains("no such function") {
                "42883"
            } else if lower.contains("primary-key component must be supplied and non-null") {
                "23502"
            } else {
                "42601"
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitter_skips_quotes_and_comments() {
        assert_eq!(
            split_statements("SELECT 1; SELECT 2"),
            ["SELECT 1", "SELECT 2"]
        );
        assert_eq!(
            split_statements("SELECT 'a;b'; -- x;\nSELECT 2"),
            ["SELECT 'a;b'", "-- x;\nSELECT 2"]
        );
        assert_eq!(split_statements("/* ; */SELECT 1"), ["/* ; */SELECT 1"]);
        assert_eq!(split_statements("SELECT \"a;b\""), ["SELECT \"a;b\""]);
        assert!(split_statements("  ; ; ").is_empty());
    }

    #[test]
    fn trivia_only_counts_as_empty() {
        assert!(is_trivia_only(""));
        assert!(is_trivia_only("  ; ; "));
        assert!(is_trivia_only("-- ping"));
        assert!(is_trivia_only("/* hello */"));
        assert!(!is_trivia_only("SELECT 1"));
        assert!(!is_trivia_only("'just a string'"));
        assert!(!is_trivia_only("-- lead\nSELECT 1"));
        assert!(split_statements("-- ping").is_empty());
        assert_eq!(split_statements("SELECT 1; -- tail"), ["SELECT 1"]);
    }

    #[test]
    fn placeholders_rewrite_outside_literals() {
        assert_eq!(
            rewrite_placeholders("SELECT $1, $2").unwrap(),
            ("SELECT ?1, ?2".to_owned(), 2)
        );
        assert_eq!(
            rewrite_placeholders("SELECT '$1', $12").unwrap(),
            ("SELECT '$1', ?12".to_owned(), 12)
        );
        assert_eq!(
            rewrite_placeholders("SELECT 1").unwrap(),
            ("SELECT 1".to_owned(), 0)
        );
        assert!(rewrite_placeholders("SELECT $0").is_err());
        assert!(rewrite_placeholders("SELECT $").unwrap().1 == 0);
    }

    #[test]
    fn routing_covers_statements() {
        assert!(matches!(route_statement("select 1"), Route::Sql));
        assert!(matches!(
            route_statement("-- hi\nCREATE TABLE t(x)"),
            Route::Ddl(_)
        ));
        assert!(matches!(route_statement("begin"), Route::Txn("BEGIN")));
        assert!(matches!(
            route_statement("START TRANSACTION"),
            Route::Txn("BEGIN")
        ));
        assert!(matches!(route_statement("commit;"), Route::Txn("COMMIT")));
        assert!(matches!(
            route_statement("rollback work"),
            Route::Txn("ROLLBACK")
        ));
        assert!(matches!(route_statement("BEGIN FOO"), Route::Sql));
        assert!(matches!(
            route_statement("murmur unlock key 'x'"),
            Route::Murmur(_)
        ));
        assert!(matches!(
            route_statement("SELECT version()"),
            Route::Version
        ));
        assert!(matches!(
            route_statement("select  version ( ) ;"),
            Route::Version
        ));
        assert!(matches!(route_statement("SELECT version(), 1"), Route::Sql));
        assert!(matches!(
            route_statement("SHOW server_version"),
            Route::ShowVersion
        ));
        assert!(matches!(route_statement("SHOW ALL"), Route::Sql));
        assert!(is_unlock_command("MURMUR UNLOCK KEY 'x'"));
        assert!(!is_unlock_command("MURMUR SET TABLE t REPLICATED true"));
        assert!(!is_unlock_command("SELECT 1"));
    }

    #[test]
    fn probe_shapes() {
        assert_eq!(
            probe_sql("SELECT 1;"),
            Some("SELECT * FROM (SELECT 1) LIMIT 1".to_owned())
        );
        assert_eq!(
            probe_sql("WITH x AS (SELECT 1) SELECT * FROM x"),
            Some("SELECT * FROM (WITH x AS (SELECT 1) SELECT * FROM x) LIMIT 1".to_owned())
        );
        assert_eq!(
            probe_sql("PRAGMA table_info(t)"),
            Some("PRAGMA table_info(t)".to_owned())
        );
        assert_eq!(probe_sql("PRAGMA journal_mode = WAL"), None);
        assert_eq!(probe_sql("INSERT INTO t VALUES (1)"), None);
        assert_eq!(probe_sql("EXPLAIN SELECT 1"), None);
        let columns = vec![
            ColInfo {
                name: "a".into(),
                decltype: Some("INTEGER".into()),
            },
            ColInfo {
                name: "b".into(),
                decltype: None,
            },
            ColInfo {
                name: "c".into(),
                decltype: None,
            },
        ];
        let sample = vec![Value::Integer(1), Value::Text("x".into()), Value::Null];
        assert_eq!(
            column_types(&columns, Some(&sample)),
            vec![Type::INT8, Type::TEXT, Type::TEXT]
        );
        assert_eq!(
            column_types(&columns, None),
            vec![Type::INT8, Type::TEXT, Type::TEXT]
        );
    }

    #[test]
    fn decltype_mapping_and_date_math() {
        assert_eq!(decltype_to_type(Some("INTEGER")), Type::INT8);
        assert_eq!(decltype_to_type(Some("VARCHAR(10)")), Type::TEXT);
        assert_eq!(decltype_to_type(Some("DOUBLE")), Type::FLOAT8);
        assert_eq!(decltype_to_type(Some("BLOB")), Type::BYTEA);
        assert_eq!(decltype_to_type(Some("NUMERIC")), Type::TEXT);
        assert_eq!(decltype_to_type(None), Type::TEXT);
        assert_eq!(value_type(&Value::Integer(1)), Some(Type::INT8));
        assert_eq!(value_type(&Value::Null), None);
        assert_eq!(days_to_iso(0), "1970-01-01");
        assert_eq!(days_to_iso(20742), "2026-10-16");
        assert_eq!(decode_pg_date(&0i32.to_be_bytes()).unwrap(), "2000-01-01");
        assert_eq!(days_to_iso(10957), "2000-01-01");
        assert_eq!(sqlite_atoi("  -12x"), -12);
        assert_eq!(sqlite_atoi("abc"), 0);
    }
}

//! Disposable search indexes. Source rows and their Spool records are authoritative.
use crate::{Error, Options, Result, RowState, Schema, Value, model::quote};
use rusqlite::{
    Connection, fallible_streaming_iterator::FallibleStreamingIterator, hooks::Action,
    session::ChangesetIter,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5Index {
    pub name: String,
    pub source_table: String,
    pub columns: Vec<String>,
    /// A built-in tokenizer: unicode61 (default), ascii, porter, or trigram.
    pub tokenizer: String,
}
impl Fts5Index {
    pub fn new(
        name: impl Into<String>,
        source_table: impl Into<String>,
        columns: Vec<String>,
    ) -> Self {
        Self {
            name: name.into(),
            source_table: source_table.into(),
            columns,
            tokenizer: "unicode61".into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DistanceMetric {
    #[default]
    L2,
    Cosine,
    L1,
}
/// Persistent configuration for one application's BLOB vector column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorColumnConfig {
    pub dimensions: usize,
    #[serde(default)]
    pub distance: DistanceMetric,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndex {
    pub name: String,
    pub source_table: String,
    /// Source BLOB containing little-endian float32 values; also the index column name.
    pub column: String,
    pub dimensions: usize,
    pub distance: DistanceMetric,
}
impl VectorIndex {
    pub fn new(
        name: impl Into<String>,
        source_table: impl Into<String>,
        column: impl Into<String>,
        dimensions: usize,
    ) -> Self {
        Self {
            name: name.into(),
            source_table: source_table.into(),
            column: column.into(),
            dimensions,
            distance: DistanceMetric::L2,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchIndex {
    Fts5(Fts5Index),
    Vector(VectorIndex),
}
impl From<Fts5Index> for SearchIndex {
    fn from(value: Fts5Index) -> Self {
        Self::Fts5(value)
    }
}
impl From<VectorIndex> for SearchIndex {
    fn from(value: VectorIndex) -> Self {
        Self::Vector(value)
    }
}
impl SearchIndex {
    pub fn name(&self) -> &str {
        match self {
            Self::Fts5(x) => &x.name,
            Self::Vector(x) => &x.name,
        }
    }
    pub fn source_table(&self) -> &str {
        match self {
            Self::Fts5(x) => &x.source_table,
            Self::Vector(x) => &x.source_table,
        }
    }
    fn columns(&self) -> Vec<&str> {
        match self {
            Self::Fts5(x) => x.columns.iter().map(String::as_str).collect(),
            Self::Vector(x) => vec![&x.column],
        }
    }
    fn create_sql(&self) -> String {
        let module = match self {
            Self::Fts5(x) => format!(
                "fts5({}, tokenize='{}')",
                x.columns
                    .iter()
                    .map(|c| quote(c))
                    .collect::<Vec<_>>()
                    .join(","),
                x.tokenizer
            ),
            Self::Vector(x) => format!(
                "vec0({} float[{}] distance_metric={})",
                x.column,
                x.dimensions,
                match x.distance {
                    DistanceMetric::L2 => "l2",
                    DistanceMetric::Cosine => "cosine",
                    DistanceMetric::L1 => "l1",
                }
            ),
        };
        format!(
            "CREATE VIRTUAL TABLE temp.{} USING {module}",
            quote(self.name())
        )
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchIndexStatus {
    pub name: String,
    pub ready: bool,
    pub rebuilding: bool,
    pub pending_keys: usize,
    pub queued_bytes: usize,
    pub error: Option<String>,
}

pub(crate) fn schema_index_name(table: &crate::Table) -> String {
    format!("murmur_fts_{}", table.id)
}

pub(crate) fn vector_index_name(table: &crate::Table, column: &crate::Column) -> String {
    format!("murmur_vec_{}_{}", table.id, column.id)
}

/// Shared by startup, schema maintenance, and explicit-index replacement.
pub(crate) fn schema_definitions(explicit: &[SearchIndex], schema: &Schema) -> Result<Vec<SearchIndex>> {
    let mut definitions = explicit.to_vec();
    for table in &schema.tables {
        let columns: Vec<_> = table.columns.iter().filter(|c| c.fts).map(|c| c.name.clone()).collect();
        if !columns.is_empty() {
            definitions.push(Fts5Index::new(schema_index_name(table), &table.name, columns).into());
        }
        for column in &table.columns {
            if let Some(config) = &column.vector {
                if column.name.eq_ignore_ascii_case(crate::primary_key::COLUMN) {
                    return Err(Error::Invalid("vector declarations require an application column".into()));
                }
                definitions.push(VectorIndex {
                    name: vector_index_name(table, column), source_table: table.name.clone(),
                    column: column.name.clone(), dimensions: config.dimensions, distance: config.distance,
                }.into());
            }
        }
    }
    let options = Options { search_indexes: definitions, ..Options::default() };
    validate_options(&options)?;
    Ok(options.search_indexes)
}

pub(crate) type Guard = Arc<RwLock<BTreeMap<String, bool>>>;

pub(crate) fn validate_options(o: &Options) -> Result<()> {
    if o.search_queue_bytes == 0
        || o.search_queue_bytes > 128 << 20
        || o.search_batch_rows == 0
        || o.search_batch_rows > 65536
        || o.search_interval.is_zero()
    {
        return Err(Error::Invalid("invalid search maintenance limits".into()));
    }
    let mut names = BTreeSet::new();
    for index in &o.search_indexes {
        if index.name().is_empty()
            || index.name().contains('\0')
            || index.source_table().is_empty()
            || index.source_table().contains('\0')
            || index.name().to_ascii_lowercase().starts_with("sqlite_")
            || index.name().to_ascii_lowercase().starts_with("_crdt_")
            || !names.insert(index.name().to_ascii_lowercase())
        {
            return Err(Error::Invalid(
                "invalid or duplicate search index name".into(),
            ));
        }
        let columns = index.columns();
        if columns.is_empty()
            || columns.iter().any(|c| {
                c.is_empty()
                    || c.contains('\0')
                    || c.eq_ignore_ascii_case("rowid")
                    || c.eq_ignore_ascii_case(index.name())
            })
            || columns
                .iter()
                .map(|c| c.to_ascii_lowercase())
                .collect::<BTreeSet<_>>()
                .len()
                != columns.len()
        {
            return Err(Error::Invalid("invalid search index columns".into()));
        }
        match index {
            SearchIndex::Fts5(x)
                if !["unicode61", "ascii", "porter", "trigram"].contains(&x.tokenizer.as_str()) =>
            {
                return Err(Error::Invalid("unsupported FTS5 tokenizer".into()));
            }
            SearchIndex::Vector(x) => {
                if !x
                    .column
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphabetic)
                    || !x
                        .column
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                {
                    return Err(Error::Invalid(
                        "sqlite-vec column names must be ASCII SQL identifiers".into(),
                    ));
                }
                if !cfg!(feature = "vector") {
                    return Err(Error::Invalid(
                        "vector indexes require the vector Cargo feature".into(),
                    ));
                }
                if x.dimensions == 0 || x.dimensions > 8192 {
                    return Err(Error::Invalid("vector dimensions must be 1..=8192".into()));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[derive(Clone)]
struct Index {
    definition: SearchIndex,
    owned: BTreeSet<String>,
    installed: bool,
    ready: bool,
    rebuild: bool,
    started: bool,
    last: Option<i64>,
    keys: BTreeSet<i64>,
    error: Option<String>,
}
impl Index {
    fn invalidate(&mut self) {
        self.rebuild = true;
        self.started = false;
        self.last = None;
        self.keys.clear();
        self.ready = false;
    }
}

#[derive(Clone)]
pub(crate) struct Manager {
    explicit: Vec<SearchIndex>,
    indexes: Vec<Index>,
    queued: VecDeque<Vec<u8>>,
    queued_bytes: usize,
    queued_sources: BTreeSet<String>,
    budget: usize,
    batch: usize,
    next: usize,
    publications: BTreeSet<String>,
    pub(crate) guard: Guard,
}
impl Manager {
    pub(crate) fn new(o: &Options) -> Self {
        Self {
            explicit: o.search_indexes.clone(),
            indexes: o
                .search_indexes
                .iter()
                .cloned()
                .map(|definition| Index {
                    owned: BTreeSet::from([definition.name().to_ascii_lowercase()]),
                    definition,
                    installed: false,
                    ready: false,
                    rebuild: false,
                    started: false,
                    last: None,
                    keys: BTreeSet::new(),
                    error: None,
                })
                .collect(),
            queued: VecDeque::new(),
            queued_bytes: 0,
            queued_sources: BTreeSet::new(),
            budget: o.search_queue_bytes,
            batch: o.search_batch_rows,
            next: 0,
            publications: BTreeSet::new(),
            guard: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
    pub(crate) fn register(c: &Connection, definitions: &[SearchIndex]) -> Result<()> {
        if !definitions.is_empty() {
            // TEMP pages must not spill document text or embeddings to plaintext
            // files. Changing temp_store drops TEMP objects, so only change it
            // before the first managed index is installed.
            let temp_store: i64 = c.query_row("PRAGMA temp_store", [], |r| r.get(0))?;
            if temp_store != 2 {
                c.execute_batch("PRAGMA temp_store=MEMORY")?;
            }
        }
        if !definitions
            .iter()
            .any(|i| matches!(i, SearchIndex::Vector(_)))
        {
            return Ok(());
        }
        #[cfg(feature = "vector")]
        {
            // sqlite-vec is compiled with SQLITE_CORE. Call its actual C ABI on this
            // connection, avoiding process-wide auto-extension registration.
            type Init = unsafe extern "C" fn(
                *mut rusqlite::ffi::sqlite3,
                *mut *mut std::ffi::c_char,
                *const rusqlite::ffi::sqlite3_api_routines,
            ) -> std::ffi::c_int;
            let init: Init =
                unsafe { std::mem::transmute(sqlite_vec::sqlite3_vec_init as *const ()) };
            let mut message = std::ptr::null_mut();
            let rc = unsafe { init(c.handle(), &mut message, std::ptr::null()) };
            let error = if message.is_null() {
                None
            } else {
                Some(
                    unsafe { std::ffi::CStr::from_ptr(message) }
                        .to_string_lossy()
                        .into_owned(),
                )
            };
            unsafe {
                rusqlite::ffi::sqlite3_free(message.cast());
            }
            if rc != rusqlite::ffi::SQLITE_OK {
                return Err(Error::Invalid(
                    error.unwrap_or_else(|| "sqlite-vec initialization failed".into()),
                ));
            }
            Ok(())
        }
        #[cfg(not(feature = "vector"))]
        {
            let _ = c;
            Err(Error::Invalid(
                "vector indexes require the vector Cargo feature".into(),
            ))
        }
    }
    pub(crate) fn validate_schema(&self, s: &Schema, allow_missing: bool) -> Result<()> {
        for index in &self.indexes {
            let d = &index.definition;
            if s.tables
                .iter()
                .any(|t| index.owned.contains(&t.name.to_ascii_lowercase()))
            {
                return Err(Error::Invalid(format!(
                    "source schema collides with search index {}",
                    d.name()
                )));
            }
            let Some(t) = s.tables.iter().find(|t| t.name == d.source_table()) else {
                if allow_missing && !index.installed {
                    continue;
                }
                return Err(Error::Invalid(format!(
                    "missing source table {}",
                    d.source_table()
                )));
            };
            let pk = crate::primary_key::application_columns(t);
            if pk.len() != 1 || !pk[0].declared_type.eq_ignore_ascii_case("INTEGER") {
                return Err(Error::Invalid(format!(
                    "{} requires one INTEGER PRIMARY KEY for search indexing",
                    t.name
                )));
            }
            for name in d.columns() {
                let col = t.columns.iter().find(|c| c.name == name).ok_or_else(|| {
                    Error::Invalid(format!("missing search column {}.{name}", t.name))
                })?;
                let expected = if matches!(d, SearchIndex::Vector(_)) {
                    "BLOB"
                } else {
                    "TEXT"
                };
                if !col.declared_type.eq_ignore_ascii_case(expected) {
                    return Err(Error::Invalid(format!(
                        "{}.{} must be declared {expected}",
                        t.name, col.name
                    )));
                }
            }
        }
        Ok(())
    }
    pub(crate) fn sync_schema(
        &mut self,
        c: &Connection,
        s: &Schema,
        allow_missing: bool,
    ) -> Result<()> {
        // Schema-derived definitions are disposable; only the column flags in
        // Spool are authoritative. Reconcile before validating so local drops
        // and renames cannot leave stale derived definitions behind.
        let definitions = schema_definitions(&self.explicit, s)?;
        if definitions != self.indexes.iter().map(|i| i.definition.clone()).collect::<Vec<_>>() {
            let options = Options { search_indexes: definitions, ..Options::default() };
            validate_options(&options)?;
            let candidate = Manager::new(&options);
            candidate.validate_schema(s, allow_missing)?;
            Self::register(c, &options.search_indexes)?;
            self.drop_indexes(c)?;
            self.indexes = candidate.indexes;
            self.queued.clear();
            self.queued_sources.clear();
            self.queued_bytes = 0;
            self.next = 0;
        }
        self.validate_schema(s, allow_missing)?;
        for index in &mut self.indexes {
            if !s
                .tables
                .iter()
                .any(|t| t.name == index.definition.source_table())
            {
                continue;
            }
            if !index.installed {
                let before = temp_tables(c)?;
                c.execute_batch(&index.definition.create_sql())?;
                let after = temp_tables(c)?;
                index.owned = after.difference(&before).cloned().collect();
                index.installed = true;
            }
            index.invalidate();
        }
        // Modules may create shadow names that collide with MAIN source tables.
        // Validate actual ownership after creation, before publishing the schema.
        self.validate_schema(s, allow_missing)?;
        self.publish_guard();
        Ok(())
    }
    pub(crate) fn drop_indexes(&self, c: &Connection) -> Result<()> {
        for index in &self.indexes {
            if index.installed {
                c.execute_batch(&format!(
                    "DROP TABLE temp.{}",
                    quote(index.definition.name())
                ))?;
            }
        }
        Ok(())
    }
    pub(crate) fn publish_guard(&self) {
        *self.guard.write().unwrap() = self
            .indexes
            .iter()
            .flat_map(|i| i.owned.iter().map(move |name| (name.clone(), i.ready)))
            .collect();
    }
    pub(crate) fn status(&self) -> Vec<SearchIndexStatus> {
        self.indexes
            .iter()
            .map(|i| SearchIndexStatus {
                name: i.definition.name().into(),
                ready: i.ready,
                rebuilding: i.rebuild,
                pending_keys: i.keys.len(),
                queued_bytes: self.queued_bytes,
                error: i.error.clone(),
            })
            .collect()
    }
    pub(crate) fn take_publications(&mut self) -> Vec<String> {
        std::mem::take(&mut self.publications).into_iter().collect()
    }
    fn memory(&self) -> usize {
        self.queued_bytes.saturating_add(
            self.indexes
                .iter()
                .map(|i| i.keys.len().saturating_mul(32))
                .sum::<usize>(),
        )
    }
    fn overflow(&mut self) {
        self.queued.clear();
        self.queued_bytes = 0;
        for index in &mut self.indexes {
            if index.installed
                && (!index.keys.is_empty()
                    || self
                        .queued_sources
                        .contains(index.definition.source_table()))
            {
                index.invalidate();
            }
        }
        self.queued_sources.clear();
        self.publish_guard();
    }
    pub(crate) fn enqueue(&mut self, bytes: &[u8], affected: &[String]) {
        if !self
            .indexes
            .iter()
            .any(|i| affected.iter().any(|t| t == i.definition.source_table()))
        {
            return;
        }
        self.queued_sources.extend(
            affected
                .iter()
                .filter(|t| {
                    self.indexes
                        .iter()
                        .any(|i| i.definition.source_table() == t.as_str())
                })
                .cloned(),
        );
        if self.memory().saturating_add(bytes.len()) > self.budget {
            self.overflow();
            return;
        }
        self.queued.push_back(bytes.to_vec());
        self.queued_bytes += bytes.len();
    }
    pub(crate) fn rows(&mut self, s: &Schema, rows: &[RowState]) {
        for row in rows {
            if let Some(t) = s.tables.iter().find(|t| t.id == row.table) {
                if t.application_key.is_some() {
                    for index in &mut self.indexes { if index.definition.source_table() == t.name { index.invalidate(); } }
                } else if let Some(Value::Integer(key)) = row.key.first() { self.key(&t.name, *key); }
            }
        }
    }
    fn key(&mut self, table: &str, key: i64) {
        for index in &mut self.indexes {
            if index.definition.source_table() == table && index.installed {
                index.keys.insert(key);
            }
        }
        if self.memory() > self.budget {
            self.overflow();
        }
    }
    fn decode_pending(&mut self) -> Result<()> {
        while let Some(bytes) = self.queued.pop_front() {
            self.queued_bytes -= bytes.len();
            let mut cursor = std::io::Cursor::new(&bytes);
            let reader: &mut dyn std::io::Read = &mut cursor;
            let mut iter = ChangesetIter::start_strm(&reader)?;
            while let Some(item) = iter.next()? {
                let op = item.op()?;
                if !self
                    .indexes
                    .iter()
                    .any(|i| i.definition.source_table() == op.table_name())
                {
                    continue;
                }
                let pk = item
                    .pk()?
                    .iter()
                    .position(|p| *p != 0)
                    .ok_or(Error::SchemaMismatch)?;
                let value = Value::read(if op.code() == Action::SQLITE_INSERT {
                    item.new_value(pk)?
                } else {
                    item.old_value(pk)?
                })?;
                let Value::Integer(key) = value else {
                    for index in &mut self.indexes { if index.definition.source_table() == op.table_name() { index.invalidate(); } }
                    continue;
                };
                self.key(op.table_name(), key);
            }
        }
        self.queued_sources.clear();
        Ok(())
    }
    pub(crate) fn pending(&self) -> bool {
        !self.queued.is_empty() || self.indexes.iter().any(|i| i.rebuild || !i.keys.is_empty())
    }
    /// A maintenance turn never executes on the application transaction path.
    pub(crate) fn tick(&mut self, c: &Connection, s: &Schema) -> Result<Vec<String>> {
        self.decode_pending()?;
        let mut published = Vec::new();
        let mut remaining = self.batch;
        let mut first_error = None;
        let start = self.next;
        let length = self.indexes.len();
        for offset in 0..length {
            if remaining == 0 {
                break;
            }
            let position = (start + offset) % length;
            let index = &mut self.indexes[position];
            if !index.installed || (!index.rebuild && index.keys.is_empty()) {
                continue;
            }
            self.next = (position + 1) % length;
            let saved = (index.rebuild, index.started, index.last);
            let mut completed_keys = Vec::new();
            let result = (|| -> Result<usize> {
                c.execute_batch("SAVEPOINT murmur_search_work")?;
                let t = s
                    .tables
                    .iter()
                    .find(|t| t.name == index.definition.source_table())
                    .ok_or(Error::SchemaMismatch)?;
                let pk = crate::primary_key::application_columns(t)[0].name.as_str();
                let mut count = 0;
                if index.rebuild {
                    if !index.started {
                        c.execute(
                            &format!("DELETE FROM temp.{}", quote(index.definition.name())),
                            [],
                        )?;
                        index.started = true;
                    }
                    let columns = index
                        .definition
                        .columns()
                        .iter()
                        .map(|c| quote(c))
                        .collect::<Vec<_>>()
                        .join(",");
                    let sql = format!(
                        "SELECT {},{} FROM main.{} {} ORDER BY {} LIMIT ?",
                        quote(pk),
                        columns,
                        quote(&t.name),
                        if index.last.is_some() {
                            format!("WHERE {} > ?", quote(pk))
                        } else {
                            String::new()
                        },
                        quote(pk)
                    );
                    let mut q = c.prepare(&sql)?;
                    let mut cursor = if let Some(last) = index.last {
                        q.query(rusqlite::params![last, remaining as i64])?
                    } else {
                        q.query([remaining as i64])?
                    };
                    while let Some(row) = cursor.next()? {
                        let key = row.get(0)?;
                        let values = (1..=index.definition.columns().len())
                            .map(|i| Value::read(row.get_ref(i)?))
                            .collect::<rusqlite::Result<Vec<_>>>()?;
                        insert(c, &index.definition, key, &values)?;
                        index.last = Some(key);
                        count += 1;
                    }
                    if count < remaining {
                        index.rebuild = false;
                    }
                } else {
                    let keys = index
                        .keys
                        .iter()
                        .take(remaining)
                        .copied()
                        .collect::<Vec<_>>();
                    for key in &keys {
                        c.execute(
                            &format!(
                                "DELETE FROM temp.{} WHERE rowid=?",
                                quote(index.definition.name())
                            ),
                            [key],
                        )?;
                        let columns = index
                            .definition
                            .columns()
                            .iter()
                            .map(|c| quote(c))
                            .collect::<Vec<_>>()
                            .join(",");
                        let mut q = c.prepare(&format!(
                            "SELECT {columns} FROM main.{} WHERE {}=?",
                            quote(&t.name),
                            quote(pk)
                        ))?;
                        let mut cursor = q.query([key])?;
                        if let Some(row) = cursor.next()? {
                            let values = (0..index.definition.columns().len())
                                .map(|i| Value::read(row.get_ref(i)?))
                                .collect::<rusqlite::Result<Vec<_>>>()?;
                            insert(c, &index.definition, *key, &values)?;
                        }
                    }
                    count = keys.len();
                    completed_keys = keys;
                }
                c.execute_batch("RELEASE murmur_search_work")?;
                Ok(count)
            })();
            match result {
                Ok(count) => {
                    for key in completed_keys {
                        index.keys.remove(&key);
                    }
                    remaining = remaining.saturating_sub(count.max(1));
                    index.error = None;
                    if !index.rebuild && index.keys.is_empty() {
                        index.ready = true;
                    }
                    if index.ready {
                        published.push(index.definition.name().to_owned());
                    }
                }
                Err(error) => {
                    let _ = c.execute_batch(
                        "ROLLBACK TO murmur_search_work; RELEASE murmur_search_work",
                    );
                    (index.rebuild, index.started, index.last) = saved;
                    index.ready = false;
                    index.error = Some(error.to_string());
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        self.publish_guard();
        self.publications.extend(published.iter().cloned());
        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(published)
        }
    }
    pub(crate) fn refresh(&mut self, c: &Connection, s: &Schema) -> Result<Vec<String>> {
        let mut published = BTreeSet::new();
        while self.pending() {
            published.extend(self.tick(c, s)?);
        }
        Ok(published.into_iter().collect())
    }
}
fn temp_tables(c: &Connection) -> Result<BTreeSet<String>> {
    let mut q = c.prepare("SELECT name FROM sqlite_temp_schema WHERE type IN ('table','index')")?;
    Ok(q.query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .map(|s| s.to_ascii_lowercase())
        .collect())
}
fn insert(c: &Connection, d: &SearchIndex, key: i64, values: &[Value]) -> Result<()> {
    if let SearchIndex::Vector(v) = d {
        if matches!(values[0], Value::Null) {
            return Ok(());
        }
        let Value::Blob(bytes) = &values[0] else {
            return Err(Error::Invalid("embedding must be a float32 BLOB".into()));
        };
        if bytes.len() != v.dimensions * 4
            || bytes
                .chunks_exact(4)
                .any(|chunk| !f32::from_le_bytes(chunk.try_into().unwrap()).is_finite())
        {
            return Err(Error::Invalid(format!(
                "invalid embedding for {} row {key}",
                v.name
            )));
        }
    }
    let mut params = vec![Value::Integer(key)];
    params.extend_from_slice(values);
    c.execute(
        &format!(
            "INSERT INTO temp.{}(rowid,{}) VALUES({})",
            quote(d.name()),
            d.columns()
                .iter()
                .map(|c| quote(c))
                .collect::<Vec<_>>()
                .join(","),
            vec!["?"; params.len()].join(",")
        ),
        rusqlite::params_from_iter(&params),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporary_pages_stay_in_memory_and_registration_preserves_existing_indexes() -> Result<()> {
        let c = Connection::open_in_memory()?;
        c.execute_batch("PRAGMA temp_store=FILE")?;
        let definitions = vec![Fts5Index::new("search", "docs", vec!["title".into()]).into()];
        Manager::register(&c, &definitions)?;
        assert_eq!(
            c.query_row("PRAGMA temp_store", [], |r| r.get::<_, i64>(0))?,
            2
        );
        c.execute_batch("CREATE VIRTUAL TABLE temp.search USING fts5(title); INSERT INTO temp.search VALUES('preserved')")?;
        Manager::register(&c, &definitions)?;
        assert_eq!(
            c.query_row(
                "SELECT count(*) FROM temp.search WHERE search MATCH 'preserved'",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            1
        );
        Ok(())
    }

    #[cfg(feature = "vector")]
    #[test]
    fn vector_module_registration_is_connection_local() -> Result<()> {
        let c = Connection::open_in_memory()?;
        Manager::register(
            &c,
            &[VectorIndex::new("vectors", "docs", "embedding", 2).into()],
        )?;
        assert!(
            c.query_row("SELECT vec_version()", [], |r| r.get::<_, String>(0))?
                .starts_with('v')
        );
        let other = Connection::open_in_memory()?;
        assert!(
            other
                .query_row("SELECT vec_version()", [], |r| r.get::<_, String>(0))
                .is_err()
        );
        Ok(())
    }
}

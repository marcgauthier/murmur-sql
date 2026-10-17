//! Managed row identities. All SQL rewriting stays at the local SQL boundary;
//! conflict versions and transition baselines live only in Spool.
use crate::{Error, Result, model::*, schema::pk_columns};
use rusqlite::{Connection, functions::FunctionFlags};

pub(crate) const COLUMN: &str = "id_murmur_primary_key";
fn invalid(message: &str) -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
        Some(message.into()),
    )
}
fn missing_key() -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT_NOTNULL),
        Some("every application primary-key component must be supplied and non-null".into()),
    )
}
fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[derive(Clone, Debug)]
pub(crate) struct Token {
    pub text: String,
    pub start: usize,
    pub end: usize,
    pub quoted: bool,
}
impl Token {
    fn is(&self, s: &str) -> bool {
        !self.quoted && self.text.eq_ignore_ascii_case(s)
    }
}
/// Token offsets allow us to preserve SQLite expressions and placeholder spelling.
pub(crate) fn tokens(sql: &str) -> rusqlite::Result<Vec<Token>> {
    let b = sql.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        if b[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if b[i..].starts_with(b"--") {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b[i..].starts_with(b"/*") {
            i += 2;
            while i + 1 < b.len() && &b[i..i + 2] != b"*/" {
                i += 1;
            }
            if i + 1 >= b.len() {
                return Err(invalid("unterminated SQL comment"));
            }
            i += 2;
            continue;
        }
        let start = i;
        if matches!(b[i], b'\'' | b'"' | b'`' | b'[') {
            let opener = b[i];
            let closer = if opener == b'[' { b']' } else { opener };
            i += 1;
            let mut text = Vec::new();
            let mut closed = false;
            while i < b.len() {
                if b[i] == closer {
                    if closer != b']' && i + 1 < b.len() && b[i + 1] == closer {
                        text.push(closer);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    closed = true;
                    break;
                }
                text.push(b[i]);
                i += 1;
            }
            if !closed {
                return Err(invalid("unterminated SQL quote"));
            }
            out.push(Token {
                text: String::from_utf8(text).map_err(|_| invalid("invalid SQL UTF-8"))?,
                start,
                end: i,
                quoted: true,
            });
        } else {
            if b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] >= 128 {
                i += 1;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] >= 128) {
                    i += 1;
                }
            } else {
                i += 1;
            }
            out.push(Token {
                text: sql[start..i].into(),
                start,
                end: i,
                quoted: false,
            });
        }
    }
    Ok(out)
}
fn top_indices(t: &[Token]) -> Vec<usize> {
    let mut depth = 0;
    let mut out = Vec::new();
    for (i, t) in t.iter().enumerate() {
        if t.is(")") {
            depth -= 1;
        }
        if depth == 0 {
            out.push(i);
        }
        if t.is("(") {
            depth += 1;
        }
    }
    out
}
pub(crate) fn statements(sql: &str) -> rusqlite::Result<Vec<&str>> {
    let t = tokens(sql)?;
    let mut start = 0;
    let mut out = Vec::new();
    for i in top_indices(&t) {
        if t[i].is(";") {
            if !tokens(&sql[start..t[i].start])?.is_empty() {
                out.push(&sql[start..t[i].start]);
            }
            start = t[i].end;
        }
    }
    if !tokens(&sql[start..])?.is_empty() {
        out.push(&sql[start..]);
    }
    Ok(out)
}

pub(crate) fn application_columns(t: &Table) -> Vec<&Column> {
    match &t.application_key {
        Some(k) => k
            .columns
            .iter()
            .filter_map(|n| t.columns.iter().find(|c| c.name == *n))
            .collect(),
        None => pk_columns(t),
    }
}
pub(crate) fn inspect_key(c: &Connection, t: &Table) -> Result<ApplicationKey> {
    let columns = pk_columns(t)
        .iter()
        .map(|c| c.name.clone())
        .collect::<Vec<_>>();
    let mut key = ApplicationKey {
        collations: vec!["BINARY".into(); columns.len()],
        descending: vec![false; columns.len()],
        columns,
        strict: false,
        conflict: None,
        without_rowid: false,
    };
    let ts = tokens(&t.sql)?;
    if let Some(close) = top_indices(&ts).into_iter().find(|i| ts[*i].is(")")) {
        let options = &ts[close + 1..];
        key.strict = options.iter().any(|t| t.is("STRICT"));
        key.without_rowid = options
            .windows(2)
            .any(|w| w[0].is("WITHOUT") && w[1].is("ROWID"));
    }
    if let Some(pk) = ts
        .windows(2)
        .position(|p| p[0].is("PRIMARY") && p[1].is("KEY"))
    {
        let mut end = pk + 2;
        if ts.get(end).is_some_and(|t| t.is("(")) {
            let mut depth = 0;
            while end < ts.len() {
                if ts[end].is("(") {
                    depth += 1;
                }
                if ts[end].is(")") {
                    depth -= 1;
                }
                end += 1;
                if depth == 0 {
                    break;
                }
            }
        } else if ts.get(end).is_some_and(|t| t.is("ASC") || t.is("DESC")) {
            end += 1;
        }
        if ts.get(end).is_some_and(|t| t.is("ON"))
            && ts.get(end + 1).is_some_and(|t| t.is("CONFLICT"))
        {
            key.conflict = ts.get(end + 2).map(|t| t.text.to_ascii_uppercase());
        }
    }
    let mut q = c.prepare(&format!("PRAGMA index_list({})", quote(&t.name)))?;
    let names = q
        .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, String>(3)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (name, origin) in names {
        if origin != "pk" {
            continue;
        }
        let mut q = c.prepare(&format!("PRAGMA index_xinfo({})", quote(&name)))?;
        let entries = q
            .query_map([], |r| {
                Ok((
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, bool>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, bool>(5)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for (name, desc, coll, keycol) in entries {
            if !keycol {
                continue;
            }
            if let Some(i) = name.and_then(|n| key.columns.iter().position(|c| *c == n)) {
                key.collations[i] = coll;
                key.descending[i] = desc;
            }
        }
    }
    Ok(key)
}

/// Stable, length-prefixed encoding; numeric values use SQLite equality, and
/// built-in text collations are normalized before hashing.
pub(crate) fn uuid(table: &str, collations: &[String], values: &[Value]) -> Result<String> {
    if values.len() != collations.len() {
        return Err(Error::Invalid("application key width mismatch".into()));
    }
    let mut bytes = b"murmur/replication-primary-key/v1\0".to_vec();
    fn part(out: &mut Vec<u8>, value: &[u8]) {
        out.extend_from_slice(&(value.len() as u64).to_be_bytes());
        out.extend_from_slice(value);
    }
    part(&mut bytes, table.to_ascii_lowercase().as_bytes());
    bytes.extend_from_slice(&(values.len() as u64).to_be_bytes());
    for (value, collation) in values.iter().zip(collations) {
        match value {
            Value::Null => {
                return Err(Error::Invalid(
                    "application primary key must be supplied and non-null".into(),
                ));
            }
            Value::Integer(n) => {
                bytes.push(1);
                bytes.extend_from_slice(&n.to_be_bytes());
            }
            Value::Real(n)
                if n.is_finite()
                    && *n >= i64::MIN as f64
                    && *n < -(i64::MIN as f64)
                    && (*n as i64) as f64 == *n =>
            {
                bytes.push(1);
                bytes.extend_from_slice(&(*n as i64).to_be_bytes());
            }
            Value::Real(n) => {
                bytes.push(2);
                bytes.extend_from_slice(&n.to_bits().to_be_bytes());
            }
            Value::Text(s) => {
                bytes.push(3);
                let s = if collation.eq_ignore_ascii_case("NOCASE") {
                    let mut b = s.as_bytes().to_ascii_lowercase();
                    if let Some(n) = b.iter().position(|c| *c == 0) {
                        b[n..].fill(0);
                    }
                    String::from_utf8(b).map_err(|_| Error::Invalid("invalid NOCASE key".into()))?
                } else if collation.eq_ignore_ascii_case("RTRIM") {
                    s.trim_end_matches(' ').into()
                } else if collation.eq_ignore_ascii_case("BINARY") {
                    s.clone()
                } else {
                    return Err(Error::Invalid(
                        "unsupported application key collation".into(),
                    ));
                };
                part(&mut bytes, s.as_bytes());
            }
            Value::Blob(b) => {
                bytes.push(4);
                part(&mut bytes, b);
            }
        }
    }
    Ok(::uuid::Uuid::new_v5(&::uuid::Uuid::NAMESPACE_URL, &bytes)
        .hyphenated()
        .to_string())
}
pub(crate) fn validate_row(t: &Table, row: &RowState) -> Result<()> {
    if let Some(key) = &t.application_key {
        let values = key
            .columns
            .iter()
            .map(|name| {
                let col = t
                    .columns
                    .iter()
                    .find(|c| c.name == *name)
                    .ok_or(Error::SchemaMismatch)?;
                row.cells
                    .iter()
                    .find(|c| c.column == col.id)
                    .map(|c| c.value.clone())
                    .ok_or(Error::SchemaMismatch)
            })
            .collect::<Result<Vec<_>>>()?;
        let expected = Value::Text(uuid(&t.name, &key.collations, &values)?);
        let col = t
            .columns
            .iter()
            .find(|c| c.name == COLUMN)
            .ok_or(Error::SchemaMismatch)?;
        if row.key != [expected.clone()]
            || row
                .cells
                .iter()
                .find(|c| c.column == col.id)
                .map(|c| &c.value)
                != Some(&expected)
        {
            return Err(Error::Invalid(
                "invalid managed replication primary key".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn register(c: &Connection) -> Result<()> {
    c.create_scalar_function(
        "_murmur_uuid",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            if ctx.len() < 3 {
                return Err(invalid("invalid managed key arguments"));
            }
            let table = ctx.get::<String>(0)?;
            let collations: Vec<String> = serde_json::from_str(&ctx.get::<String>(1)?)
                .map_err(|_| invalid("invalid key collations"))?;
            if (2..ctx.len()).any(|i| matches!(ctx.get_raw(i), rusqlite::types::ValueRef::Null)) {
                return Err(missing_key());
            }
            let values = (2..ctx.len())
                .map(|i| Value::read(ctx.get_raw(i)))
                .collect::<rusqlite::Result<Vec<_>>>()?;
            uuid(&table, &collations, &values).map_err(|e| invalid(&e.to_string()))
        },
    )?;
    // Use SQLite's own numeric conversion instead of approximating affinity in Rust.
    unsafe extern "C" fn affinity(
        ctx: *mut rusqlite::ffi::sqlite3_context,
        n: i32,
        args: *mut *mut rusqlite::ffi::sqlite3_value,
    ) {
        use rusqlite::ffi::*;
        unsafe {
            if n != 2 {
                sqlite3_result_error(ctx, c"invalid affinity arguments".as_ptr(), -1);
                return;
            }
            let a = std::slice::from_raw_parts(args, 2);
            let kind = sqlite3_value_int(a[0]);
            let v = a[1];
            let original = sqlite3_value_type(v);
            if original == SQLITE_NULL {
                sqlite3_result_error(
                    ctx,
                    c"every application primary-key component must be supplied and non-null"
                        .as_ptr(),
                    -1,
                );
                sqlite3_result_error_code(ctx, SQLITE_CONSTRAINT_NOTNULL);
                return;
            }
            if kind == 1 && matches!(original, SQLITE_INTEGER | SQLITE_FLOAT) {
                let ptr = sqlite3_value_text(v);
                let len = sqlite3_value_bytes(v);
                sqlite3_result_text(ctx, ptr.cast(), len, SQLITE_TRANSIENT());
                return;
            }
            if kind >= 2 {
                let numeric = sqlite3_value_numeric_type(v);
                if matches!(numeric, SQLITE_INTEGER | SQLITE_FLOAT) {
                    if kind == 3 {
                        sqlite3_result_double(ctx, sqlite3_value_double(v));
                        return;
                    }
                    if numeric == SQLITE_FLOAT {
                        let x = sqlite3_value_double(v);
                        if x >= i64::MIN as f64 && x < -(i64::MIN as f64) && (x as i64) as f64 == x
                        {
                            sqlite3_result_int64(ctx, x as i64);
                            return;
                        }
                    }
                }
            }
            sqlite3_result_value(ctx, v);
        }
    }
    unsafe {
        let rc = rusqlite::ffi::sqlite3_create_function_v2(
            c.handle(),
            c"_murmur_affinity".as_ptr(),
            2,
            rusqlite::ffi::SQLITE_UTF8 | rusqlite::ffi::SQLITE_DETERMINISTIC,
            std::ptr::null_mut(),
            Some(affinity),
            None,
            None,
            None,
        );
        if rc != rusqlite::ffi::SQLITE_OK {
            return Err(Error::Invalid(
                "cannot register key affinity function".into(),
            ));
        }
    }
    Ok(())
}
fn affinity(c: &Column) -> i32 {
    let s = c.declared_type.to_ascii_uppercase();
    if s.contains("INT") {
        2
    } else if ["CHAR", "CLOB", "TEXT"].iter().any(|x| s.contains(x)) {
        1
    } else if s.is_empty() || s.contains("BLOB") {
        0
    } else if ["REAL", "FLOA", "DOUB"].iter().any(|x| s.contains(x)) {
        3
    } else {
        2
    }
}

/// Transform INSERT sources once, after binding, with materialized CTEs. The
/// returned statement remains a normal rusqlite Statement (including RETURNING).
pub(crate) fn rewrite(sql: &str, schema: &Schema) -> rusqlite::Result<String> {
    if !schema.tables.iter().any(|t| t.application_key.is_some()) {
        return Ok(sql.into());
    }
    let first = sql
        .trim_start()
        .split_ascii_whitespace()
        .next()
        .unwrap_or("");
    if ["SELECT", "UPDATE", "DELETE", "PRAGMA", "EXPLAIN"]
        .iter()
        .any(|word| first.eq_ignore_ascii_case(word))
    {
        return Ok(sql.into());
    }
    let t = tokens(sql)?;
    let tops = top_indices(&t);
    let Some(insert) = tops
        .iter()
        .copied()
        .find(|i| t[*i].is("INSERT") || t[*i].is("REPLACE"))
    else {
        return Ok(sql.into());
    };
    // Only the outer statement can be an INSERT.
    if insert != 0 && !t.first().is_some_and(|t| t.is("WITH")) {
        return Ok(sql.into());
    }
    let mut i = insert + 1;
    if t.get(i).is_some_and(|t| t.is("OR")) {
        i += 2;
    }
    if t.get(i).is_some_and(|t| t.is("INTO")) {
        i += 1;
    }
    let Some(name) = t.get(i) else {
        return Ok(sql.into());
    };
    i += 1;
    let name = if t.get(i).is_some_and(|t| t.is(".")) {
        i += 1;
        let n = t.get(i).ok_or_else(|| invalid("missing table name"))?;
        i += 1;
        n
    } else {
        name
    };
    let Some(table) = schema
        .tables
        .iter()
        .find(|tb| tb.name.eq_ignore_ascii_case(&name.text) && tb.application_key.is_some())
    else {
        return Ok(sql.into());
    };
    let key = table.application_key.as_ref().unwrap();
    if t.get(i).is_some_and(|t| t.is("AS")) {
        i += 2;
    }
    let mut columns = Vec::new();
    if t.get(i).is_some_and(|t| t.is("(")) {
        i += 1;
        while i < t.len() && !t[i].is(")") {
            columns.push(t[i].text.clone());
            i += 1;
            if t.get(i).is_some_and(|t| t.is(",")) {
                i += 1;
            } else if !t.get(i).is_some_and(|t| t.is(")")) {
                return Err(invalid("invalid INSERT column list"));
            }
        }
        i += 1;
    } else {
        columns = table
            .columns
            .iter()
            .filter(|c| c.name != COLUMN)
            .map(|c| c.name.clone())
            .collect();
    }
    if columns.iter().any(|n| n.eq_ignore_ascii_case(COLUMN)) {
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_AUTH),
            Some("id_murmur_primary_key is managed and read-only".into()),
        ));
    }
    if key
        .columns
        .iter()
        .any(|n| !columns.iter().any(|c| c.eq_ignore_ascii_case(n)))
    {
        return Err(missing_key());
    }
    let source = t
        .get(i)
        .ok_or_else(|| invalid("missing INSERT source"))?
        .start;
    if t[i].is("DEFAULT") {
        return Err(missing_key());
    }
    let suffix = tops
        .iter()
        .copied()
        .find(|j| {
            *j >= i
                && (t[*j].is("RETURNING")
                    || t[*j].is(";")
                    || (t[*j].is("ON") && t.get(*j + 1).is_some_and(|x| x.is("CONFLICT"))))
        })
        .map(|j| t[j].start)
        .unwrap_or(sql.len());
    let mut ordinal = 0;
    let (cte, normalized) = loop {
        let a = format!("_murmur_insert_input_{ordinal}");
        let b = format!("_murmur_insert_values_{ordinal}");
        if !t
            .iter()
            .any(|t| t.text.eq_ignore_ascii_case(&a) || t.text.eq_ignore_ascii_case(&b))
        {
            break (a, b);
        }
        ordinal += 1;
    };
    let names = (0..columns.len())
        .map(|j| format!("v{j}"))
        .collect::<Vec<_>>();
    let projection = columns
        .iter()
        .enumerate()
        .map(|(j, n)| {
            let c = table
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(n))
                .ok_or_else(|| invalid("unknown INSERT column"))?;
            Ok(if key.columns.contains(&c.name) {
                format!("_murmur_affinity({},v{j}) AS v{j}", affinity(c))
            } else {
                format!("v{j}")
            })
        })
        .collect::<rusqlite::Result<Vec<_>>>()?
        .join(",");
    let args = key
        .columns
        .iter()
        .map(|n| {
            format!(
                "v{}",
                columns
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(n))
                    .unwrap()
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let prefix = if insert == 0 {
        "WITH ".into()
    } else {
        format!("{},", sql[..t[insert].start].trim_end())
    };
    let head = &sql[t[insert].start..name.end];
    // Preserve an INSERT table alias, if present.
    let alias_start = t.iter().position(|x| x.start == name.start).unwrap() + 1;
    let alias = if t.get(alias_start).is_some_and(|x| x.is("AS")) {
        format!(" AS {}", quote(&t[alias_start + 1].text))
    } else {
        String::new()
    };
    Ok(format!(
        "{prefix}{cte}({}) AS MATERIALIZED ({}), {normalized} AS MATERIALIZED (SELECT {projection} FROM {cte}) {head}{alias} ({},{}) SELECT {},_murmur_uuid({},{},{args}) FROM {normalized} WHERE true {}",
        names.join(","),
        &sql[source..suffix],
        columns
            .iter()
            .map(|n| quote(n))
            .collect::<Vec<_>>()
            .join(","),
        quote(COLUMN),
        names.join(","),
        literal(&table.name),
        literal(&serde_json::to_string(&key.collations).unwrap()),
        &sql[suffix..]
    ))
}

/// Rebuild definitions without losing defaults, collations, or non-key columns.
pub(crate) fn definition(t: &Table, key: &ApplicationKey, enable: bool) -> Result<String> {
    let tokens = tokens(&t.sql)?;
    let open = tokens
        .iter()
        .position(|t| t.is("("))
        .ok_or(Error::SchemaMismatch)?;
    let mut depth = 1;
    let mut start = open + 1;
    let mut definitions = Vec::new();
    let mut close = None;
    for i in open + 1..tokens.len() {
        if tokens[i].is("(") {
            depth += 1;
        }
        if tokens[i].is(")") {
            depth -= 1;
        }
        if depth == 0 || (depth == 1 && tokens[i].is(",")) {
            let part = &tokens[start..i];
            if !part.is_empty() {
                let table_pk = part
                    .first()
                    .is_some_and(|t| t.is("PRIMARY") || t.is("CONSTRAINT"));
                let managed_unique = !enable && part.first().is_some_and(|t| t.is("UNIQUE"));
                if !table_pk && !managed_unique && !part[0].text.eq_ignore_ascii_case(COLUMN) {
                    let mut text = t.sql[part[0].start..part.last().unwrap().end].to_owned();
                    if let Some(pk) = part
                        .windows(2)
                        .position(|p| p[0].is("PRIMARY") && p[1].is("KEY"))
                    {
                        let mut end = pk + 2;
                        if part.get(end).is_some_and(|t| t.is("ASC") || t.is("DESC")) {
                            end += 1;
                        }
                        if part.get(end).is_some_and(|t| t.is("ON")) {
                            end += 3;
                        }
                        let begin = if pk >= 2 && part[pk - 2].is("CONSTRAINT") {
                            pk - 2
                        } else {
                            pk
                        };
                        text.replace_range(
                            part[begin].start - part[0].start
                                ..part
                                    .get(end)
                                    .map(|t| t.start)
                                    .unwrap_or(part.last().unwrap().end)
                                    - part[0].start,
                            "",
                        );
                    }
                    if enable
                        && key
                            .columns
                            .iter()
                            .any(|n| n.eq_ignore_ascii_case(&part[0].text))
                        && !crate::schema::keywords(&text)
                            .windows(2)
                            .any(|p| p == ["NOT", "NULL"])
                    {
                        text.push_str(" NOT NULL");
                    }
                    definitions.push(text);
                }
            }
            start = i + 1;
            if depth == 0 {
                close = Some(i);
                break;
            }
        }
    }
    if close.is_none() {
        return Err(Error::SchemaMismatch);
    }
    let columns = key
        .columns
        .iter()
        .enumerate()
        .map(|(i, n)| {
            format!(
                "{} COLLATE {} {}",
                quote(n),
                quote(&key.collations[i]),
                if key.descending[i] { "DESC" } else { "ASC" }
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    if enable {
        definitions.push(format!("{} TEXT NOT NULL PRIMARY KEY", quote(COLUMN)));
        definitions.push(format!("UNIQUE ({columns}){}", conflict(key)));
    } else {
        definitions.push(format!("PRIMARY KEY ({columns}){}", conflict(key)));
    }
    Ok(format!(
        "CREATE TABLE {} ({}){}",
        quote(&t.name),
        definitions.join(","),
        t.sql[tokens[close.unwrap()].end..].trim_end_matches(';')
    ))
}

/// Column definitions from SQLite's own schema SQL, retaining defaults and
/// collations during canonical schema exchange. Table constraints are rebuilt
/// from validated key metadata.
pub(crate) fn column_definitions(sql: &str) -> Result<std::collections::BTreeMap<String, String>> {
    let ts = tokens(sql)?;
    let open = ts
        .iter()
        .position(|t| t.is("("))
        .ok_or(Error::SchemaMismatch)?;
    let mut depth = 1;
    let mut start = open + 1;
    let mut out = std::collections::BTreeMap::new();
    for i in open + 1..ts.len() {
        if ts[i].is("(") {
            depth += 1;
        }
        if ts[i].is(")") {
            depth -= 1;
        }
        if depth == 0 || (depth == 1 && ts[i].is(",")) {
            let part = &ts[start..i];
            if let Some(name) = part.first() {
                if !["PRIMARY", "UNIQUE", "CONSTRAINT", "CHECK", "FOREIGN"]
                    .iter()
                    .any(|s| name.is(s))
                {
                    if part.iter().any(|t| t.is(";")) {
                        return Err(Error::SchemaMismatch);
                    }
                    let mut text = sql[name.start..part.last().unwrap().end].to_owned();
                    if let Some(pk) = part
                        .windows(2)
                        .position(|p| p[0].is("PRIMARY") && p[1].is("KEY"))
                    {
                        let mut end = pk + 2;
                        if part.get(end).is_some_and(|t| t.is("ASC") || t.is("DESC")) {
                            end += 1;
                        }
                        if part.get(end).is_some_and(|t| t.is("ON")) {
                            end += 3;
                        }
                        let begin = if pk >= 2 && part[pk - 2].is("CONSTRAINT") {
                            pk - 2
                        } else {
                            pk
                        };
                        text.replace_range(
                            part[begin].start - name.start
                                ..part
                                    .get(end)
                                    .map(|t| t.start)
                                    .unwrap_or(part.last().unwrap().end)
                                    - name.start,
                            "",
                        );
                    }
                    if out
                        .insert(name.text.clone(), text.trim().to_owned())
                        .is_some()
                    {
                        return Err(Error::SchemaMismatch);
                    }
                }
            }
            start = i + 1;
            if depth == 0 {
                return Ok(out);
            }
        }
    }
    Err(Error::SchemaMismatch)
}
pub(crate) fn validate_table(t: &Table) -> Result<()> {
    if let Some(key) = &t.application_key {
        if key.columns.is_empty()
            || key.columns.len() != key.collations.len()
            || key.columns.len() != key.descending.len()
            || key
                .columns
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != key.columns.len()
            || key.columns.iter().any(|n| {
                n.eq_ignore_ascii_case(COLUMN)
                    || !t.columns.iter().any(|c| c.name == *n && c.not_null)
            })
            || key.conflict.as_ref().is_some_and(|c| {
                !["ROLLBACK", "ABORT", "FAIL", "IGNORE", "REPLACE"].contains(&c.as_str())
            })
            || key.collations.iter().any(|c| {
                !["BINARY", "NOCASE", "RTRIM"]
                    .iter()
                    .any(|s| c.eq_ignore_ascii_case(s))
            })
        {
            return Err(Error::SchemaMismatch);
        }
        let pk = pk_columns(t);
        if pk.len() != 1
            || pk[0].name != COLUMN
            || !pk[0].declared_type.eq_ignore_ascii_case("TEXT")
            || !pk[0].not_null
        {
            return Err(Error::SchemaMismatch);
        }
    }
    Ok(())
}

pub(crate) fn conflict(key: &ApplicationKey) -> String {
    key.conflict
        .as_ref()
        .map(|c| format!(" ON CONFLICT {c}"))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Database, Migration, OpenMode, Options};
    #[test]
    fn uuid_reference_vector_and_component_boundaries() -> Result<()> {
        assert_eq!(
            uuid("items", &["BINARY".into()], &[Value::Integer(1)])?,
            "80c7e056-7cbe-5d14-a89f-a823fb14ba45"
        );
        assert_eq!(
            uuid("ITEMS", &["BINARY".into()], &[Value::Real(1.0)])?,
            uuid("items", &["BINARY".into()], &[Value::Integer(1)])?
        );
        let collations = vec!["BINARY".into(); 2];
        assert_ne!(
            uuid(
                "t",
                &collations,
                &[Value::Text("ab".into()), Value::Text("c".into())]
            )?,
            uuid(
                "t",
                &collations,
                &[Value::Text("a".into()), Value::Text("bc".into())]
            )?
        );
        Ok(())
    }
    #[test]
    fn failed_config_publication_recovers_using_authoritative_flags() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("db");
        let db = Database::open(
            &path,
            [7; 32],
            Options {
                mode: OpenMode::CreateNew,
                ..Options::default()
            },
        )?;
        db.migrate(Migration::new(
            "CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT)",
        ))?;
        db.transaction(|s| {
            s.execute("INSERT INTO items VALUES(1,'kept')", [])?;
            Ok(())
        })?;
        // An existing directory cannot be atomically replaced by config.toml.
        assert!(
            db.set_replication_config("items", true, temp.path())
                .is_err()
        );
        assert!(db.retry_storage().is_err());
        assert!(
            db.transaction(|s| {
                s.execute("INSERT INTO items VALUES(2,'blocked')", [])?;
                Ok(())
            })
            .is_err()
        );
        let _ = db.close();
        let db = Database::open_with_flags(
            &path,
            [7; 32],
            Options::default(),
            std::collections::BTreeMap::from([(
                id(&[b"sqlite-table/v4/".as_slice(), b"items"].concat()),
                false,
            )]),
        )?;
        assert!(!db.replication_enabled("items")?);
        assert!(db.schema()?.tables[0].application_key.is_none());
        assert_eq!(
            db.read(
                |s| Ok(s.query_row("SELECT v FROM items WHERE id=1", [], |r| r
                    .get::<_, String>(0))?)
            )?,
            "kept"
        );
        db.close()
    }
}

#[cfg(feature = "replication")]
pub(crate) fn resolve_key(c: &Connection, t: &Table, values: Vec<Value>) -> Result<Vec<Value>> {
    let columns = application_columns(t);
    if values.len() != columns.len() || values.iter().any(|v| matches!(v, Value::Null)) {
        return Err(Error::Invalid(
            "application primary-key width or value mismatch".into(),
        ));
    }
    let Some(key) = &t.application_key else {
        return Ok(values);
    };
    let args = columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("_murmur_affinity({},?{})", affinity(c), i + 1))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT _murmur_uuid({},{},{args})",
        literal(&t.name),
        literal(&serde_json::to_string(&key.collations).unwrap())
    );
    Ok(vec![Value::Text(c.query_row(
        &sql,
        rusqlite::params_from_iter(values),
        |r| r.get(0),
    )?)])
}

pub(crate) fn references_managed_column(sql: &str) -> rusqlite::Result<bool> {
    Ok(tokens(sql)?
        .iter()
        .any(|t| t.text.eq_ignore_ascii_case(COLUMN) && sql.as_bytes()[t.start] != b'\''))
}

#[cfg(feature = "replication")]
pub(crate) fn application_predicate(t: &Table) -> String {
    application_columns(t)
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let collation = t
                .application_key
                .as_ref()
                .map(|key| format!(" COLLATE {}", quote(&key.collations[i])))
                .unwrap_or_default();
            format!("{}{collation} IS ?", quote(&c.name))
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

//! Best-effort parameter type inference for Describe responses.
//!
//! Drivers encode bound values against the parameter types we report; always
//! answering UNKNOWN makes every typed value fail client-side. Like Postgres,
//! this module infers `$N`/`?N` types from context:
//!
//! - `INSERT INTO t [(cols)] VALUES (...)`: positional column mapping.
//! - Comparisons `col <op> $N` / `$N <op> col`, `SET col = $N`, `col IN
//!   ($N, ...)`, `col BETWEEN $N AND $N`: the other side's column type.
//! - `col LIKE $N`: TEXT (patterns are text).
//!
//! Identifiers resolve against the statement's table scope (`FROM`/`JOIN`
//! with aliases, `UPDATE`, `INSERT INTO`, `DELETE FROM`, `TABLE`). Ambiguous
//! or unknown references stay UNKNOWN. Inference never fails the describe.
use murmur::Schema;
use pgwire::api::Type;

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Word(String),
    Param(usize),
    Quoted(String),
    Punct(char),
}

/// Lex words, parameters, quoted identifiers, and punctuation. String
/// literals, numbers, and comments carry no inference signal and are skipped.
fn lex(sql: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            _ if c.is_whitespace() || c == ';' => {}
            '-' if chars.peek() == Some(&'-') => {
                for c in chars.by_ref() {
                    if c == '\n' {
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut prev = '\0';
                for c in chars.by_ref() {
                    if prev == '*' && c == '/' {
                        break;
                    }
                    prev = c;
                }
            }
            '\'' => {
                // String literal: skip, honouring '' escapes.
                loop {
                    match chars.next() {
                        None => break,
                        Some('\'') if chars.peek() == Some(&'\'') => {
                            chars.next();
                        }
                        Some('\'') => break,
                        Some(_) => {}
                    }
                }
            }
            '"' | '`' => {
                let quote = c;
                let mut ident = String::new();
                loop {
                    match chars.next() {
                        None => break,
                        Some(q) if q == quote => {
                            if chars.peek() == Some(&quote) {
                                chars.next();
                                ident.push(quote);
                            } else {
                                break;
                            }
                        }
                        Some(other) => ident.push(other),
                    }
                }
                tokens.push(Token::Quoted(ident));
            }
            '[' => {
                let mut ident = String::new();
                for c in chars.by_ref() {
                    if c == ']' {
                        break;
                    }
                    ident.push(c);
                }
                tokens.push(Token::Quoted(ident));
            }
            '$' => {
                let mut digits = String::new();
                while chars.peek().is_some_and(|p| p.is_ascii_digit()) {
                    digits.push(chars.next().unwrap());
                }
                if let Ok(index) = digits.parse::<usize>() {
                    if index > 0 {
                        tokens.push(Token::Param(index));
                        continue;
                    }
                }
                // A bare `$` is content-free here; ignore it.
            }
            '?' => {
                let mut digits = String::new();
                while chars.peek().is_some_and(|p| p.is_ascii_digit()) {
                    digits.push(chars.next().unwrap());
                }
                if let Ok(index) = digits.parse::<usize>() {
                    if index > 0 {
                        tokens.push(Token::Param(index));
                    }
                }
                // Anonymous `?` needs positional counting; out of scope.
            }
            _ if c.is_ascii_alphabetic() || c == '_' => {
                let mut word = String::new();
                word.push(c);
                while chars.peek().is_some_and(|p| p.is_ascii_alphanumeric() || *p == '_' || *p == '$') {
                    word.push(chars.next().unwrap());
                }
                tokens.push(Token::Word(word));
            }
            _ if c.is_ascii_digit() || (c == '.' && chars.peek().is_some_and(|p| p.is_ascii_digit())) => {
                // Number: skip (a leading `.5` keeps its dot out of the way).
                let mut dot = c == '.';
                while let Some(p) = chars.peek() {
                    if p.is_ascii_alphanumeric() || (!dot && *p == '.') {
                        dot = dot || *p == '.';
                        chars.next();
                    } else {
                        break;
                    }
                }
            }
            _ => tokens.push(Token::Punct(c)),
        }
    }
    tokens
}

fn word_upper(token: &Token) -> Option<String> {
    match token {
        Token::Word(w) => Some(w.to_ascii_uppercase()),
        _ => None,
    }
}

/// Words that can never be a table alias in a factor position.
fn is_factor_stop(word: &str) -> bool {
    matches!(
        word,
        "WHERE" | "GROUP" | "ORDER" | "LIMIT" | "OFFSET" | "SET" | "VALUES" | "ON" | "USING"
            | "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "OUTER" | "CROSS" | "NATURAL"
            | "UNION" | "EXCEPT" | "INTERSECT" | "RETURNING" | "WINDOW" | "HAVING" | "FETCH"
            | "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "TABLE" | "AS" | "AND" | "OR" | "NOT"
            | "NULL" | "IS" | "IN" | "LIKE" | "MATCH" | "BETWEEN" | "EXISTS" | "CASE" | "WHEN" | "THEN"
            | "ELSE" | "END" | "BY" | "ASC" | "DESC"
    )
}

/// One table in scope: schema index plus the names that address it.
struct ScopeTable {
    index: usize,
    names: Vec<String>,
}

/// Collect table factors from FROM/JOIN/UPDATE/INSERT INTO/DELETE FROM/TABLE.
fn scope_tables(tokens: &[Token], schema: &Schema) -> Vec<ScopeTable> {
    let mut scope = Vec::new();
    let mut idx = 0;
    while idx < tokens.len() {
        let takes_factor = match word_upper(&tokens[idx]).as_deref() {
            Some("FROM") | Some("JOIN") | Some("UPDATE") | Some("TABLE") => true,
            Some("INTO") => {
                idx > 0 && word_upper(&tokens[idx - 1]).as_deref() == Some("INSERT")
            }
            _ => false,
        };
        if !takes_factor {
            idx += 1;
            continue;
        }
        // `TABLE t` only takes a factor as the statement head.
        if word_upper(&tokens[idx]).as_deref() == Some("TABLE") && idx != 0 {
            idx += 1;
            continue;
        }
        idx += 1;
        // Possibly comma-separated factors.
        loop {
            // Dotted name: keep the last part.
            let mut name: Option<String> = None;
            loop {
                match tokens.get(idx) {
                    Some(Token::Word(w)) | Some(Token::Quoted(w)) => {
                        name = Some(w.clone());
                        idx += 1;
                    }
                    _ => break,
                }
                if tokens.get(idx) == Some(&Token::Punct('.')) {
                    idx += 1;
                    continue;
                }
                break;
            }
            let Some(name) = name else { break };
            // Optional alias.
            let mut alias: Option<String> = None;
            if let Some(next) = tokens.get(idx) {
                if word_upper(next).as_deref() == Some("AS") {
                    if let Some(Token::Word(a)) | Some(Token::Quoted(a)) = tokens.get(idx + 1) {
                        alias = Some(a.clone());
                        idx += 2;
                    } else {
                        idx += 1;
                    }
                } else if let Token::Word(a) | Token::Quoted(a) = next {
                    if !is_factor_stop(&a.to_ascii_uppercase()) {
                        alias = Some(a.clone());
                        idx += 1;
                    }
                }
            }
            if let Some(table) = schema.tables.iter().position(|t| t.name.eq_ignore_ascii_case(&name)) {
                let mut names = vec![schema.tables[table].name.clone()];
                if let Some(alias) = alias {
                    if !alias.eq_ignore_ascii_case(&name) {
                        names.push(alias);
                    }
                }
                scope.push(ScopeTable { index: table, names });
            }
            if tokens.get(idx) == Some(&Token::Punct(',')) {
                idx += 1;
                continue;
            }
            break;
        }
    }
    scope
}

/// A column reference: optional qualifier plus column name.
fn column_ref(tokens: &[Token], idx: usize) -> Option<(Option<String>, String, usize)> {
    let (mut qualifier, mut name, mut next) = match tokens.get(idx) {
        Some(Token::Word(w)) | Some(Token::Quoted(w)) => (None, w.clone(), idx + 1),
        _ => return None,
    };
    if tokens.get(next) == Some(&Token::Punct('.')) {
        match tokens.get(next + 1) {
            Some(Token::Word(w)) | Some(Token::Quoted(w)) => {
                qualifier = Some(name);
                name = w.clone();
                next += 2;
            }
            _ => return None,
        }
    }
    Some((qualifier, name, next))
}

/// Resolve a column reference to a schema column. Unqualified references must
/// match exactly one table in scope.
fn resolve<'a>(
    scope: &[ScopeTable],
    schema: &'a Schema,
    qualifier: Option<&str>,
    name: &str,
) -> Option<&'a murmur::Column> {
    let mut found = None;
    for table in scope {
        let addressed = match qualifier {
            Some(q) => table.names.iter().any(|n| n.eq_ignore_ascii_case(q)),
            None => true,
        };
        if !addressed {
            continue;
        }
        if let Some(column) = schema.tables[table.index]
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
        {
            if qualifier.is_none() && found.is_some() {
                return None; // ambiguous
            }
            found = Some(column);
            if qualifier.is_some() {
                return found;
            }
        }
    }
    found
}

fn column_type(column: &murmur::Column) -> Type {
    // BOOLEAN storage has no exact wire twin for result columns, but a bound
    // boolean must still encode; parameters alone map it to BOOL.
    if column.declared_type.eq_ignore_ascii_case("BOOL")
        || column.declared_type.eq_ignore_ascii_case("BOOLEAN")
    {
        return Type::BOOL;
    }
    crate::server::query::decltype_to_type(Some(&column.declared_type))
}

fn is_comparison(tokens: &[Token], idx: usize) -> Option<usize> {
    let (first, second) = (tokens.get(idx), tokens.get(idx + 1));
    match (first, second) {
        (Some(Token::Punct('=')) , _) => Some(idx + 1),
        (Some(Token::Punct('<')), Some(Token::Punct('=' | '>'))) => Some(idx + 2),
        (Some(Token::Punct('>')), Some(Token::Punct('='))) => Some(idx + 2),
        (Some(Token::Punct('<' | '>')), _) => Some(idx + 1),
        (Some(Token::Punct('!')), Some(Token::Punct('='))) => Some(idx + 2),
        _ => None,
    }
}

/// Infer parameter types positionally for `INSERT INTO t [(cols)] VALUES ...`.
fn insert_assignments(
    tokens: &[Token],
    schema: &Schema,
    max_param: usize,
) -> Vec<Option<Type>> {
    let mut inferred = vec![None; max_param];
    let mut idx = 0;
    while idx < tokens.len() {
        let is_insert = word_upper(&tokens[idx]).as_deref() == Some("INSERT")
            && tokens.get(idx + 1).and_then(|t| word_upper(t)).as_deref() == Some("INTO");
        if !is_insert {
            idx += 1;
            continue;
        }
        // Table name after INTO (last dotted part).
        let mut cursor = idx + 2;
        let mut name: Option<&str> = None;
        loop {
            match tokens.get(cursor) {
                Some(Token::Word(w)) | Some(Token::Quoted(w)) => {
                    name = Some(w);
                    cursor += 1;
                }
                _ => break,
            }
            if tokens.get(cursor) == Some(&Token::Punct('.')) {
                cursor += 1;
                continue;
            }
            break;
        }
        let table = name.and_then(|name| {
            schema.tables.iter().find(|t| t.name.eq_ignore_ascii_case(name))
        });
        // Optional column list.
        let mut columns: Vec<String> = Vec::new();
        if tokens.get(cursor) == Some(&Token::Punct('(')) {
            cursor += 1;
            loop {
                match tokens.get(cursor) {
                    Some(Token::Word(w)) | Some(Token::Quoted(w)) => {
                        columns.push(w.clone());
                        cursor += 1;
                    }
                    _ => break,
                }
                if tokens.get(cursor) == Some(&Token::Punct(',')) {
                    cursor += 1;
                    continue;
                }
                break;
            }
            if tokens.get(cursor) == Some(&Token::Punct(')')) {
                cursor += 1;
            }
        }
        // Every top-level VALUES tuple maps positionally.
        while cursor < tokens.len() {
            if word_upper(&tokens[cursor]).as_deref() != Some("VALUES") {
                cursor += 1;
                continue;
            }
            cursor += 1;
            while tokens.get(cursor) == Some(&Token::Punct('(')) {
                cursor += 1;
                let mut position = 0;
                let mut depth = 0;
                let mut item: Vec<Token> = Vec::new();
                loop {
                    match tokens.get(cursor) {
                        None => break,
                        Some(Token::Punct('(')) => {
                            depth += 1;
                            item.push(tokens[cursor].clone());
                            cursor += 1;
                        }
                        Some(Token::Punct(')')) if depth > 0 => {
                            depth -= 1;
                            item.push(tokens[cursor].clone());
                            cursor += 1;
                        }
                        Some(Token::Punct(')')) => {
                            assign_position(&mut inferred, table, &columns, position, &item);
                            cursor += 1;
                            break;
                        }
                        Some(Token::Punct(',')) if depth == 0 => {
                            assign_position(&mut inferred, table, &columns, position, &item);
                            position += 1;
                            item.clear();
                            cursor += 1;
                        }
                        Some(token) => {
                            item.push(token.clone());
                            cursor += 1;
                        }
                    }
                }
                if tokens.get(cursor) == Some(&Token::Punct(',')) {
                    cursor += 1;
                    continue;
                }
                break;
            }
        }
        idx = cursor.max(idx + 1);
    }
    inferred
}

/// Assign a lone `$N` tuple item to its target column.
fn assign_position(
    inferred: &mut [Option<Type>],
    table: Option<&murmur::Table>,
    columns: &[String],
    position: usize,
    item: &[Token],
) {
    let [Token::Param(index)] = item else { return };
    let index = *index;
    // This lexer also accepts `?N`, which the placeholder rewriter does
    // not count, so an index can exceed the result slots. Clamp instead
    // of panicking; unknown slots stay UNKNOWN.
    if index < 1 || index > inferred.len() {
        return;
    }
    let Some(table) = table else { return };
    let column = if columns.is_empty() {
        table.columns.get(position)
    } else {
        columns.get(position).and_then(|name| {
            table.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name))
        })
    };
    if let Some(column) = column {
        let slot = &mut inferred[index - 1];
        if slot.is_none() {
            *slot = Some(column_type(column));
        }
    }
}

/// Infer parameter types. Returns one entry per `$N` (1-based); `None` stays
/// UNKNOWN.
pub(crate) fn infer_param_types(schema: &Schema, sql: &str, max_param: usize) -> Vec<Option<Type>> {
    if max_param == 0 {
        return Vec::new();
    }
    // Query-only descriptions of derived TEMP tables. Never publish these
    // synthetic tables to application schema, persistence, or replication.
    let mut query_schema = schema.clone();
    for table in &schema.tables {
        for column in &table.columns {
            if column.vector.is_some() {
                let columns = [(column.name.as_str(), "BLOB"), ("rowid", "INTEGER"), ("k", "INTEGER"), ("distance", "REAL")]
                    .into_iter().map(|(name, ty)| murmur::Column {
                        vector: None, fts: false, policy: murmur::MergePolicy::Lww,
                        id: String::new(), name: name.into(), primary_key: 0, declared_type: ty.into(), not_null: false,
                    }).collect();
                query_schema.tables.push(murmur::Table { application_key: None, id: String::new(),
                    name: murmur::host::vector_index_name(table, column), sql: String::new(), columns });
            }
        }
    }
    let schema = &query_schema;
    let tokens = lex(sql);
    let scope = scope_tables(&tokens, schema);
    let mut inferred = insert_assignments(&tokens, schema, max_param);

    let mut assign = |index: usize, ty: Type| {
        if index >= 1 && index <= max_param {
            let slot = &mut inferred[index - 1];
            if slot.is_none() {
                *slot = Some(ty);
            }
        }
    };

    let mut idx = 0;
    while idx < tokens.len() {
        // `ref <cmp> $N` and `$N <cmp> ref`.
        if let Some((qualifier, name, next)) = column_ref(&tokens, idx) {
            if let Some(after) = is_comparison(&tokens, next) {
                if let Some(Token::Param(index)) = tokens.get(after) {
                    if let Some(column) = resolve(&scope, schema, qualifier.as_deref(), &name) {
                        assign(*index, column_type(column));
                    }
                }
            }
            // `ref LIKE/MATCH $N`: patterns and FTS queries are text.
            if matches!(word_upper(tokens.get(next).unwrap_or(&Token::Punct(','))).as_deref(), Some("LIKE" | "MATCH")) {
                if let Some(Token::Param(index)) = tokens.get(next + 1) {
                    assign(*index, Type::TEXT);
                }
            }
            // `ref IN ($N, ...)` and `ref BETWEEN $N AND $N`.
            if word_upper(tokens.get(next).unwrap_or(&Token::Punct(','))).as_deref() == Some("IN") {
                if tokens.get(next + 1) == Some(&Token::Punct('(')) {
                    let mut cursor = next + 2;
                    let mut depth = 0;
                    while let Some(token) = tokens.get(cursor) {
                        match token {
                            Token::Punct('(') => depth += 1,
                            Token::Punct(')') if depth > 0 => depth -= 1,
                            Token::Punct(')') => break,
                            Token::Param(index) if depth == 0 => {
                                if let Some(column) =
                                    resolve(&scope, schema, qualifier.as_deref(), &name)
                                {
                                    assign(*index, column_type(column));
                                }
                            }
                            _ => {}
                        }
                        cursor += 1;
                    }
                }
            }
            if word_upper(tokens.get(next).unwrap_or(&Token::Punct(','))).as_deref() == Some("BETWEEN") {
                let pair = (tokens.get(next + 1), tokens.get(next + 2), tokens.get(next + 3));
                if let (Some(Token::Param(first)), Some(middle), Some(Token::Param(second))) = pair {
                    if word_upper(middle).as_deref() == Some("AND") {
                        if let Some(column) = resolve(&scope, schema, qualifier.as_deref(), &name) {
                            let ty = column_type(column);
                            assign(*first, ty.clone());
                            assign(*second, ty);
                        }
                    }
                }
            }
        }
        if let Some(Token::Param(index)) = tokens.get(idx) {
            if let Some(after) = is_comparison(&tokens, idx + 1) {
                if let Some((qualifier, name, _)) = column_ref(&tokens, after) {
                    if let Some(column) = resolve(&scope, schema, qualifier.as_deref(), &name) {
                        assign(*index, column_type(column));
                    }
                }
            }
        }
        // `LIMIT $N` / `OFFSET $N` (and the comma form `LIMIT $N, $M`):
        // counts are always integers.
        if matches!(word_upper(tokens.get(idx).unwrap_or(&Token::Punct(','))).as_deref(), Some("LIMIT" | "OFFSET")) {
            let mut cursor = idx + 1;
            while let Some(token) = tokens.get(cursor) {
                match token {
                    Token::Param(index) => {
                        assign(*index, Type::INT8);
                        cursor += 1;
                    }
                    Token::Punct(',') => cursor += 1,
                    _ => break,
                }
            }
        }
        idx += 1;
    }
    inferred
}

#[cfg(test)]
mod tests {
    use super::*;
    use murmur::{Column, MergePolicy, Table};

    fn schema() -> Schema {
        let column = |name: &str, declared_type: &str| Column { vector: None, fts: false,
            policy: MergePolicy::Lww,
            id: name.into(),
            name: name.into(),
            primary_key: 0,
            declared_type: declared_type.into(),
            not_null: false,
        };
        Schema {
            table_replication: Default::default(),
            tables: vec![
                Table {
                    application_key: None,
                    id: "items".into(),
                    name: "items".into(),
                    sql: String::new(),
                    columns: vec![
                        column("id", "INTEGER"),
                        column("name", "TEXT"),
                        column("score", "REAL"),
                        column("payload", "BLOB"),
                    ],
                },
                Table {
                    application_key: None,
                    id: "audit".into(),
                    name: "audit".into(),
                    sql: String::new(),
                    columns: vec![column("id", "INTEGER"), column("note", "TEXT")],
                },
            ],
            indexes: Vec::new(),
            history: [0; 32],
        }
    }

    #[test]
    fn insert_and_comparison_patterns() {
        let schema = schema();
        let infer = |sql: &str, max: usize| infer_param_types(&schema, sql, max);
        assert_eq!(
            infer("INSERT INTO items VALUES ($1, $2, $3, $4)", 4),
            [Some(Type::INT8), Some(Type::TEXT), Some(Type::FLOAT8), Some(Type::BYTEA)]
        );
        assert_eq!(
            infer("INSERT INTO items (name, id) VALUES ($1, $2)", 2),
            [Some(Type::TEXT), Some(Type::INT8)]
        );
        assert_eq!(
            infer("INSERT INTO items VALUES ($1, 'x'), ($2, 'y')", 2),
            [Some(Type::INT8), Some(Type::INT8)]
        );
        assert_eq!(
            infer("SELECT * FROM items WHERE id = $1", 1),
            [Some(Type::INT8)]
        );
        assert_eq!(
            infer("SELECT * FROM items WHERE $1 = name", 1),
            [Some(Type::TEXT)]
        );
        assert_eq!(
            infer("UPDATE items SET score = $1 WHERE id = $2", 2),
            [Some(Type::FLOAT8), Some(Type::INT8)]
        );
        assert_eq!(
            infer("SELECT * FROM items WHERE id IN ($1, $2)", 2),
            [Some(Type::INT8), Some(Type::INT8)]
        );
        assert_eq!(
            infer("SELECT * FROM items WHERE id BETWEEN $1 AND $2", 2),
            [Some(Type::INT8), Some(Type::INT8)]
        );
        assert_eq!(infer("SELECT * FROM items WHERE name LIKE $1", 1), [Some(Type::TEXT)]);
        assert_eq!(
            infer("SELECT * FROM items i WHERE i.id = $1", 1),
            [Some(Type::INT8)]
        );
        // Unknown tables, unknown columns, and ambiguous references stay UNKNOWN.
        assert_eq!(infer("SELECT * FROM missing WHERE id = $1", 1), [None]);
        assert_eq!(infer("SELECT * FROM items WHERE nope = $1", 1), [None]);
        assert_eq!(
            infer("SELECT * FROM items, audit WHERE id = $1", 1),
            [None]
        );
        assert_eq!(
            infer("SELECT * FROM items, audit WHERE items.id = $1", 1),
            [Some(Type::INT8)]
        );
        // Quoted literals never leak parameters.
        assert_eq!(infer("SELECT * FROM items WHERE name = '$1'", 0), Vec::new());
    }

    #[test]
    fn limit_and_offset_params_are_integers() {
        let schema = schema();
        let infer = |sql: &str, max: usize| infer_param_types(&schema, sql, max);
        assert_eq!(
            infer("SELECT * FROM items WHERE id >= $1 ORDER BY id LIMIT $2", 2),
            [Some(Type::INT8), Some(Type::INT8)]
        );
        assert_eq!(
            infer("SELECT * FROM items LIMIT $1 OFFSET $2", 2),
            [Some(Type::INT8), Some(Type::INT8)]
        );
        assert_eq!(
            infer("SELECT * FROM items LIMIT $1, $2", 2),
            [Some(Type::INT8), Some(Type::INT8)]
        );
        // A LIMIT scan never crosses into the next clause.
        assert_eq!(
            infer("SELECT * FROM items LIMIT 10", 0),
            Vec::new()
        );
    }

    #[test]
    fn out_of_range_params_never_panic() {
        let schema = schema();
        let infer = |sql: &str, max: usize| infer_param_types(&schema, sql, max);
        // `?N` is lexed but not counted by the placeholder rewriter, so it
        // can exceed the slot count; it must be ignored, not index past
        // the end (INSERT VALUES, comparisons, and LIMIT forms).
        assert_eq!(
            infer("INSERT INTO items VALUES ($1, ?999)", 1),
            [Some(Type::INT8)]
        );
        assert_eq!(
            infer("INSERT INTO items VALUES (?999, $1)", 1),
            [Some(Type::TEXT)]
        );
        assert_eq!(
            infer("SELECT * FROM items WHERE id = ?999", 0),
            Vec::new()
        );
        assert_eq!(
            infer("SELECT * FROM items WHERE id = $1 AND score > ?999", 1),
            [Some(Type::INT8)]
        );
        assert_eq!(infer("SELECT * FROM items LIMIT ?999", 0), Vec::new());
    }
}

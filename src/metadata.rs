//! Conflict versions live in Spool. These types are transient record payloads,
//! not an independently maintained in-memory metadata database.
use crate::{Error, Result, model::*};
use rusqlite::{
    fallible_streaming_iterator::FallibleStreamingIterator, hooks::Action, session::ChangesetIter,
};
use serde::{Deserialize, Serialize};
use spool::Mutation;
use crate::logical_store::LogicalStore as Store;
#[derive(Serialize, Deserialize)]
pub(crate) struct RowMeta {
    pub key: Vec<u8>,
    pub generation: Version,
    pub cells: Vec<Cell>,
}
pub(crate) fn row_key(table: &str, key: &[u8]) -> Vec<u8> {
    format!("04/meta/row/{table}/{}", blake3::hash(key).to_hex()).into_bytes()
}
fn tomb_key(t: &Tombstone) -> Result<Vec<u8>> {
    Ok(format!(
        "05/meta/tomb/{}/{}",
        t.table,
        blake3::hash(&encode(&(t.key.clone(), t.generation))?).to_hex()
    )
    .into_bytes())
}
pub(crate) fn read(store: &Store, table: &str, key: &[u8]) -> Result<Option<RowMeta>> {
    let value = store
        .get(&row_key(table, key))?
        .map(|b| decode::<RowMeta>(&b))
        .transpose()?;
    if value.as_ref().is_some_and(|v| v.key != key) {
        return Err(Error::Corrupt("metadata key collision".into()));
    }
    Ok(value)
}
pub(crate) fn put(table: &str, meta: &RowMeta) -> Result<Mutation> {
    Ok(Mutation::put(row_key(table, &meta.key), encode(meta)?))
}
fn tomb(table: &str, key: Vec<u8>, generation: Version, version: Version) -> Result<Mutation> {
    let t = Tombstone {
        table: table.into(),
        key,
        generation,
        version,
    };
    Ok(Mutation::put(tomb_key(&t)?, encode(&t)?))
}
pub(crate) fn tables_in_changeset(bytes: &[u8]) -> Vec<String> {
    if bytes.is_empty() {
        return Vec::new();
    }
    let mut cursor = std::io::Cursor::new(bytes);
    let reader: &mut dyn std::io::Read = &mut cursor;
    let mut iter = match ChangesetIter::start_strm(&reader) {
        Ok(it) => it,
        Err(_) => return Vec::new(),
    };
    let mut set = std::collections::HashSet::new();
    while let Ok(Some(item)) = iter.next() {
        if let Ok(op) = item.op() {
            set.insert(op.table_name().to_string());
        }
    }
    set.into_iter().collect()
}
/// Inspect only primary keys in a pending session capture. Used by background
/// publication to prove that uncaptured local SQL cannot invalidate a point merge.
#[cfg(feature="replication")]
pub(crate) fn changeset_overlaps(schema:&Schema,bytes:&[u8],keys:&std::collections::BTreeSet<(String,Vec<u8>)>)->Result<bool> {
    if bytes.is_empty(){return Ok(false);}
    let mut cursor=std::io::Cursor::new(bytes);let reader:&mut dyn std::io::Read=&mut cursor;
    let mut iter=ChangesetIter::start_strm(&reader)?;
    while let Some(item)=iter.next()? {
        let op=item.op()?;let table=schema.tables.iter().find(|table|table.name==op.table_name()).ok_or(Error::SchemaMismatch)?;
        if op.number_of_columns() as usize!=table.columns.len(){return Err(Error::SchemaMismatch);}
        let key=crate::schema::pk_columns(table).iter().map(|column|{
            let index=table.columns.iter().position(|candidate|candidate.id==column.id).unwrap();
            Value::read(if op.code()==Action::SQLITE_INSERT{item.new_value(index)?}else{item.old_value(index)?})
        }).collect::<rusqlite::Result<Vec<_>>>()?;
        if keys.contains(&(table.id.clone(),encode(&key)?)){return Ok(true);}
    }
    Ok(false)
}
pub(crate) fn delta(
    store: &Store,
    schema: &Schema,
    bytes: &[u8],
    version: Version,
) -> Result<Vec<Mutation>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let mut cursor = std::io::Cursor::new(bytes);
    let reader: &mut dyn std::io::Read = &mut cursor;
    let mut iter = ChangesetIter::start_strm(&reader)?;
    let mut changes = Vec::new();
    while let Some(item) = iter.next()? {
        let op = item.op()?;
        let table = schema
            .tables
            .iter()
            .find(|t| t.name == op.table_name())
            .ok_or_else(|| Error::Corrupt("unknown changeset table".into()))?;
        if op.number_of_columns() as usize != table.columns.len()
            || item
                .pk()?
                .iter()
                .zip(&table.columns)
                .any(|(pk, c)| (*pk != 0) != (c.primary_key > 0))
        {
            return Err(Error::SchemaMismatch);
        }
        let key = crate::schema::pk_columns(table)
            .iter()
            .map(|col| {
                let i = table.columns.iter().position(|c| c.id == col.id).unwrap();
                Value::read(if op.code() == Action::SQLITE_INSERT {
                    item.new_value(i)?
                } else {
                    item.old_value(i)?
                })
            })
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if key.iter().any(|v| matches!(v, Value::Null)) {
            return Err(Error::Corrupt("null changeset primary key".into()));
        }
        let key = encode(&key)?;
        let old = read(store, &table.id, &key)?;
        match op.code() {
            Action::SQLITE_INSERT => {
                if let Some(old) = old {
                    changes.push(tomb(&table.id, key.clone(), old.generation, version)?);
                }
                changes.push(put(
                    &table.id,
                    &RowMeta {
                        key,
                        generation: version,
                        cells: table
                            .columns
                            .iter()
                            .enumerate()
                            .map(|(i, c)| Ok(Cell { column: c.id.clone(), value: Value::read(item.new_value(i)?)?, version }))
                            .collect::<Result<Vec<_>>>()?,
                    },
                )?);
            }
            Action::SQLITE_UPDATE => {
                let mut meta =
                    old.ok_or_else(|| Error::Corrupt("update has no row metadata".into()))?;
                for (i, c) in table.columns.iter().enumerate() {
                    match item.new_value(i) {
                        Ok(value) => {
                            let cell = meta
                                .cells
                                .iter_mut()
                                .find(|cell| cell.column == c.id)
                                .ok_or(Error::SchemaMismatch)?;
                            cell.version = version;
                            cell.value = Value::read(value)?;
                        }
                        Err(rusqlite::Error::InvalidColumnIndex(_)) => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                changes.push(put(&table.id, &meta)?);
            }
            Action::SQLITE_DELETE => {
                let meta =
                    old.ok_or_else(|| Error::Corrupt("delete has no row metadata".into()))?;
                changes.push(tomb(&table.id, key.clone(), meta.generation, version)?);
                changes.push(Mutation::delete(row_key(&table.id, &key)));
            }
            _ => return Err(Error::Corrupt("unsupported changeset action".into())),
        }
    }
    Ok(changes)
}
pub(crate) fn migration(
    store: &Store,
    before: &[RowState],
    after: &[RowState],
    version: Version,
) -> Result<Vec<Mutation>> {
    if before.is_empty() && after.is_empty() {
        return Ok(Vec::new());
    }
    let mut changes = Vec::new();
    let mut before_map = std::collections::HashMap::with_capacity(before.len());
    for row in before {
        let key = encode(&row.key)?;
        before_map.insert((row.table.as_str(), key), row);
    }
    let mut after_map = std::collections::HashMap::with_capacity(after.len());
    for row in after {
        let key = encode(&row.key)?;
        after_map.insert((row.table.as_str(), key), row);
    }
    for ((table, key), _row) in &before_map {
        if !after_map.contains_key(&(table, key.clone())) {
            let old = read(store, table, key)?
                .ok_or_else(|| Error::Corrupt("migration row metadata missing".into()))?;
            changes.push(tomb(table, key.clone(), old.generation, version)?);
            changes.push(Mutation::delete(row_key(table, key)));
        }
    }
    for ((table, key), row) in &after_map {
        let old = read(store, table, key)?;
        let prior = before_map.get(&(table, key.clone())).copied();
        let cells = row
            .cells
            .iter()
            .map(|cell| {
                let unchanged = prior
                    .and_then(|r| r.cells.iter().find(|c| c.column == cell.column))
                    .is_some_and(|c| c.value == cell.value);
                let v = if unchanged {
                    old.as_ref()
                        .and_then(|m| m.cells.iter().find(|c| c.column == cell.column))
                        .map(|c| c.version)
                        .unwrap_or(version)
                } else {
                    version
                };
                Cell { column: cell.column.clone(), value: cell.value.clone(), version: v }
            })
            .collect();
        changes.push(put(
            table,
            &RowMeta {
                key: key.clone(),
                generation: old.map(|o| o.generation).unwrap_or(version),
                cells,
            },
        )?);
    }
    Ok(changes)
}
pub(crate) fn replica(store: &Store, state: &ReplicaState) -> Result<Vec<Mutation>> {
    let mut mutations = store
        .keys_with_prefix(b"04/meta/row/")
        .into_iter()
        .map(Mutation::delete)
        .collect::<Vec<_>>();
    // State is the validated join, including all previously known tombstones.
    for row in &state.rows {
        mutations.push(put(
            &row.table,
            &RowMeta {
                key: encode(&row.key)?,
                generation: row.generation,
                cells: row.cells.clone(),
            },
        )?);
    }
    for t in &state.tombstones {
        mutations.push(Mutation::put(tomb_key(t)?, encode(t)?));
    }
    Ok(mutations)
}
pub(crate) fn export(
    store: &Store,
    schema: Schema,
    database_id: [u8; 16],
    mut rows: Vec<RowState>,
    clock: Version,
) -> Result<ReplicaState> {
    for row in &mut rows {
        let meta = read(store, &row.table, &encode(&row.key)?)?
            .ok_or_else(|| Error::Corrupt("row metadata missing".into()))?;
        row.generation = meta.generation;
        for cell in &mut row.cells {
            cell.version = meta
                .cells
                .iter()
                .find(|c| c.column == cell.column)
                .map(|c| c.version)
                .ok_or(Error::SchemaMismatch)?;
        }
    }
    let mut tombstones = Vec::new();
    for key in store.keys_with_prefix(b"05/meta/tomb/") {
        let b = store
            .get(&key)?
            .ok_or_else(|| Error::Corrupt("tombstone missing".into()))?;
        tombstones.push(decode::<Tombstone>(&b)?);
    }
    tombstones
        .sort_by(|a, b| (&a.table, &a.key, a.generation).cmp(&(&b.table, &b.key, b.generation)));
    Ok(ReplicaState {
        database_id,
        schema,
        rows,
        tombstones,
        clock,
    })
}

/// Read a transient logical state from the authoritative encrypted Spool.
/// The checkpoint worker serializes this read with all metadata commits.
pub(crate) fn state(store: &Store, schema: Schema, database_id: [u8;16], clock: Version) -> Result<ReplicaState> {
    let mut rows = Vec::new();
    for table in &schema.tables {
        for key in store.keys_with_prefix(format!("04/meta/row/{}/", table.id).as_bytes()) {
            let bytes = store.get(&key)?.ok_or_else(|| Error::Corrupt("missing row record".into()))?;
            let meta: RowMeta = decode(&bytes)?;
            rows.push(RowState { table: table.id.clone(), key: decode(&meta.key)?, generation: meta.generation, cells: meta.cells });
        }
    }
    export(store, schema, database_id, rows, clock)
}

/// Only the rows/tombstones changed by one committed local capture.
pub(crate) fn changed_state(records: &[Mutation], schema: Schema, database_id: [u8;16], clock: Version) -> Result<ReplicaState> {
    let mut rows = Vec::new(); let mut tombstones = Vec::new();
    for record in records {
        if record.deleted {continue;}
        if record.key.starts_with(b"04/meta/row/") {
            let meta: RowMeta = decode(&record.value)?;
            let key = std::str::from_utf8(&record.key).map_err(|_|Error::Corrupt("row key".into()))?;
            let table = key.split('/').nth(3).ok_or(Error::SchemaMismatch)?.to_string();
            rows.push(RowState {table, key:decode(&meta.key)?,generation:meta.generation,cells:meta.cells});
        } else if record.key.starts_with(b"05/meta/tomb/") {tombstones.push(decode(&record.value)?);}
    }
    Ok(ReplicaState {database_id,schema,rows,tombstones,clock})
}

pub(crate) fn patch(store:&Store,rows:&[RowState],deleted:&[RowState],tombstones:&[Tombstone])->Result<Vec<Mutation>> {
    let mut changes=Vec::new();
    for row in deleted {changes.push(Mutation::delete(row_key(&row.table,&encode(&row.key)?)));}
    for row in rows {changes.push(put(&row.table,&RowMeta{key:encode(&row.key)?,generation:row.generation,cells:row.cells.clone()})?);}
    for t in tombstones {let key=tomb_key(t)?;let mut t=t.clone();if let Some(bytes)=store.get(&key)? {let old:Tombstone=decode(&bytes)?;t.version=t.version.max(old.version);}changes.push(Mutation::put(key,encode(&t)?));}
    Ok(changes)
}

/// Maximum deleted generation for one primary key. Historical tombstones remain intact.
fn deleted_key(table: &str, key: &[u8]) -> Vec<u8> {
    format!("05/meta/deleted/{table}/{}", blake3::hash(key).to_hex()).into_bytes()
}
pub(crate) fn summarize_tombs(store: &Store, records: &mut Vec<Mutation>) -> Result<()> {
    let mut summaries = std::collections::BTreeMap::<Vec<u8>, Tombstone>::new();
    for record in records.iter().filter(|r| !r.deleted && r.key.starts_with(b"05/meta/tomb/")) {
        let tomb: Tombstone = decode(&record.value)?;
        let key = deleted_key(&tomb.table, &tomb.key);
        if !summaries.contains_key(&key) {
            if let Some(bytes) = store.get(&key)? { summaries.insert(key.clone(), decode(&bytes)?); }
        }
        let current = summaries.entry(key).or_insert_with(|| tomb.clone());
        if current.key != tomb.key || current.table != tomb.table { return Err(Error::Corrupt("deleted key collision".into())); }
        if (tomb.generation,tomb.version) > (current.generation,current.version) { *current = tomb; }
    }
    for (key,tomb) in summaries { records.push(Mutation::put(key,encode(&tomb)?)); }
    Ok(())
}
/// Read only rows and deletion floors that can be affected by this batch.
pub(crate) fn affected_state(store:&Store, schema:Schema, database_id:[u8;16], clock:Version,
    keys:&std::collections::BTreeSet<(String,Vec<u8>)>) -> Result<ReplicaState> {
    let mut rows = Vec::new(); let mut tombstones = Vec::new();
    for (table,key) in keys {
        if let Some(meta) = read(store,table,key)? {
            rows.push(RowState{table:table.clone(), key:decode(key)?, generation:meta.generation,cells:meta.cells});
        }
        if let Some(bytes) = store.get(&deleted_key(table,key))? {
            let tomb:Tombstone=decode(&bytes)?;
            if &tomb.table!=table || &tomb.key!=key {return Err(Error::Corrupt("deleted key collision".into()));}
            tombstones.push(tomb);
        }
    }
    Ok(ReplicaState{database_id,schema,clock,rows,tombstones})
}

/// Switch physical key domains without mixing local application-key tombstones
/// with UUID tombstones. Archived records are authoritative Spool records, never
/// a second SQLite database or an in-memory metadata mirror.
pub(crate) fn transition(store:&Store, table:&str, enabled:bool, before:&[RowState], after:&[RowState], version:Version)->Result<Vec<Mutation>> {
    let mut changes=Vec::new();
    let domain=if enabled {"local"}else{"replicated"};
    let archive=format!("12/key-transition/{table}/{domain}/");
    for prefix in [format!("04/meta/row/{table}/"), format!("05/meta/tomb/{table}/"), format!("05/meta/deleted/{table}/"), format!("10/policy/state/{table}/"), format!("09/bridge/owner/{table}/")] {
        for key in store.keys_with_prefix(prefix.as_bytes()) {
            if !key.starts_with(b"04/meta/row/") && !key.starts_with(b"05/meta/deleted/") {
                let value=store.get(&key)?.ok_or_else(||Error::Corrupt("transition record missing".into()))?;
                // A reversible path encoding preserves every historical tombstone.
                let name=key.iter().map(|b|format!("{b:02x}")).collect::<String>();
                changes.push(Mutation::put(format!("{archive}{name}").into_bytes(),value));
            }
            changes.push(Mutation::delete(key));
        }
    }
    if !enabled {
        // Fence all published incarnations. These fences stay out of outgoing
        // batches until re-enabling publishes a new snapshot of the table.
        for row in before {
            let key=encode(&row.key)?;
            let old=read(store,table,&key)?.ok_or_else(||Error::Corrupt("transition row missing".into()))?;
            let record=tomb(table,key,old.generation,version)?;
            let name=record.key.iter().map(|b|format!("{b:02x}")).collect::<String>();
            changes.push(Mutation::put(format!("{archive}{name}").into_bytes(),record.value));
        }
    } else {
        for key in store.keys_with_prefix(format!("12/key-transition/{table}/replicated/").as_bytes()) {
            let value=store.get(&key)?.ok_or_else(||Error::Corrupt("transition archive missing".into()))?;
            // Only deletion records are restored. Causal policy state starts a
            // fresh incarnation from the current application values.
            let name=std::str::from_utf8(&key).map_err(|_|Error::Corrupt("archive key".into()))?.rsplit('/').next().unwrap();
            if name.starts_with("30352f6d6574612f746f6d622f") {
                let mut deletion:Tombstone=decode(&value)?;
                deletion.version=version;
                changes.push(Mutation::put(tomb_key(&deletion)?,encode(&deletion)?));
            }
        }
    }
    for row in after {
        changes.push(put(table,&RowMeta{key:encode(&row.key)?,generation:version,cells:row.cells.iter().map(|cell|Cell{column:cell.column.clone(),value:cell.value.clone(),version}).collect()})?);
    }
    Ok(changes)
}

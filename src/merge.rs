use crate::{Error, Result, model::*};
use std::collections::BTreeMap;
type RowKey = (String, Vec<u8>);
fn merge_row(
    schema: &Schema,
    rows: &mut BTreeMap<RowKey, RowState>,
    row: &RowState,
    floors: &BTreeMap<RowKey, Version>,
) -> Result<RowKey> {
    #[cfg(not(feature = "replication"))]
    let _ = floors;
    let table = schema
        .tables
        .iter()
        .find(|t| t.id == row.table)
        .ok_or_else(|| Error::Corrupt("remote table".into()))?;
    if row.key.len() != crate::schema::pk_columns(table).len()
        || row.key.iter().any(|k| matches!(k, Value::Null))
        || row.cells.len() != table.columns.len()
    {
        return Err(Error::Corrupt("remote row shape".into()));
    }
    for col in &table.columns {
        if row.cells.iter().filter(|c| c.column == col.id).count() != 1 {
            return Err(Error::Corrupt("remote columns".into()));
        }
    }
    for (col, key) in crate::schema::pk_columns(table).iter().zip(&row.key) {
        if &row.cells.iter().find(|c| c.column == col.id).unwrap().value != key {
            return Err(Error::Corrupt("remote key mismatch".into()));
        }
    }
    crate::primary_key::validate_row(table, row)?;
    let key = (row.table.clone(), encode(&row.key)?);
    if let Some(old) = rows.get_mut(&key) {
        if row.generation > old.generation {
            #[cfg(feature = "replication")]
            {
                // A delete at or past the old incarnation fences its extrema
                // (delete-then-reinsert starts fresh); otherwise the unfenced
                // Min/Max cells join forward as values, not LWW.
                let fenced = floors.get(&key).is_some_and(|floor| *floor >= old.generation);
                if fenced {
                    *old = row.clone();
                } else {
                    let mut merged = row.clone();
                    join_extrema(table, &mut merged, old)?;
                    *old = merged;
                }
            }
            #[cfg(not(feature = "replication"))]
            {
                *old = row.clone();
            }
        } else if row.generation == old.generation {
            for cell in &row.cells {
                let target = old
                    .cells
                    .iter_mut()
                    .find(|c| c.column == cell.column)
                    .unwrap();
                let policy = table
                    .columns
                    .iter()
                    .find(|c| c.id == cell.column)
                    .ok_or(Error::SchemaMismatch)?
                    .policy;
                #[cfg(feature = "replication")]
                if matches!(policy, MergePolicy::Min | MergePolicy::Max) {
                    let order = crate::policy::compare(&cell.value, &target.value)?;
                    if policy == MergePolicy::Min && order.is_lt()
                        || policy == MergePolicy::Max && order.is_gt()
                    {
                        *target = cell.clone();
                    }
                    continue;
                }
                if cell.version > target.version {
                    *target = cell.clone();
                } else if cell.version == target.version
                    && cell.value != target.value
                    && !matches!(policy, MergePolicy::Counter | MergePolicy::Set)
                {
                    return Err(Error::Corrupt(
                        "different values for identical version".into(),
                    ));
                }
            }
        }
        #[cfg(feature = "replication")]
        if row.generation < old.generation {
            // Older incarnation: ordinary cells stay ignored, but unfenced
            // Min/Max extrema still join forward (value semilattice).
            let fenced = floors.get(&key).is_some_and(|floor| *floor >= row.generation);
            if !fenced {
                join_extrema(table, old, row)?;
            }
        }
    } else {
        rows.insert(key.clone(), row.clone());
    }
    Ok(key)
}

/// Value-join Min/Max cells from `source` into `target` (same key).
#[cfg(feature = "replication")]
fn join_extrema(table: &Table, target: &mut RowState, source: &RowState) -> Result<()> {
    for cell in target.cells.iter_mut() {
        let policy = table
            .columns
            .iter()
            .find(|c| c.id == cell.column)
            .ok_or(Error::SchemaMismatch)?
            .policy;
        if !matches!(policy, MergePolicy::Min | MergePolicy::Max) {
            continue;
        }
        let Some(other) = source.cells.iter().find(|c| c.column == cell.column) else {
            continue;
        };
        let order = crate::policy::compare(&other.value, &cell.value)?;
        if policy == MergePolicy::Min && order.is_lt()
            || policy == MergePolicy::Max && order.is_gt()
        {
            *cell = other.clone();
        }
    }
    Ok(())
}

/// Pure state join. No SQLite handles or local clock allocation.
pub(crate) fn join(a: &ReplicaState, b: &ReplicaState) -> Result<ReplicaState> {
    if a.database_id != b.database_id || a.schema != b.schema {
        return Err(Error::SchemaMismatch);
    }
    let mut tombs: BTreeMap<(String, Vec<u8>, Version), Tombstone> = BTreeMap::new();
    for t in a.tombstones.iter().chain(&b.tombstones) {
        let key = (t.table.clone(), t.key.clone(), t.generation);
        let e = tombs.entry(key).or_insert_with(|| t.clone());
        e.version = e.version.max(t.version);
    }
    let mut max_deleted: BTreeMap<(String, Vec<u8>), Version> = BTreeMap::new();
    for (table, key, generation) in tombs.keys() {
        max_deleted
            .entry((table.clone(), key.clone()))
            .and_modify(|g| *g = (*g).max(*generation))
            .or_insert(*generation);
    }
    let mut rows: BTreeMap<(String, Vec<u8>), RowState> = BTreeMap::new();
    for row in a.rows.iter().chain(&b.rows) {
        merge_row(&a.schema, &mut rows, row, &max_deleted)?;
    }
    rows.retain(|key, row| {
        max_deleted
            .get(key)
            .is_none_or(|generation| *generation < row.generation)
    });
    let mut clock = a.clock.max(b.clock);
    for row in rows.values() {
        clock = clock.max(row.generation);
        for cell in &row.cells {
            clock = clock.max(cell.version);
        }
    }
    for t in tombs.values() {
        clock = clock.max(t.version).max(t.generation);
    }
    Ok(ReplicaState {
        database_id: a.database_id,
        schema: a.schema.clone(),
        rows: rows.into_values().collect(),
        tombstones: tombs.into_values().collect(),
        clock,
    })
}
/// Preserve sequential join semantics while keeping one transient keyed batch.
/// Unchanged rows/tombstones are visited once, rather than once per transaction.
#[cfg(feature = "replication")]
pub(crate) fn join_batch(
    baseline: &ReplicaState,
    incoming: &[ReplicaState],
) -> Result<ReplicaState> {
    if incoming.is_empty() {
        return Ok(baseline.clone());
    }
    let mut tombs = BTreeMap::<(String, Vec<u8>, Version), Tombstone>::new();
    let mut floors = BTreeMap::<RowKey, Version>::new();
    for tomb in &baseline.tombstones {
        let key = (tomb.table.clone(), tomb.key.clone());
        floors
            .entry(key.clone())
            .and_modify(|floor| *floor = (*floor).max(tomb.generation))
            .or_insert(tomb.generation);
        let old = tombs
            .entry((key.0, key.1, tomb.generation))
            .or_insert_with(|| tomb.clone());
        old.version = old.version.max(tomb.version);
    }
    let mut rows = BTreeMap::new();
    for row in &baseline.rows {
        merge_row(&baseline.schema, &mut rows, row, &floors)?;
    }
    let mut clock = baseline.clock;
    for (index, state) in incoming.iter().enumerate() {
        if state.database_id != baseline.database_id || state.schema != baseline.schema {
            return Err(Error::SchemaMismatch);
        }
        clock = clock.max(state.clock);
        let mut touched = std::collections::BTreeSet::new();
        // Tombstones land before rows so each row merges against exactly the
        // floors its sequential join would see (accumulated plus current).
        for tomb in &state.tombstones {
            let key = (tomb.table.clone(), tomb.key.clone());
            touched.insert(key.clone());
            floors
                .entry(key.clone())
                .and_modify(|floor| *floor = (*floor).max(tomb.generation))
                .or_insert(tomb.generation);
            let old = tombs
                .entry((key.0, key.1, tomb.generation))
                .or_insert_with(|| tomb.clone());
            old.version = old.version.max(tomb.version);
            clock = clock.max(tomb.generation).max(tomb.version);
        }
        for row in &state.rows {
            touched.insert(merge_row(&baseline.schema, &mut rows, row, &floors)?);
        }
        if index == 0 {
            // The first sequential join also validates/prunes the baseline.
            rows.retain(|key, row| floors.get(key).is_none_or(|floor| *floor < row.generation));
            for row in rows.values() {
                clock = row_clock(clock, row);
            }
            for tomb in tombs.values() {
                clock = clock.max(tomb.generation).max(tomb.version);
            }
        } else {
            for key in touched {
                if rows.get(&key).is_some_and(|row| {
                    floors
                        .get(&key)
                        .is_some_and(|floor| *floor >= row.generation)
                }) {
                    rows.remove(&key);
                }
                if let Some(row) = rows.get(&key) {
                    clock = row_clock(clock, row);
                }
            }
        }
    }
    Ok(ReplicaState {
        database_id: baseline.database_id,
        schema: baseline.schema.clone(),
        rows: rows.into_values().collect(),
        tombstones: tombs.into_values().collect(),
        clock,
    })
}
#[cfg(feature = "replication")]
fn row_clock(mut clock: Version, row: &RowState) -> Version {
    clock = clock.max(row.generation);
    for cell in &row.cells {
        clock = clock.max(cell.version);
    }
    clock
}

#[derive(Debug)]
pub(crate) struct Patch {
    pub rows: Vec<RowState>,
    pub deleted: Vec<RowState>,
    pub tombstones: Vec<Tombstone>,
    pub cursor: u64,
    pub clock: Version,
    pub state: Option<ReplicaState>,
}
pub(crate) fn prepare(a: &ReplicaState, b: &ReplicaState, cursor: u64) -> Result<Patch> {
    prepare_joined(a, join(a, b)?, cursor)
}
pub(crate) fn prepare_joined(a: &ReplicaState, joined: ReplicaState, cursor: u64) -> Result<Patch> {
    let old_rows: BTreeMap<_, _> = a
        .rows
        .iter()
        .map(|r| Ok(((r.table.clone(), encode(&r.key)?), r)))
        .collect::<Result<_>>()?;
    let new_rows: BTreeMap<_, _> = joined
        .rows
        .iter()
        .map(|r| Ok(((r.table.clone(), encode(&r.key)?), r)))
        .collect::<Result<_>>()?;
    let old_tombs: BTreeMap<_, _> = a
        .tombstones
        .iter()
        .map(|t| ((t.table.clone(), t.key.clone(), t.generation), t))
        .collect();
    let rows = new_rows
        .iter()
        .filter(|(key, row)| old_rows.get(*key).is_none_or(|old| *old != **row))
        .map(|(_, row)| (*row).clone())
        .collect();
    let deleted = old_rows
        .iter()
        .filter(|(key, _)| !new_rows.contains_key(*key))
        .map(|(_, row)| (*row).clone())
        .collect();
    let tombstones = joined
        .tombstones
        .iter()
        .filter(|t| {
            old_tombs
                .get(&(t.table.clone(), t.key.clone(), t.generation))
                .is_none_or(|old| *old != *t)
        })
        .cloned()
        .collect();
    Ok(Patch {
        rows,
        deleted,
        tombstones,
        cursor,
        clock: joined.clock,
        state: Some(joined),
    })
}

#[cfg(feature = "replication")]
pub(crate) fn prepare_delta(a: &ReplicaState, joined: ReplicaState, cursor: u64) -> Result<Patch> {
    let mut patch = prepare_joined(a, joined, cursor)?;
    patch.state = None;
    Ok(patch)
}

#[cfg(all(test, feature = "replication"))]
mod batch_tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    fn version(millis: u64) -> Version {
        Version {
            millis,
            logical: 0,
            node: [1; 16],
        }
    }
    fn fixture(count: usize, policy: MergePolicy) -> ReplicaState {
        let schema = Schema {
            tables: vec![Table {
                application_key: None,
                id: "items".into(),
                name: "items".into(),
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER,payload BLOB)".into(),
                columns: vec![
                    Column { vector: None, fts: false,
                        id: "id".into(),
                        name: "id".into(),
                        primary_key: 1,
                        declared_type: "INTEGER".into(),
                        not_null: true,
                        policy: MergePolicy::Lww,
                    },
                    Column { vector: None, fts: false,
                        id: "n".into(),
                        name: "n".into(),
                        primary_key: 0,
                        declared_type: "INTEGER".into(),
                        not_null: false,
                        policy,
                    },
                    Column { vector: None, fts: false,
                        id: "payload".into(),
                        name: "payload".into(),
                        primary_key: 0,
                        declared_type: "BLOB".into(),
                        not_null: false,
                        policy: MergePolicy::Lww,
                    },
                ],
            }],
            ..Schema::default()
        };
        let rows = (0..count)
            .map(|id| RowState {
                table: "items".into(),
                key: vec![Value::Integer(id as i64)],
                generation: version(1),
                cells: vec![
                    Cell {
                        column: "id".into(),
                        value: Value::Integer(id as i64),
                        version: version(1),
                    },
                    Cell {
                        column: "n".into(),
                        value: Value::Integer(0),
                        version: version(1),
                    },
                    Cell {
                        column: "payload".into(),
                        value: Value::Blob(vec![42; 1024]),
                        version: version(1),
                    },
                ],
            })
            .collect();
        ReplicaState {
            database_id: [7; 16],
            schema,
            rows,
            tombstones: vec![],
            clock: Version::default(),
        }
    }
    #[test]
    fn batch_join_matches_sequential_joins_through_deletes_reinserts_and_policies() -> Result<()> {
        for policy in [
            MergePolicy::Lww,
            MergePolicy::Min,
            MergePolicy::Max,
            MergePolicy::Counter,
            MergePolicy::Set,
        ] {
            let baseline = fixture(32, policy);
            let mut sequential = baseline.clone();
            let mut states = Vec::new();
            let mut rng = rand::rngs::StdRng::seed_from_u64(777);
            for step in 1..=80 {
                let mut state = baseline.clone();
                state.rows.clear();
                state.tombstones.clear();
                let id = rng.gen_range(0..32);
                let mut row = baseline.rows[id].clone();
                if rng.gen_bool(0.25) {
                    row.generation = version(step + 2);
                }
                row.cells[1].version = version(step + 100);
                row.cells[1].value = Value::Integer(step as i64);
                state.rows = vec![row.clone(), row.clone()]; // Duplicate delivery within an envelope.
                if rng.gen_bool(0.4) {
                    state.tombstones.push(Tombstone {
                        table: row.table,
                        key: encode(&row.key)?,
                        generation: row.generation,
                        version: version(step + 90),
                    });
                }
                // Deliberately omit precomputed clock maxima. Deleted rows must
                // not inflate the clock beyond sequential-join behavior.
                state.clock = Version::default();
                sequential = join(&sequential, &state)?;
                states.push(state);
                assert_eq!(
                    join_batch(&baseline, &states)?,
                    sequential,
                    "policy {policy:?}, prefix {step}"
                );
            }
        }
        Ok(())
    }
    #[test]
    fn batch_join_keeps_schema_shape_and_equal_version_conflict_rejection() -> Result<()> {
        let baseline = fixture(2, MergePolicy::Lww);
        let mut changed = baseline.clone();
        changed.rows.truncate(1);
        changed.rows[0].cells[1].value = Value::Integer(99);
        assert!(matches!(
            join_batch(&baseline, &[changed]),
            Err(Error::Corrupt(_))
        ));
        let mut changed = baseline.clone();
        changed.schema.tables[0].name = "other".into();
        assert!(matches!(
            join_batch(&baseline, &[changed]),
            Err(Error::SchemaMismatch)
        ));
        let mut changed = baseline.clone();
        changed.rows[0].cells.pop();
        assert!(matches!(
            join_batch(&baseline, &[changed]),
            Err(Error::Corrupt(_))
        ));
        assert_eq!(join_batch(&baseline, &[])?, baseline);
        Ok(())
    }
    #[test]
    #[ignore = "release affected-row batch microbenchmark"]
    fn affected_row_batch_join_cost() -> Result<()> {
        let baseline = fixture(512, MergePolicy::Lww);
        let states: Vec<_> = (0..64)
            .map(|batch| {
                let mut state = baseline.clone();
                state.rows = state.rows[batch * 8..batch * 8 + 8].to_vec();
                for row in &mut state.rows {
                    row.cells[1].value = Value::Integer(99);
                    row.cells[1].version = version(2);
                }
                state
            })
            .collect();
        let repeats = 20;
        let start = std::time::Instant::now();
        for _ in 0..repeats {
            let mut state = baseline.clone();
            for incoming in &states {
                state = join(&state, incoming)?;
            }
            std::hint::black_box(state);
        }
        let sequential = start.elapsed();
        let start = std::time::Instant::now();
        for _ in 0..repeats {
            std::hint::black_box(join_batch(&baseline, &states)?);
        }
        eprintln!(
            "{repeats} batches, 64 deltas/512 affected rows/1 KiB payloads: sequential={sequential:?}, indexed batch={:?}",
            start.elapsed()
        );
        Ok(())
    }
}

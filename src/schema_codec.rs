//! Schema encoding keeps the format-6 positional layout. Binary search metadata
//! is a versioned extension in the index-string vector, removed before any SQL
//! reaches SQLite. This also decodes schemas nested in historical log events,
//! snapshots, signed payloads and bridge bundles without rewriting those bytes.
use crate::model::{ApplicationKey, Column, MergePolicy, Schema, Table};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

const FTS: &str = "\0murmur/column-fts/v1/";
const VECTOR: &str = "\0murmur/column-vector/v1/";

#[derive(Serialize, Deserialize)]
struct StoredColumn {
    policy: MergePolicy,
    id: String,
    name: String,
    primary_key: usize,
    declared_type: String,
    not_null: bool,
}
#[derive(Serialize, Deserialize)]
struct StoredTable {
    application_key: Option<ApplicationKey>,
    id: String,
    name: String,
    sql: String,
    columns: Vec<StoredColumn>,
}
#[derive(Serialize, Deserialize)]
struct StoredSchema {
    tables: Vec<StoredTable>,
    indexes: Vec<String>,
    history: [u8; 32],
}
#[derive(Serialize, Deserialize)]
struct HumanSchema {
    tables: Vec<Table>,
    indexes: Vec<String>,
    history: [u8; 32],
}
impl Serialize for Schema {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            return HumanSchema {
                tables: self.tables.clone(),
                indexes: self.indexes.clone(),
                history: self.history,
            }
            .serialize(serializer);
        }
        let mut indexes = self.indexes.clone();
        for table in &self.tables {
            for column in &table.columns {
                if let Some(config) = &column.vector {
                    let target = serde_json::to_string(&(&table.id, &column.id, config))
                        .map_err(serde::ser::Error::custom)?;
                    indexes.push(format!("{VECTOR}{target}"));
                }
                if column.fts {
                    let target = serde_json::to_string(&(&table.id, &column.id))
                        .map_err(serde::ser::Error::custom)?;
                    indexes.push(format!("{FTS}{target}"));
                }
            }
        }
        StoredSchema {
            tables: self
                .tables
                .iter()
                .map(|t| StoredTable {
                    application_key: t.application_key.clone(),
                    id: t.id.clone(),
                    name: t.name.clone(),
                    sql: t.sql.clone(),
                    columns: t
                        .columns
                        .iter()
                        .map(|c| StoredColumn {
                            policy: c.policy,
                            id: c.id.clone(),
                            name: c.name.clone(),
                            primary_key: c.primary_key,
                            declared_type: c.declared_type.clone(),
                            not_null: c.not_null,
                        })
                        .collect(),
                })
                .collect(),
            indexes,
            history: self.history,
        }
        .serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for Schema {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let s = HumanSchema::deserialize(deserializer)?;
            return Ok(Schema {
                tables: s.tables,
                indexes: s.indexes,
                history: s.history,
                ..Schema::default()
            });
        }
        let s = StoredSchema::deserialize(deserializer)?;
        let mut schema = Schema {
            tables: s
                .tables
                .into_iter()
                .map(|t| Table {
                    application_key: t.application_key,
                    id: t.id,
                    name: t.name,
                    sql: t.sql,
                    columns: t
                        .columns
                        .into_iter()
                        .map(|c| Column {
                            vector: None,
                            fts: false,
                            policy: c.policy,
                            id: c.id,
                            name: c.name,
                            primary_key: c.primary_key,
                            declared_type: c.declared_type,
                            not_null: c.not_null,
                        })
                        .collect(),
                })
                .collect(),
            history: s.history,
            ..Schema::default()
        };
        for index in s.indexes {
            if let Some(target) = index.strip_prefix(FTS) {
                let (table, column): (String, String) =
                    serde_json::from_str(target).map_err(D::Error::custom)?;
                let col = schema
                    .tables
                    .iter_mut()
                    .find(|t| t.id == table)
                    .and_then(|t| t.columns.iter_mut().find(|c| c.id == column))
                    .ok_or_else(|| {
                        D::Error::custom("FTS declaration references a missing column")
                    })?;
                if col.fts {
                    return Err(D::Error::custom("duplicate FTS declaration"));
                }
                col.fts = true;
            } else if let Some(target) = index.strip_prefix(VECTOR) {
                let (table, column, config): (String, String, crate::VectorColumnConfig) =
                    serde_json::from_str(target).map_err(D::Error::custom)?;
                let col = schema
                    .tables
                    .iter_mut()
                    .find(|t| t.id == table)
                    .and_then(|t| t.columns.iter_mut().find(|c| c.id == column))
                    .ok_or_else(|| {
                        D::Error::custom("vector declaration references a missing column")
                    })?;
                if col.vector.is_some() {
                    return Err(D::Error::custom("duplicate vector declaration"));
                }
                col.vector = Some(config);
            } else if index.starts_with('\0') {
                return Err(D::Error::custom("unknown schema extension"));
            } else {
                schema.indexes.push(index);
            }
        }
        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_and_extended_schemas_roundtrip_in_nested_payloads() {
        let old = StoredSchema {
            tables: vec![StoredTable {
                application_key: None,
                id: "t".into(),
                name: "docs".into(),
                sql: "CREATE TABLE docs(id INTEGER PRIMARY KEY,body TEXT)".into(),
                columns: vec![StoredColumn {
                    policy: MergePolicy::Lww,
                    id: "c".into(),
                    name: "body".into(),
                    primary_key: 0,
                    declared_type: "TEXT".into(),
                    not_null: false,
                }],
            }],
            indexes: vec![],
            history: [3; 32],
        };
        let bytes = postcard::to_allocvec(&(12u32, &old, 99u64)).unwrap();
        let (_, mut schema, tail): (u32, Schema, u64) = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(tail, 99);
        assert!(!schema.tables[0].columns[0].fts);
        assert_eq!(
            postcard::to_allocvec(&(12u32, &schema, 99u64)).unwrap(),
            bytes
        );
        assert!(schema.tables[0].columns[0].vector.is_none());
        schema.tables[0].columns[0].vector = Some(crate::VectorColumnConfig { dimensions: 3, distance: crate::DistanceMetric::Cosine });
        schema.tables[0].columns[0].fts = true;
        let encoded = postcard::to_allocvec(&(12u32, &schema, 99u64)).unwrap();
        let (_, decoded, tail): (u32, Schema, u64) = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(decoded, schema);
        assert_eq!(tail, 99);
        assert!(decoded.indexes.is_empty());
        assert_eq!(
            serde_json::from_str::<Schema>(&serde_json::to_string(&schema).unwrap()).unwrap(),
            schema
        );
    }
}

//! High/Low provenance and ownership are encrypted Spool records.
use crate::{*,model::*,metadata,merge,storage::Opened,network::Adapter};
use murmur_replication::{self as net,Backend,Record,bridge::{Header,Role}};
use serde::{Serialize,Deserialize};
use std::collections::BTreeMap;
#[derive(Clone,Debug,Serialize,Deserialize)]
pub(crate) struct Owner {pub table:String,pub key:Vec<Value>,pub column:String,pub epoch:Version,pub release_epoch:Version,pub owned:bool,pub cell:Option<Cell>}
#[derive(Clone,Debug,Serialize,Deserialize)]
struct Shadow {pub database:[u8;16],pub row:Option<RowState>,pub tombstones:Vec<Tombstone>,pub table:String,pub key:Vec<Value>}
pub(crate) fn owner_key(table:&str,key:&[Value],column:&str)->Result<Vec<u8>>{Ok(format!("09/bridge/owner/{}/{}/{}",table,id(&encode(&key)?),column).into_bytes())}
fn shadow_key(shadow:&Shadow)->Result<Vec<u8>>{Ok(format!("09/bridge/shadow/{}/{}/{}",shadow.table,id(&encode(&shadow.key)?),id(&shadow.database)).into_bytes())}
pub(crate) fn current_owner(o:&Opened,table:&str,key:&[Value],column:&str)->Result<Option<Owner>>{o.store.get(&owner_key(table,key,column)?)?.map(|b|decode(&b)).transpose()}
pub(crate) fn promote(o:&Opened,records:&mut Vec<spool::Mutation>,version:Version)->Result<()> {
    if !o.replication_config.as_ref().and_then(|c|c.bridge.as_ref()).is_some_and(|b|b.role==Role::HighReceiver){return Ok(());}
    let rows=metadata::changed_state(records,o.schema.clone(),o.identity.database,version)?;
    for row in rows.rows {let table=o.schema.tables.iter().find(|t|t.id==row.table).ok_or(Error::SchemaMismatch)?;for cell in &row.cells {if cell.version!=version || table.columns.iter().any(|c|c.id==cell.column && c.primary_key>0){continue;}let previous=current_owner(o,&row.table,&row.key,&cell.column)?;let owner=Owner{table:row.table.clone(),key:row.key.clone(),column:cell.column.clone(),epoch:previous.as_ref().filter(|p|p.owned).map(|p|p.epoch).unwrap_or(version),release_epoch:previous.as_ref().map(|p|p.release_epoch).unwrap_or_default(),owned:true,cell:Some(cell.clone())};records.push(spool::Mutation::put(owner_key(&owner.table,&owner.key,&owner.column)?,encode(&owner)?));}}
    for tomb in rows.tombstones {let key:Vec<Value>=decode(&tomb.key)?;let owner=Owner{table:tomb.table.clone(),key,column:"__row__".into(),epoch:version,release_epoch:Version::default(),owned:true,cell:None};records.push(spool::Mutation::put(owner_key(&owner.table,&owner.key,&owner.column)?,encode(&owner)?));}Ok(())
}
fn join_shadow(a:&Shadow,b:&Shadow,schema:&Schema)->Result<Shadow>{if a.database!=b.database || a.table!=b.table || a.key!=b.key{return Err(Error::Corrupt("shadow identity".into()));}let state=|s:&Shadow|ReplicaState{database_id:s.database,schema:schema.clone(),rows:s.row.clone().into_iter().collect(),tombstones:s.tombstones.clone(),clock:Version::default()};let joined=merge::join(&state(a),&state(b))?;Ok(Shadow{database:a.database,row:joined.rows.into_iter().next(),tombstones:joined.tombstones,table:a.table.clone(),key:a.key.clone()})}
/// Merge control epochs before projecting cells. A delayed ownership event must
/// never undo a newer release, regardless of ordinary cell timestamp ordering.
pub(crate) fn join(o:&Opened,records:Vec<Record>,state:&mut ReplicaState)->Result<Vec<Record>> {
    let mut changes=BTreeMap::<Vec<u8>,Vec<u8>>::new();let mut other=Vec::new();
    for record in records {if !record.key.starts_with(b"09/bridge/owner/") && !record.key.starts_with(b"09/bridge/shadow/"){other.push(record);continue;}let value=record.value.ok_or_else(||Error::Corrupt("bridge control deletion".into()))?;let old=changes.get(&record.key).cloned().or(o.store.get(&record.key)?);
        let resolved=if record.key.starts_with(b"09/bridge/owner/"){let incoming:Owner=decode(&value)?;if owner_key(&incoming.table,&incoming.key,&incoming.column)?!=record.key{return Err(Error::Corrupt("ownership identity".into()));}if let Some(bytes)=old {let previous:Owner=decode(&bytes)?;if (previous.epoch,previous.cell.as_ref().map(|c|c.version))>(incoming.epoch,incoming.cell.as_ref().map(|c|c.version)) {bytes}else{value}}else{value}}
        else {let incoming:Shadow=decode(&value)?;if shadow_key(&incoming)?!=record.key{return Err(Error::Corrupt("shadow record identity".into()));}if let Some(bytes)=old{encode(&join_shadow(&decode(&bytes)?,&incoming,&state.schema)?)?}else{value}};
        changes.insert(record.key,resolved);
    }
    let mut affected=std::collections::BTreeSet::new();
    for row in &state.rows {affected.insert((row.table.clone(),encode(&row.key)?));}
    affected.extend(affected_records(&records_from_changes(&changes))?);
    let mut owners=BTreeMap::new(); let mut shadows=BTreeMap::new();
    for (table,key) in &affected {
        let values:Vec<Value>=decode(key)?;
        let columns=state.schema.tables.iter().find(|t| &t.id==table).ok_or(Error::SchemaMismatch)?;
        for column in std::iter::once("__row__").chain(columns.columns.iter().map(|c|c.id.as_str())) {
            let key=owner_key(table,&values,column)?;
            if let Some(bytes)=changes.get(&key).cloned().or(o.store.get(&key)?) { owners.insert(key,decode::<Owner>(&bytes)?); }
        }
        for key in o.store.keys_with_prefix(shadow_prefix(table,key).as_bytes()) {
            if let Some(bytes)=o.store.get(&key)? {shadows.insert(key,decode::<Shadow>(&bytes)?);}
        }
    }
    for(key,value)in &changes {if key.starts_with(b"09/bridge/shadow/"){shadows.insert(key.clone(),decode(value)?);}}
    // These maps exist only for this one bounded projection, not as a Rust replica.
    for shadow in shadows.values() {
        let row_owner=owners.get(&owner_key(&shadow.table,&shadow.key,"__row__")?);if row_owner.is_some_and(|o|o.owned){state.rows.retain(|r|r.table!=shadow.table || r.key!=shadow.key);continue;}
        if let Some(low)=&shadow.row {
            let row=if let Some(index)=state.rows.iter().position(|r|r.table==low.table && r.key==low.key){&mut state.rows[index]}else {
                let mut row=low.clone();if let Some(release)=row_owner.filter(|o|!o.owned){row.generation=row.generation.max(release.epoch);}state.rows.push(row);state.rows.last_mut().unwrap()
            };
            if !owners.values().any(|owner|owner.table==row.table && owner.key==row.key && owner.owned) {row.generation=low.generation.max(row_owner.map(|o|o.epoch).unwrap_or_default());}
            for cell in &low.cells {let owner=owners.get(&owner_key(&row.table,&row.key,&cell.column)?);if let Some(owner)=owner.filter(|o|o.owned){if let Some(value)=&owner.cell {if let Some(target)=row.cells.iter_mut().find(|c|c.column==cell.column){*target=value.clone();}}}else if let Some(target)=row.cells.iter_mut().find(|c|c.column==cell.column){*target=cell.clone();}}
        }else if !owners.values().any(|owner|owner.table==shadow.table && owner.key==shadow.key && owner.owned) {state.rows.retain(|r|r.table!=shadow.table || r.key!=shadow.key);}
    }
    other.extend(changes.into_iter().map(|(key,value)|Record::put(key,value)));Ok(other)
}
fn net_error(e:Error)->net::Error{net::Error::Storage(e.to_string())}
fn publish(adapter:&Adapter,patch:merge::Patch,records:Vec<Record>)->net::Result<bool>{let database=adapter.database()?;let result=database.call(move|c|c.publish_network(patch,records,true)).map_err(net_error)?;if !result{return Ok(false);}adapter.spool(|o|{o.store.flush()?;Ok(())})?;if let Some(error)=database.status().storage_error{return Err(net::Error::Storage(error));}Ok(true)}
pub(crate) fn import(adapter:&Adapter,header:Header,payload:Vec<u8>)->net::Result<bool>{
    let content:net::Content=crate::network::decode_content(&payload).map_err(net_error)?;
    let low=content.sqlite.as_ref().map(|b|decode::<ReplicaState>(b)).transpose().map_err(net_error)?;
    if let Some(low)=&low {adapter.observe(low.clock.millis,low.clock.logical)?;}
    let cursor=adapter.database()?.call(|c|{let s=c.status.lock().unwrap();Ok((s.committed==c.captured_cursor && s.durable==c.captured_cursor && s.storage_error.is_none()).then_some(s.committed))}).map_err(net_error)?;let Some(cursor)=cursor else{return Ok(false);};
    let prepared=adapter.spool(move|o|{
        let config=o.replication_config.as_ref().and_then(|c|c.bridge.as_ref()).filter(|b|b.role==Role::HighReceiver && b.trusted_domains.contains(&header.source_database)).ok_or_else(||Error::Invalid("untrusted bridge domain".into()))?;let _=config;
        let receipt=format!("09/bridge/receipt/{}/{}",id(&header.source_database),header.origin.as_ref().map(|p|format!("{}-{:020}",id(&p.node),p.sequence)).unwrap_or_else(||id(&header.bundle))).into_bytes();
        if let Some(previous)=o.store.get(&receipt)? {if previous!=header.digest{return Err(Error::Corrupt("conflicting bridge receipt".into()));}return Ok(None);}
        if o.cursor!=cursor{return Err(Error::Busy);}let mut state=ReplicaState{database_id:o.identity.database,schema:o.schema.clone(),clock:o.clock,rows:Vec::new(),tombstones:Vec::new()};let mut records=content.records;
        if let Some(mut low)=low {if low.database_id!=header.source_database{return Err(Error::SchemaMismatch);}
        let allowed:std::collections::BTreeSet<_> = low.schema.tables.iter().filter(|source| {
            state.schema.tables.iter().find(|target|target.name==source.name)
                .is_none_or(|target|crate::table_replication::enabled(&state.schema,&target.id))
        }).map(|table|table.id.clone()).collect();
        low.rows.retain(|row|allowed.contains(&row.table));
        low.tombstones.retain(|tomb|allowed.contains(&tomb.table));
        for row in &mut low.rows {
            let low_table=low.schema.tables.iter().find(|t|t.id==row.table).ok_or(Error::SchemaMismatch)?;let high_table=state.schema.tables.iter().find(|t|t.name==low_table.name).ok_or(Error::SchemaMismatch)?;
            if schema::pk_columns(low_table).iter().map(|c|(&c.name,&c.declared_type,c.primary_key)).collect::<Vec<_>>()!=schema::pk_columns(high_table).iter().map(|c|(&c.name,&c.declared_type,c.primary_key)).collect::<Vec<_>>(){return Err(Error::SchemaMismatch);}
            for cell in &mut row.cells {let low_column=low_table.columns.iter().find(|c|c.id==cell.column).ok_or(Error::SchemaMismatch)?;let high=high_table.columns.iter().find(|c|c.name==low_column.name && c.policy==low_column.policy && c.declared_type.eq_ignore_ascii_case(&low_column.declared_type)).ok_or(Error::SchemaMismatch)?;cell.column=high.id.clone();}
            row.table=high_table.id.clone();for column in &high_table.columns {if !row.cells.iter().any(|c|c.column==column.id){if column.not_null || column.primary_key>0{return Err(Error::SchemaMismatch);}row.cells.push(Cell{column:column.id.clone(),value:Value::Null,version:Version::default()});}}
            let shadow=Shadow{database:header.source_database,row:Some(row.clone()),tombstones:Vec::new(),table:row.table.clone(),key:row.key.clone()};records.push(Record::put(shadow_key(&shadow)?,encode(&shadow)?));
        }
        for tomb in &low.tombstones {let low_table=low.schema.tables.iter().find(|t|t.id==tomb.table).ok_or(Error::SchemaMismatch)?;let high=state.schema.tables.iter().find(|t|t.name==low_table.name).ok_or(Error::SchemaMismatch)?;let mut tomb=tomb.clone();tomb.table=high.id.clone();let key:Vec<Value>=decode(&tomb.key)?;let shadow=Shadow{database:header.source_database,row:None,tombstones:vec![tomb],table:high.id.clone(),key};records.push(Record::put(shadow_key(&shadow)?,encode(&shadow)?));}
        state.clock=state.clock.max(low.clock);}
        records=crate::table_replication::filter_records(records,&o.schema)?;
        let keys=affected_records(&records)?;let baseline=metadata::affected_state(&o.store,o.schema.clone(),o.identity.database,o.clock,&keys)?;
        state.rows=baseline.rows.clone();state.tombstones=baseline.tombstones.clone();
        records=join(o,records,&mut state)?;
        // Low policy components keep their domain/scope but use the High visible
        // generation. Ownership selects the appropriate causal branch.
        for record in &mut records {if record.key.starts_with(b"10/policy/state/") {let mut policy:crate::policy::State=decode(record.value.as_ref().ok_or(Error::SchemaMismatch)?)?;if let Some(row)=state.rows.iter().find(|r|r.table==policy.table && r.key==policy.key){policy.generation=row.generation;record.key=crate::policy::state_key(&policy)?;record.value=Some(encode(&policy)?);}}}
        records=crate::policy::join(o,records,&mut state)?;
        records.push(Record::put(receipt,header.digest.to_vec()));records.push(Record::put(format!("09/bridge/provenance/bundle/{}",id(&header.bundle)).into_bytes(),encode(&header)?));
        records.push(Record::put(format!("09/bridge/applied/{}/{}",net::hex(&header.source_database),header.stream).into_bytes(),encode(&header.sequence)?));
        crate::table_replication::preserve_local(&mut state, &baseline);
        Ok(Some((merge::prepare_delta(&baseline,state,cursor)?,records)))
    })?;let Some((patch,records))=prepared else{return Ok(true);};publish(adapter,patch,records)
}
pub(crate) fn release(adapter:&Adapter,table:String,key:Vec<u8>,column:Option<String>)->net::Result<bool>{let values:Vec<Value>=decode(&key).map_err(net_error)?;let name=table.clone();let (cursor,values)=adapter.database()?.call(move|c|{let table=c.schema.tables.iter().find(|t|t.name==name).ok_or(Error::SchemaMismatch)?;let values=crate::primary_key::resolve_key(&c.connection,table,values)?;let s=c.status.lock().unwrap();Ok(((s.committed==c.captured_cursor && s.durable==c.captured_cursor).then_some(s.committed),values))}).map_err(net_error)?;let Some(cursor)=cursor else{return Ok(false);};let outcome=adapter.spool(move|o|{let table=o.schema.tables.iter().find(|t|t.name==table).ok_or(Error::SchemaMismatch)?;let column=if let Some(name)=column {table.columns.iter().find(|c|c.name==name).ok_or(Error::SchemaMismatch)?.id.clone()}else{"__row__".into()};let version=o.clock.next(o.identity.node)?;let Some(mut owner)=current_owner(o,&table.id,&values,&column)?.filter(|o|o.owned) else{return Ok(None);};owner.owned=false;owner.epoch=version;owner.release_epoch=version;let record=Record::put(owner_key(&owner.table,&owner.key,&owner.column)?,encode(&owner)?);let keys=affected_records(std::slice::from_ref(&record))?;let baseline=metadata::affected_state(&o.store,o.schema.clone(),o.identity.database,o.clock,&keys)?;let mut state=baseline.clone();let records=join(o,vec![record],&mut state)?;let records=crate::policy::join(o,records,&mut state)?;state.clock=version;Ok(Some((merge::prepare_delta(&baseline,state,cursor)?,records)))})?;let Some((patch,records))=outcome else{return Err(net::Error::Invalid("field is not High-owned".into()));};publish(adapter,patch,records)}

pub(crate) fn policy_scope(o:&Opened,row:&RowState,column:&str,records:&[Record])->Result<([u8;16],Version,Version,Version)> {
    let key=owner_key(&row.table,&row.key,column)?;
    let owner=records.iter().rev().find(|r|r.key==key).and_then(|r|r.value.as_ref()).map(|b|decode::<Owner>(b)).transpose()?.or(current_owner(o,&row.table,&row.key,column)?);
    if let Some(owner)=owner.as_ref().filter(|o|o.owned){return Ok((o.identity.database,owner.release_epoch,row.generation,owner.epoch));}
    let mut latest:Option<(Version,[u8;16],Version)>=None;
    let prefix=shadow_prefix(&row.table,&encode(&row.key)?);let mut shadows=BTreeMap::new();for key in o.store.keys_with_prefix(prefix.as_bytes()){if let Some(bytes)=o.store.get(&key)?{shadows.insert(key,bytes);}}for r in records.iter().filter(|r|r.key.starts_with(prefix.as_bytes())){if let Some(bytes)=&r.value{shadows.insert(r.key.clone(),bytes.clone());}}
    for bytes in shadows.values(){let shadow:Shadow=decode(bytes)?;if shadow.table!=row.table || shadow.key!=row.key {continue;}if let Some(source)=shadow.row {if let Some(cell)=source.cells.iter().find(|c|c.column==column){let candidate=(cell.version,shadow.database,source.generation);if latest.is_none_or(|old|candidate>old){latest=Some(candidate);}}}}
    if let Some((version,database,generation))=latest {return Ok((database,Version::default(),generation,version));}
    Ok((o.identity.database,owner.map(|o|o.release_epoch).unwrap_or_default(),row.generation,row.generation))
}

fn shadow_prefix(table:&str,key:&[u8])->String {format!("09/bridge/shadow/{}/{}/",table,id(key))}
fn records_from_changes(changes:&BTreeMap<Vec<u8>,Vec<u8>>)->Vec<Record> {
    changes.iter().map(|(key,value)|Record::put(key.clone(),value.clone())).collect()
}
pub(crate) fn affected_records(records:&[Record])->Result<std::collections::BTreeSet<(String,Vec<u8>)>> {
    let mut keys=std::collections::BTreeSet::new();
    for record in records {if let Some(value)=&record.value {
        if record.key.starts_with(b"09/bridge/owner/") {let owner:Owner=decode(value)?;keys.insert((owner.table,encode(&owner.key)?));}
        else if record.key.starts_with(b"09/bridge/shadow/") {let shadow:Shadow=decode(value)?;keys.insert((shadow.table,encode(&shadow.key)?));}
        else if record.key.starts_with(b"10/policy/state/") {let policy:crate::policy::State=decode(value)?;keys.insert((policy.table,encode(&policy.key)?));}
    }}
    Ok(keys)
}

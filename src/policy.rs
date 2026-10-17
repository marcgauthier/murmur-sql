//! Explicit operation intents; durable causal state stays in Spool.
use crate::{*,model::*,storage::Opened,metadata};
use murmur_replication::{self as net,policies::{Causal,Counter,Set,Tag,Policy}};
use serde::{Serialize,Deserialize};
use std::collections::BTreeMap;
#[derive(Clone,Debug,Serialize,Deserialize)]
pub(crate) struct State {pub version:Version,pub domain:[u8;16],pub branch:Version,pub scope:Version,pub baseline:Value,pub basis:Version,pub table:String,pub column:String,pub key:Vec<Value>,pub generation:Version,pub causal:Causal}
pub(crate) fn state_key(state:&State)->Result<Vec<u8>> {Ok(format!("10/policy/state/{}/{}/{}/{}",state.table,state.column,id(&encode(&state.key)?),id(&encode(&(state.generation,state.domain,state.branch,state.scope))?)).into_bytes())}
pub(crate) fn configure(schema:&mut Schema,config:Option<&net::Config>)->Result<()> {
    if let Some(config)=config {for((table,column),policy)in &config.merge_policies {let t=schema.tables.iter_mut().find(|t|&t.name==table).ok_or_else(||Error::Invalid(format!("unknown policy table {table}")))?;let application_key_column=t.application_key.as_ref().is_some_and(|key|key.columns.contains(column));let c=t.columns.iter_mut().find(|c|&c.name==column).ok_or_else(||Error::Invalid(format!("unknown policy column {column}")))?;if c.primary_key>0 || application_key_column {return Err(Error::Invalid("primary key merge policy".into()));}c.policy=match policy{Policy::Lww=>MergePolicy::Lww,Policy::Counter=>MergePolicy::Counter,Policy::Set=>MergePolicy::Set,Policy::Min=>MergePolicy::Min,Policy::Max=>MergePolicy::Max};}}
    Ok(())
}
impl Sql<'_> {
    fn operation(&self,table:&str,key:Vec<Value>,column:&str,operation:PolicyOperation)->Result<()> {
        let intents=self.intents.ok_or_else(||Error::Invalid("policy operations require a transaction".into()))?;
        let t=self.schema.tables.iter().find(|t|t.name==table).ok_or(Error::SchemaMismatch)?;let c=t.columns.iter().find(|c|c.name==column).ok_or(Error::SchemaMismatch)?;
        let required=match &operation{PolicyOperation::Counter(_)=>MergePolicy::Counter,PolicyOperation::Add(_)|PolicyOperation::Remove(_)=>MergePolicy::Set,PolicyOperation::Min(_)=>MergePolicy::Min,PolicyOperation::Max(_)=>MergePolicy::Max};if c.policy!=required || key.len()!=crate::primary_key::application_columns(t).len() || key.iter().any(|k|matches!(k,Value::Null)) {return Err(Error::Invalid("operation does not match column policy or primary key".into()));}
        if intents.borrow().len()>=10000 {return Err(Error::Limit("policy operation count".into()));}
        let predicate=crate::primary_key::application_predicate(t);
        let before=self.connection.query_row(&format!("SELECT {} FROM {} WHERE {predicate}",quote(column),quote(table)),rusqlite::params_from_iter(&key),|r|Value::read(r.get_ref(0)?))?;
        let after=match &operation {
            PolicyOperation::Counter(delta)=>match &before {Value::Integer(n)=>Value::Integer(n.checked_add(*delta).ok_or_else(||Error::Limit("counter overflow".into()))?),_=>return Err(Error::Invalid("counter projection must be INTEGER".into()))},
            PolicyOperation::Add(value)|PolicyOperation::Remove(value)=>{
                let mut values=match &before {Value::Blob(b)=>decode::<Vec<Value>>(b)?,Value::Null=>Vec::new(),_=>return Err(Error::Invalid("set projection must be BLOB or NULL".into()))};
                match &operation {PolicyOperation::Add(_)=>{if !values.contains(value){values.push(value.clone());}},_=>values.retain(|v|v!=value)}
                values.sort_by_cached_key(|v|encode(v).unwrap_or_default());Value::Blob(encode(&values)?)
            },
            PolicyOperation::Min(value)=>if compare(value,&before)?==std::cmp::Ordering::Less {value.clone()}else{before.clone()},
            PolicyOperation::Max(value)=>if compare(value,&before)?==std::cmp::Ordering::Greater {value.clone()}else{before.clone()},
        };
        // Only this fixed statement bypasses policy assignment restrictions.
        self.access.store(4,std::sync::atomic::Ordering::Relaxed);
        let mut parameters=vec![after];parameters.extend(key.clone());
        let changed=self.connection.execute(&format!("UPDATE {} SET {}=? WHERE {predicate}",quote(table),quote(column)),rusqlite::params_from_iter(&parameters));
        self.access.store(2,std::sync::atomic::Ordering::Relaxed);if changed?!=1 {return Err(Error::Invalid("policy row missing".into()));}
        let key = if t.application_key.is_some() {
            let predicate=crate::primary_key::application_predicate(t);
            vec![self.connection.query_row(&format!("SELECT {} FROM {} WHERE {predicate}",quote(crate::primary_key::COLUMN),quote(table)),rusqlite::params_from_iter(&key),|r|Value::read(r.get_ref(0)?))?]
        } else {key};
        intents.borrow_mut().push(PolicyIntent{table:table.into(),column:column.into(),key,before,operation,id:rand::random()});Ok(())
    }
    pub fn counter_add(&self,table:&str,key:Vec<Value>,column:&str,delta:i64)->Result<()> {self.operation(table,key,column,PolicyOperation::Counter(delta))}
    pub fn set_add(&self,table:&str,key:Vec<Value>,column:&str,value:Value)->Result<()> {self.operation(table,key,column,PolicyOperation::Add(value))}
    pub fn set_remove(&self,table:&str,key:Vec<Value>,column:&str,value:Value)->Result<()> {self.operation(table,key,column,PolicyOperation::Remove(value))}
    pub fn min(&self,table:&str,key:Vec<Value>,column:&str,value:Value)->Result<()> {self.operation(table,key,column,PolicyOperation::Min(value))}
    pub fn max(&self,table:&str,key:Vec<Value>,column:&str,value:Value)->Result<()> {self.operation(table,key,column,PolicyOperation::Max(value))}
}
pub(crate) fn materialize(o:&Opened,changes:&mut Vec<spool::Mutation>,intents:&[PolicyIntent],version:Version)->Result<()> {
    let mut rows=BTreeMap::<(String,Vec<u8>),metadata::RowMeta>::new();
    for record in changes.iter().filter(|r|!r.deleted && r.key.starts_with(b"04/meta/row/")) {let meta:metadata::RowMeta=decode(&record.value)?;let table=std::str::from_utf8(&record.key).map_err(|_|Error::Corrupt("policy row key".into()))?.split('/').nth(3).ok_or(Error::SchemaMismatch)?.to_string();rows.insert((table,meta.key.clone()),meta);}
    for intent in intents {let table=o.schema.tables.iter().find(|t|t.name==intent.table).ok_or(Error::SchemaMismatch)?;let key=encode(&intent.key)?;if changes.iter().any(|r|r.deleted && r.key==metadata::row_key(&table.id,&key)){continue;}if !rows.contains_key(&(table.id.clone(),key.clone())) {if let Some(meta)=metadata::read(&o.store,&table.id,&key)? {rows.insert((table.id.clone(),key),meta);}}}
    for((table_id,key),mut meta)in rows {
        let table=o.schema.tables.iter().find(|t|t.id==table_id).ok_or(Error::SchemaMismatch)?;
        for column in table.columns.iter().filter(|c|matches!(c.policy,MergePolicy::Counter|MergePolicy::Set)) {
            let cell=meta.cells.iter_mut().find(|c|c.column==column.id).ok_or(Error::SchemaMismatch)?;
            let operations:Vec<_>=intents.iter().filter(|i|i.table==table.name && i.column==column.name && encode(&i.key).is_ok_and(|k|k==key)).collect();
            let initial=operations.first().map(|i|&i.before).unwrap_or(&cell.value);
            let control_records=changes.iter().filter(|r|!r.deleted).map(|r|net::Record::put(r.key.clone(),r.value.clone())).collect::<Vec<_>>();
            let visible=RowState{table:table_id.clone(),key:decode(&key)?,generation:meta.generation,cells:Vec::new()};
            let (domain,branch,scope,basis)=crate::high_low::policy_scope(o,&visible,&column.id,&control_records)?;
            let causal=match column.policy {
                MergePolicy::Counter=>{let counter=Counter::default();if !matches!(initial,Value::Integer(_)){return Err(Error::Invalid("counter requires an integer".into()));}Causal::Counter(counter)},
                MergePolicy::Set=>{let mut set=Set::default();let values=match initial {Value::Blob(b)=>decode::<Vec<Value>>(b)?,Value::Null=>Vec::new(),_=>return Err(Error::Invalid("set requires a BLOB".into()))};let digest=blake3::hash(&encode(&(table_id.clone(),key.clone(),meta.generation,domain,branch,scope,basis))?);let mut transaction=[0;16];transaction.copy_from_slice(&digest.as_bytes()[..16]);for(index,value)in values.iter().enumerate(){set.add(encode(value)?,Tag{database:domain,node:[0;16],transaction,operation:index as u32});}Causal::Set(set)},_=>unreachable!(),
            };
            let mut state=State{version,domain,branch,scope,baseline:initial.clone(),basis,table:table_id.clone(),column:column.id.clone(),key:decode(&key)?,generation:meta.generation,causal};let storage_key=state_key(&state)?;
            if let Some(bytes)=o.store.get(&storage_key)? {state=decode(&bytes)?;}
            for intent in operations {match(&mut state.causal,&intent.operation){(Causal::Counter(counter),PolicyOperation::Counter(delta))=>counter.add(o.identity.database,o.identity.node,*delta).map_err(|e|Error::Corrupt(e.to_string()))?,(Causal::Set(set),PolicyOperation::Add(value))=>set.add(encode(value)?,Tag{database:o.identity.database,node:o.identity.node,transaction:intent.id,operation:0}),(Causal::Set(set),PolicyOperation::Remove(value))=>set.remove(&encode(value)?),_=>return Err(Error::Corrupt("causal operation mismatch".into()))}}
            state.version=state.version.max(version);cell.value=projection(&state)?;cell.version=cell.version.max(state.version);
            changes.push(spool::Mutation::put(storage_key,encode(&state)?));
        }
        let record=metadata::put(&table_id,&meta)?;
        if let Some(existing)=changes.iter_mut().find(|r|r.key==record.key){*existing=record;}else{changes.push(record);}
    }Ok(())
}
fn projection(state:&State)->Result<Value>{match &state.causal {Causal::Counter(c)=>{let base=match state.baseline{Value::Integer(n)=>n,_=>return Err(Error::Corrupt("counter baseline".into()))};Ok(Value::Integer(i64::try_from(base as i128+c.positive.values().map(|n|*n as i128).sum::<i128>()-c.negative.values().map(|n|*n as i128).sum::<i128>()).map_err(|_|Error::Limit("counter projection overflow".into()))?))},Causal::Set(s)=>{let values=s.values().iter().map(|b|decode::<Value>(b)).collect::<Result<Vec<_>>>()?;Ok(Value::Blob(encode(&values)?))}}}
pub(crate) fn join(o:&Opened,records:Vec<net::Record>,state:&mut ReplicaState)->Result<Vec<net::Record>> {
    let mut joined=BTreeMap::<Vec<u8>,State>::new();let mut ordinary=Vec::new();
    for record in records {if !record.key.starts_with(b"10/policy/state/"){ordinary.push(record);continue;}let bytes=record.value.ok_or_else(||Error::Corrupt("policy record deletion".into()))?;let incoming:State=decode(&bytes)?;if state_key(&incoming)?!=record.key {return Err(Error::Corrupt("policy record identity".into()));}
        let target=if let Some(prior)=joined.get(&record.key){Some(prior.clone())}else{o.store.get(&record.key)?.map(|b|decode::<State>(&b)).transpose()?};let mut resolved=target.unwrap_or_else(||incoming.clone());join_state(&mut resolved,&incoming)?;joined.insert(record.key,resolved);
    }
    let order:Vec<Vec<u8>>=joined.keys().cloned().collect();
    for key in order {let Some(policy)=joined.remove(&key) else{unreachable!("policy join key")};if let Some(index)=state.rows.iter().position(|r|r.table==policy.table && r.key==policy.key && r.generation==policy.generation){
            let row=&state.rows[index];let (domain,branch,scope,_)=crate::high_low::policy_scope(o,row,&policy.column,&ordinary)?;
            if (policy.domain,policy.branch,policy.scope)!=(domain,branch,scope) {ordinary.push(net::Record::put(key,encode(&policy)?));continue;}
            let row=&mut state.rows[index];let column=state.schema.tables.iter().find(|t|t.id==row.table).and_then(|t|t.columns.iter().find(|c|c.id==policy.column)).ok_or(Error::SchemaMismatch)?;if !matches!((&policy.causal,column.policy),(Causal::Counter(_),MergePolicy::Counter)|(Causal::Set(_),MergePolicy::Set)){return Err(Error::SchemaMismatch);}let cell=row.cells.iter_mut().find(|c|c.column==policy.column).ok_or(Error::SchemaMismatch)?;cell.value=projection(&policy)?;}
        else if let Some(rindex)=state.rows.iter().position(|r|r.table==policy.table && r.key==policy.key && r.generation>policy.generation){
            // Concurrent creates share no generation: the row merge keeps one
            // incarnation, but the loser's committed causal ops must join the
            // winning row instead of orphaning. A delete at or past the loser
            // generation fences it (delete-then-reinsert stays isolated).
            let rowkey=encode(&policy.key)?;
            let fenced=state.tombstones.iter().any(|t|t.table==policy.table && t.key==rowkey && t.generation>=policy.generation);
            if !fenced{
                let row=state.rows[rindex].clone();
                let (domain,branch,scope,basis)=crate::high_low::policy_scope(o,&row,&policy.column,&ordinary)?;
                let probe=State{version:Version::default(),domain,branch,scope,baseline:Value::Null,basis,table:row.table.clone(),column:policy.column.clone(),key:row.key.clone(),generation:row.generation,causal:Causal::Counter(Counter::default())};
                let wkey=state_key(&probe)?;
                if let Some(winner)=joined.get_mut(&wkey){if rebase_orphan(winner,&policy)?{ordinary.push(net::Record::put(wkey,encode(winner)?));}}
                // Cache the rebased winner in the batch map (not just the
                // output records) so a second orphan in the same batch joins
                // the rebased state instead of the stale stored one; without
                // this only one orphan per batch survives (last push wins).
                else if let Some(bytes)=o.store.get(&wkey)?{let mut winner:State=decode(&bytes)?;if rebase_orphan(&mut winner,&policy)?{ordinary.push(net::Record::put(wkey.clone(),encode(&winner)?));joined.insert(wkey,winner);}}
            }
        }ordinary.push(net::Record::put(key,encode(&policy)?));}
    // A release can change the selected branch without receiving another
    // policy record. Project its already durable causal state as well.
    for index in 0..state.rows.len() {let row=state.rows[index].clone();let table=state.schema.tables.iter().find(|t|t.id==row.table).ok_or(Error::SchemaMismatch)?;
        for column in table.columns.iter().filter(|c|matches!(c.policy,MergePolicy::Counter|MergePolicy::Set)) {
            let(domain,branch,scope,basis)=crate::high_low::policy_scope(o,&row,&column.id,&ordinary)?;
            let probe=State{version:Version::default(),domain,branch,scope,baseline:Value::Null,basis,table:row.table.clone(),column:column.id.clone(),key:row.key.clone(),generation:row.generation,causal:Causal::Counter(Counter::default())};let key=state_key(&probe)?;
            let bytes=ordinary.iter().rev().find(|r|r.key==key).and_then(|r|r.value.clone()).or(o.store.get(&key)?);
            if let Some(bytes)=bytes {let mut causal:State=decode(&bytes)?;
                // Rebase already-durable losers (e.g. our own pre-merge
                // incarnation) so their committed ops project too.
                let rowkey=encode(&row.key)?;
                let prefix=format!("10/policy/state/{}/{}/{}/",row.table,column.id,id(&rowkey));
                let mut modified=false;
                for okey in o.store.keys_with_prefix(prefix.as_bytes()){
                    if okey==key{continue;}
                    let Some(ob)=ordinary.iter().rev().find(|r|r.key==okey).and_then(|r|r.value.clone()).or(o.store.get(&okey)?) else{continue;};
                    let orphan:State=decode(&ob)?;
                    if orphan.generation>=causal.generation{continue;}
                    let fenced=state.tombstones.iter().any(|t|t.table==row.table && t.key==rowkey && t.generation>=orphan.generation);
                    if fenced{continue;}
                    if rebase_orphan(&mut causal,&orphan)?{modified=true;}
                }
                let cell=state.rows[index].cells.iter_mut().find(|c|c.column==column.id).ok_or(Error::SchemaMismatch)?;cell.value=projection(&causal)?;cell.version=causal.version;
                if modified{ordinary.push(net::Record::put(key,encode(&causal)?));}}
        }
    }Ok(ordinary)
}
pub(crate) fn compare(a:&Value,b:&Value)->Result<std::cmp::Ordering>{use std::cmp::Ordering;let rank=|v:&Value|match v{Value::Null=>0,Value::Integer(_)|Value::Real(_)=>1,Value::Text(_)=>2,Value::Blob(_)=>3};let ra=rank(a);let rb=rank(b);if ra!=rb{return Ok(ra.cmp(&rb));}Ok(match(a,b){(Value::Null,Value::Null)=>Ordering::Equal,(Value::Integer(a),Value::Integer(b))=>a.cmp(b),(Value::Real(a),Value::Real(b))=>a.partial_cmp(b).ok_or_else(||Error::Invalid("NaN policy value".into()))?,(Value::Integer(a),Value::Real(b))=>integer_real(*a,*b)?,(Value::Real(a),Value::Integer(b))=>integer_real(*b,*a)?.reverse(),(Value::Text(a),Value::Text(b))=>a.cmp(b),(Value::Blob(a),Value::Blob(b))=>a.cmp(b),_=>return Err(Error::Invalid("policy value types".into()))})}
fn integer_real(a:i64,b:f64)->Result<std::cmp::Ordering>{use std::cmp::Ordering;if b.is_nan(){return Err(Error::Invalid("NaN policy value".into()));}if b>=9223372036854775808.0{return Ok(Ordering::Less);}if b< -9223372036854775808.0{return Ok(Ordering::Greater);}let integer=b as i64;let order=a.cmp(&integer);if order!=Ordering::Equal{return Ok(order);}Ok(if b.fract()>0.0{Ordering::Less}else if b.fract()<0.0{Ordering::Greater}else{Ordering::Equal})}

/// Join a losing-incarnation state into the winning row's state. The winner
/// keeps its baseline (its incarnation won the row merge); the orphan's seed
/// tags never survive a baseline takeover. Scopes must match and the orphan
/// basis must not exceed the winner's, otherwise the orphan stays carried
/// but unprojected (false).
fn rebase_orphan(winner:&mut State,orphan:&State)->Result<bool>{
    if (orphan.domain,orphan.branch)!=(winner.domain,winner.branch)||orphan.basis>winner.basis{return Ok(false);}
    let mut source=orphan.clone();
    if source.baseline!=winner.baseline{
        if let Causal::Set(set)=&mut source.causal{for tags in set.additions.values_mut(){tags.retain(|t|t.node!=[0;16]);}}
        source.baseline=winner.baseline.clone();
    }
    join_state(winner,&source)?;
    Ok(true)
}

fn join_state(target:&mut State,source:&State)->Result<()> {
    target.version=target.version.max(source.version);
    let mut source=source.clone();
    if source.basis>target.basis {
        if let Causal::Set(set)=&mut target.causal {for tags in set.additions.values_mut(){tags.retain(|t|t.node!=[0;16]);}}
        target.baseline=source.baseline.clone();target.basis=source.basis;
    }else if source.basis<target.basis {if let Causal::Set(set)=&mut source.causal {for tags in set.additions.values_mut(){tags.retain(|t|t.node!=[0;16]);}}}
    else if source.baseline!=target.baseline {return Err(Error::Corrupt("different causal baselines for one identity".into()));}
    target.causal.join(&source.causal).map_err(|e|Error::Corrupt(e.to_string()))
}

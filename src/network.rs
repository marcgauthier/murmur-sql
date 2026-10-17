//! Spool-backed adapter. Network workers never own a SQLite connection.
use crate::{*,database::{Inner,Core},logical_store::LogicalStore,model::*,storage::{Opened,Work,Event},metadata,merge,schema};
use murmur_replication::{self as net, Backend,Record};
use std::sync::{Arc,Weak,mpsc};
#[cfg(test)]
#[path = "retention_tests.rs"]
mod retention_tests;
#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod snapshot_tests;
fn error(e:Error)->net::Error {net::Error::Storage(e.to_string())}
pub(crate) struct Adapter {merge_deferrals:std::sync::atomic::AtomicUsize,cancelled:Arc<std::sync::atomic::AtomicBool>,inner:Weak<Inner>,worker:mpsc::Sender<Work>,identity:([u8;16],[u8;16]),last_reclaim_ms:std::sync::atomic::AtomicU64,last_apply_barrier_ms:std::sync::atomic::AtomicU64,progress_cache:std::sync::Mutex<Option<(std::time::Instant,Vec<net::protocol::Progress>)>>}
/// Minimum spacing between physical reclaims. Collects may run every
/// second, but segment compaction costs ~1s; without this gate the
/// storage worker saturates and replication stalls behind it.
const RECLAIM_MIN_INTERVAL_MILLIS: u64 = 60_000;
static WORKER_READS: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
fn worker_reads_forced() -> bool {
    *WORKER_READS.get_or_init(|| std::env::var("MURMUR_WORKER_READS").is_ok())
}
impl Adapter {
    /// Direct read handle bypassing the storage worker queue. Spool reads
    /// are thread-safe (`Store::get` takes `&self`), and the replication
    /// hot path (push fan-out, apply select, Need serving) issues hundreds
    /// of point reads per second per node; routing each through the single
    /// worker thread inflates every apply to hundreds of milliseconds of
    /// queue wait and throttles catch-up to tens of transactions per
    /// second. Commits stay on the worker (write ordering); reads that hit
    /// a stale post-Retry handle (`Closed`) or a torn >1MiB pieced-blob
    /// read racing a concurrent commit (`Corrupt`) fall back to the
    /// serialized worker, which returns the clean current view.
    fn direct_store(&self) -> Option<LogicalStore> {
        if worker_reads_forced() {
            return None;
        }
        self.inner
            .upgrade()
            .map(|inner| LogicalStore::new(inner.store.clone()))
    }
    pub(crate) fn call<T:Send+'static>(&self,f:impl FnOnce(&mut Core)->Result<T>+Send+'static)->Result<T> {
        let database=self.database().map_err(|e|Error::Storage(e.to_string()))?;
        database.call_cancel(f,self.cancelled.clone())
    }

    pub(crate) fn database(&self)->net::Result<Database>{Ok(Database{inner:self.inner.upgrade().ok_or_else(||net::Error::Unavailable("database closed".into()))?})}
    pub(crate) fn spool<T:Send+'static>(&self,f:impl FnOnce(&mut Opened)->Result<T>+Send+'static)->net::Result<T>{let(tx,rx)=mpsc::channel();self.worker.send(Work::Network(Box::new(move|o|{let _=tx.send(f(o));}))).map_err(|_|net::Error::Unavailable("storage worker stopped".into()))?;net::run_blocking(|| rx.recv()).map_err(|_|net::Error::Unavailable("storage reply lost".into()))?.map_err(error)}
}
impl Backend for Adapter {
    fn cancel_pending(&self){self.cancelled.store(true,std::sync::atomic::Ordering::Release);}
    fn identity(&self)->([u8;16],[u8;16]){self.identity}
    fn get(&self,key:&[u8])->net::Result<Option<Vec<u8>>>{
        if let Some(store)=self.direct_store() {
            match store.get(key) {
                Ok(value)=>return Ok(value),
                Err(e) if e.is_closed()||e.kind()==spool::ErrorKind::Corrupt=>{}
                Err(e)=>return Err(error(e.into())),
            }
        }
        let key=key.to_vec();self.spool(move|o|o.store.get(&key).map_err(Error::from))}
    fn get_many(&self,keys:&[Vec<u8>])->net::Result<Vec<Option<Vec<u8>>>>{
        if let Some(store)=self.direct_store() {
            let mut out=Vec::with_capacity(keys.len());let mut fallback=false;
            for key in keys {match store.get(key){Ok(value)=>out.push(value),Err(e) if e.is_closed()||e.kind()==spool::ErrorKind::Corrupt=>{fallback=true;break;}Err(e)=>return Err(error(e.into())),}}
            if !fallback{return Ok(out);}
        }
        let keys=keys.to_vec();self.spool(move|o|{let mut out=Vec::with_capacity(keys.len());for key in &keys{out.push(o.store.get(key).map_err(Error::from)?);}Ok(out)})}
    fn get_many_bounded(&self,keys:&[Vec<u8>],max_bytes:usize)->net::Result<Vec<Option<Vec<u8>>>>{
        if let Some(store)=self.direct_store() {
            let mut out=Vec::new();let mut bytes=0usize;let mut fallback=false;
            for key in keys {match store.get(key){Ok(value)=>{let size=value.as_ref().map_or(0,Vec::len);if !out.is_empty()&&bytes.saturating_add(size)>max_bytes{break;}bytes=bytes.saturating_add(size);out.push(value);if bytes>=max_bytes{break;}}Err(e) if e.is_closed()||e.kind()==spool::ErrorKind::Corrupt=>{fallback=true;break;}Err(e)=>return Err(error(e.into())),}}
            if !fallback{return Ok(out);}
        }
        let keys=keys.to_vec();self.spool(move|o|{let mut out=Vec::new();let mut bytes=0usize;for key in &keys{let value=o.store.get(key).map_err(Error::from)?;let size=value.as_ref().map_or(0,Vec::len);if !out.is_empty()&&bytes.saturating_add(size)>max_bytes{break;}bytes=bytes.saturating_add(size);out.push(value);if bytes>=max_bytes{break;}}Ok(out)})}
    fn keys(&self,prefix:&[u8])->net::Result<Vec<Vec<u8>>>{let prefix=prefix.to_vec();self.spool(move|o|Ok(o.store.keys_with_prefix(&prefix)))}
    fn keys_page(&self,prefix:&[u8],after:Option<&[u8]>,limit:usize)->net::Result<Vec<Vec<u8>>> {
        let prefix=prefix.to_vec();let after=after.map(|key|key.to_vec());self.spool(move|o|Ok(o.store.keys_page(&prefix,after.as_deref(),limit)))
    }
    fn progress(&self)->net::Result<Vec<net::protocol::Progress>> {
        // Memoized for a second: push ticks and progress answers read heads
        // ten times a second per node, and every read is a storage-worker
        // round-trip. Callers use heads as advisory send bounds and repair
        // hints that self-correct on the next round; a one-second lag only
        // pauses the send frontier briefly.
        if let Some((at,heads))=self.progress_cache.lock().unwrap().clone() {
            if at.elapsed()<std::time::Duration::from_secs(1) {
                return Ok(heads);
            }
        }
        let heads:Vec<net::protocol::Progress>=self.spool(|o|{
            use net::protocol::*;let mut origins=std::collections::BTreeMap::new();
            for key in o.store.keys_with_prefix(b"07/replication/retained/") {if let Some(bytes)=o.store.get(&key)? {let head:Progress=decode(&bytes)?;origins.insert(head.origin,head);}}
            for (prefix,observed) in [(b"07/replication/applied/".as_slice(),false),(b"07/replication/observed/".as_slice(),true)] {
                for key in o.store.keys_with_prefix(prefix) {if let Some(bytes)=o.store.get(&key)? {
                    let name=std::str::from_utf8(&key).map_err(|_|Error::Corrupt("progress key".into()))?.rsplit('/').next().unwrap();
                    if name.len()!=32 || !name.is_ascii(){return Err(Error::Corrupt("progress origin".into()));}
                    let mut origin=[0;16];for (index,value) in origin.iter_mut().enumerate(){*value=u8::from_str_radix(&name[index*2..index*2+2],16).map_err(|_|Error::Corrupt("progress origin".into()))?;}
                    let head=origins.entry(origin).or_insert(Progress{origin,applied:0,observed:0,retained_from:0,retained_through:0});if observed {head.observed=decode(&bytes)?;}else{head.applied=decode(&bytes)?;}
                }}
            }
            for head in origins.values_mut(){head.observed=head.observed.max(head.applied);}Ok(origins.into_values().collect())
        })?;
        *self.progress_cache.lock().unwrap()=Some((std::time::Instant::now(),heads.clone()));
        Ok(heads)
    }
    fn commit(&self,records:Vec<Record>)->net::Result<()> {self.spool(move|o|{let records=records.into_iter().map(|r|match r.value{Some(v)=>spool::Mutation::put(r.key,v),None=>spool::Mutation::delete(r.key)}).collect::<Vec<_>>();o.store.commit(&records,spool::Durability::Sync)?;Ok(())})}
    fn import_bridge(&self,header:net::bridge::Header,payload:Vec<u8>)->net::Result<bool>{crate::high_low::import(self,header,payload)}
    fn release_ownership(&self,table:String,key:Vec<u8>,column:Option<String>)->net::Result<bool>{crate::high_low::release(self,table,key,column)}
    fn publish(&self,records:Vec<Record>)->net::Result<()> {
        self.spool(move |o| {
            let config = o.replication_config.clone().ok_or_else(||Error::Invalid("replication disabled".into()))?;
            validate_auxiliary(&records)?;
            let mut changes:Vec<_> = records.iter().map(|r|match &r.value {Some(v)=>spool::Mutation::put(r.key.clone(),v.clone()),None=>spool::Mutation::delete(r.key.clone())}).collect();
            let clock=o.clock.next(o.identity.node)?;
            changes.extend(sign_content(o,&config,net::Content {sqlite:None,records,origin:o.identity.node},clock,&o.schema)?);
            changes.push(spool::Mutation::put(b"06/clock".to_vec(),encode(&clock)?));
            o.store.commit(&changes,spool::Durability::Sync)?;o.clock=clock;Ok(())
        })
    }
    fn snapshot(&self)->net::Result<Vec<u8>> { self.spool(snapshot_content) }
    fn snapshot_with_progress(&self)->net::Result<(Vec<u8>,Vec<net::protocol::Progress>)> {
        self.spool(|o| Ok((snapshot_content(o)?, durable_progress(o)?)))
    }
    fn create_snapshot(
        &self,
        recipient: net::NodeId,
        config: &net::Config,
    ) -> net::Result<net::snapshot::Manifest> {
        let config = config.clone();
        let stream_config=config.clone();
        let captured=self.spool(move |o| {
            use net::snapshot::*;
            let now = net::retention::now_millis();
            let schema = sha_schema(&o.schema)?;
            for key in o
                .store
                .keys_with_prefix(OUT)
                .into_iter()
                .filter(|k| k.ends_with(b"/header"))
            {
                if let Some(bytes) = o.store.get(&key)? {
                    let m: Manifest =
                        net::decode(&bytes).map_err(|e| Error::Corrupt(e.to_string()))?;
                    let pin = o
                        .store
                        .get(&pin_key(m.transfer))?
                        .map(|b| net::decode::<Pin>(&b))
                        .transpose()
                        .map_err(|e| Error::Corrupt(e.to_string()))?;
                    if m.recipient == recipient
                        && m.expires > now
                        && m.schema == schema
                        && pin.is_some_and(|p| p.target.is_none())
                    {
                        // A chunkless header is an in-progress build: a
                        // fresh one means a sibling task is streaming it
                        // (report busy so the requester retries into the
                        // finalized build), a stale one is a dead build's
                        // leftovers (ignore it and build anew; maintain()
                        // sweeps it at grace expiry).
                        if m.chunks.is_empty() {
                            if m.expires.saturating_sub(now) > net::snapshot::BUILD_FRESH_MARGIN_MILLIS {
                                return Err(Error::Busy);
                            }
                            continue;
                        }
                        return Ok(Err(m));
                    }
                }
            }
            if o.store.keys_with_prefix(PINS).len() >= config.staging_transfers {
                return Err(Error::Limit("snapshot transfer count".into()));
            }
            let used=o.store.keys_with_prefix(OUT).into_iter().filter(|key|!key.ends_with(b"/header")).count().checked_mul(net::protocol::CHUNK_BYTES).ok_or(Error::Limit("snapshot staging bytes".into()))?;
            let budget=config.snapshot_staging_bytes.saturating_sub(used);
            if budget==0{return Err(Error::Limit("snapshot staging bytes".into()));}
            let parent=std::path::Path::new(&o.opts.path).parent().ok_or_else(||Error::Invalid("snapshot parent".into()))?;
            let path=parent.join(format!(".murmur-snapshot-{}",net::hex(&rand::random::<[u8;16]>() )));
            let checkpoint=o.store.checkpoint(&path,None)?;
            let mut options=o.opts.clone();options.path=path.to_string_lossy().into_owned();options.reclaim_interval=None;
            Ok(Ok(crate::snapshot_stream::Source{checkpoint,options,schema:o.schema.clone(),clock:o.clock,identity:(o.identity.database,o.identity.node),progress:durable_progress(o)?,budget}))
        })?;
        let source=match captured {Ok(source)=>source,Err(manifest)=>return Ok(manifest)};
        let pages=crate::snapshot_stream::Pages::open(source).map_err(error)?;
        let identity=pages.source().identity;let progress=pages.source().progress.clone();let schema=sha_schema(&pages.source().schema).map_err(error)?;
        let mut config=stream_config;
        config.snapshot_staging_bytes=config.snapshot_staging_bytes.min(pages.source().budget);
        net::snapshot::build_stream(identity,recipient,pages,progress,schema,&config,net::retention::now_millis(),|records|self.commit(records))
    }
    fn acknowledge_snapshot(
        &self,
        recipient: net::NodeId,
        transfer: net::NodeId,
        digest: [u8; 32],
    ) -> net::Result<()> {
        self.spool(move |o| {
            use net::snapshot::*;
            let key = pin_key(transfer);
            let Some(bytes) = o.store.get(&key)? else {
                return Ok(());
            };
            let mut pin: Pin = net::decode(&bytes).map_err(|e| Error::Corrupt(e.to_string()))?;
            if pin.recipient != recipient || pin.digest != digest {
                return Err(Error::Invalid("snapshot acknowledgement identity".into()));
            }
            if pin.expires <= net::retention::now_millis() {
                return Err(Error::Invalid("snapshot acknowledgement expired".into()));
            }
            if pin.target.is_none() {
                pin.target = Some(
                    durable_progress(o)?
                        .into_iter()
                        .map(|p| (p.origin, p.applied))
                        .collect(),
                );
                o.store.commit(
                    &[spool::Mutation::put(
                        key,
                        net::encode(&pin).map_err(|e| Error::Invalid(e.to_string()))?,
                    )],
                    spool::Durability::Sync,
                )?;
            }
            Ok(())
        })
    }
    fn retention_checkpoint(&self)->net::Result<()> {
        self.spool(|o| {
            crate::storage::snapshot(o)?;
            let coverage:std::collections::BTreeMap<_,_> = durable_progress(o)?.into_iter().map(|p|(p.origin,p.applied)).collect();
            o.store.commit(&[spool::Mutation::put(net::retention::COVERAGE_KEY.to_vec(),encode(&(o.sequence,coverage))?)],spool::Durability::Sync)?;
            Ok(())
        })
    }
    fn prune_history(&self,origin:[u8;16],through:u64)->net::Result<u64> {
        let database = self.database()?;
        let ticket = database.inner.scheduler.admit_timeout(crate::scheduler::WriterClass::Maintenance,std::time::Duration::from_millis(100)).map_err(error)?;
        let res = self.spool(move|o| {
            use net::{protocol::*,retention::*};
            let old=o.store.get(&floor_key(origin))?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0);
            if through<=old {return Ok(0);}
            // Recheck transfer obligations on this same serialized worker that
            // publishes captures, so a newly created pin cannot be missed.
            for key in o.store.keys_with_prefix(net::snapshot::PINS) {
                if let Some(bytes)=o.store.get(&key)? {
                    let pin:net::snapshot::Pin=net::decode(&bytes).map_err(|e|Error::Corrupt(e.to_string()))?;
                    if pin.expires>net::retention::now_millis() && through>pin.boundary.get(&origin).copied().unwrap_or(0) {
                        return Err(Error::Busy);
                    }
                }
            }

            let bytes=o.store.get(COVERAGE_KEY)?.ok_or_else(||Error::Invalid("retention snapshot missing".into()))?;
            let (sequence,coverage):(u64,std::collections::BTreeMap<[u8;16],u64>)=decode(&bytes)?;
            if sequence>o.sequence || coverage.get(&origin).copied().unwrap_or(0)<through {
                return Err(Error::Invalid("history not covered by durable snapshot".into()));
            }
            let mut head:Progress=net::decode(&o.store.get(&retained_key(origin))?.ok_or_else(||Error::Corrupt("retention head missing".into()))?).map_err(|e|Error::Corrupt(e.to_string()))?;
            let applied=o.store.get(&progress_key(origin))?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0);
            if through>applied || through>=head.retained_through {return Err(Error::Invalid("retention would remove current history".into()));}
            let keys=o.store.keys_page(format!("07/replication/log/{}/",net::hex(&origin)).as_bytes(),None,257);
            let remaining=head.retained_through.saturating_sub(through);
            let minimum=o.replication_config.as_ref().ok_or_else(||Error::Invalid("replication disabled".into()))?.minimum_retained;
            if (remaining as u64)<minimum {return Err(Error::Invalid("retention minimum would be violated".into()));}
            let mut changes=Vec::new();let mut count=0u64;
            for key in keys {
                let seq=key_sequence(&key)?;if seq>through {break;}
                if count>=256 {return Err(Error::Limit("retention deletion batch".into()));}
                changes.push(spool::Mutation::delete(key));
                changes.push(spool::Mutation::delete(time_key(origin,seq)));count+=1;
            }
            let receipts=o.store.keys_page(format!("07/replication/receipt/{}/",net::hex(&origin)).as_bytes(),None,257);
            let mut receipt_count=0;for key in receipts {if key_sequence(&key)?>through {break;}if receipt_count>=256 {return Err(Error::Limit("retention receipt batch".into()));}changes.push(spool::Mutation::delete(key));receipt_count+=1;}
            head.retained_from=head.retained_from.max(through.checked_add(1).ok_or_else(||Error::Invalid("sequence exhausted".into()))?);
            changes.push(spool::Mutation::put(retained_key(origin),net::encode(&head).map_err(|e|Error::Storage(e.to_string()))?));
            changes.push(spool::Mutation::put(floor_key(origin),encode(&through)?));
            let total=o.store.get(b"07/replication/retention/collected")?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0);
            changes.push(spool::Mutation::put(b"07/replication/retention/collected".to_vec(),encode(&total.saturating_add(count))?));
            o.store.commit(&changes,spool::Durability::Sync)?;
            Ok(count)
        });
        drop(ticket);
        res
    }
    fn reclaim_retention(&self)->net::Result<()> {
        let now = net::retention::now_millis();
        let last = self.last_reclaim_ms.load(std::sync::atomic::Ordering::Relaxed);
        if now.saturating_sub(last) < RECLAIM_MIN_INTERVAL_MILLIS {
            return Ok(());
        }
        if self.last_reclaim_ms.compare_exchange_weak(last, now, std::sync::atomic::Ordering::Relaxed, std::sync::atomic::Ordering::Relaxed).is_err() {
            return Ok(());
        }
        let database = self.database()?;
        let ticket = database.inner.scheduler.admit_timeout(crate::scheduler::WriterClass::Maintenance,std::time::Duration::from_millis(100)).map_err(error)?;
        let res = self.spool(|o|o.store.reclaim().map_err(Error::from));
        drop(ticket);
        res
    }
    fn schema(&self)->net::Result<Vec<u8>>{self.spool(|o|encode(&canonical_schema(&crate::table_replication::filter_schema(&o.schema))?))}
    fn observe(&self,millis:u64,logical:u32)->net::Result<()>{self.call(move|c|{c.collection_clock=c.collection_clock.max(Version{millis,logical,node:[0xff;16]});Ok(())}).map_err(error)}
    fn accept_schema(&self,bytes:Vec<u8>)->net::Result<bool>{
        let incoming:Schema=decode(&bytes).map_err(error)?;
        let database = self.database()?;
        let ticket = match database.inner.scheduler.admit_timeout(crate::scheduler::WriterClass::Remote,std::time::Duration::from_millis(100)){Ok(ticket)=>ticket,Err(Error::Busy)=>return Ok(false),Err(e)=>return Err(error(e))};
        let res = self.call(move|c|c.adopt_schema(incoming)).map_err(error);
        drop(ticket);
        res
    }
    fn apply(&self,payloads:Vec<Vec<u8>>,records:Vec<Record>)->net::Result<bool>{
        let t_apply=std::time::Instant::now();let timing=std::env::var("MURMUR_APPLY_TIMING").is_ok();
        let database=self.database()?;
        let reserved=self.merge_deferrals.load(std::sync::atomic::Ordering::Relaxed)>=2;
        // Parse authenticated bounded payloads before taking a publication reservation.
        let mut decoded=Vec::new();let mut auxiliary=Vec::new();
        for payload in payloads {let content:net::Content=decode_content(&payload).map_err(error)?;validate_auxiliary(&content.records).map_err(error)?;
            if let Some(bytes)=content.sqlite {decoded.push((decode::<ReplicaState>(&bytes).map_err(error)?,content.origin));}auxiliary.extend(content.records);
        }
        let t_decode=t_apply.elapsed();
        let mut ticket=Some(match database.inner.scheduler.admit_timeout(crate::scheduler::WriterClass::Remote,std::time::Duration::from_millis(100)){Ok(ticket)=>ticket,Err(Error::Busy)=>return Ok(false),Err(e)=>return Err(error(e))});
        let t_admit1=t_apply.elapsed();
        let cursor=self.call(move|c|{let needs_capture={let s=c.status.lock().unwrap();s.committed!=c.captured_cursor};if needs_capture{c.checkpoint()?;}let s=c.status.lock().unwrap();if s.storage_error.is_some(){return Ok(None);}Ok(Some(s.committed))}).map_err(error)?;
        let Some(cursor)=cursor else{self.merge_deferrals.fetch_add(1,std::sync::atomic::Ordering::Relaxed);return Ok(false);};
        // Initial collection consumes remote writer time too. Optimistic merges
        // release that admission before waiting on Spool preparation.
        if !reserved {drop(ticket.take());}
        let (patch, auxiliary,keys,disjoint)=self.spool(move|o|{if o.cursor!=cursor{return Err(Error::Busy);}let mut incoming_states=Vec::new();
            for (mut state,origin) in decoded {drop_retired_tables(o,&mut state,&origin)?;incoming_states.push(state);}
            auxiliary=crate::table_replication::filter_records(auxiliary,&o.schema)?;
            let disjoint=auxiliary.is_empty();
            let mut keys=crate::high_low::affected_records(&auxiliary)?;
            for state in &incoming_states {for row in &state.rows {if crate::table_replication::enabled(&o.schema,&row.table){keys.insert((row.table.clone(),encode(&row.key)?));}}
                for tomb in &state.tombstones {if crate::table_replication::enabled(&o.schema,&tomb.table){keys.insert((tomb.table.clone(),tomb.key.clone()));}}}
            let baseline=metadata::affected_state(&o.store,o.schema.clone(),o.identity.database,o.clock,&keys)?;
            for incoming in &mut incoming_states {

                for table in &incoming.schema.tables {if !crate::table_replication::enabled(&baseline.schema,&table.id){continue;}let local=baseline.schema.tables.iter().find(|t|t.id==table.id).ok_or(Error::SchemaMismatch)?;if local.name!=table.name || table.columns.iter().any(|c|!local.columns.iter().any(|l| {
                    let mut historical=c.clone();historical.fts=l.fts;historical.vector=l.vector.clone();*l==historical
                })) {return Err(Error::SchemaMismatch);}}
                for row in &mut incoming.rows {if !crate::table_replication::enabled(&baseline.schema,&row.table){continue;}let table=baseline.schema.tables.iter().find(|t|t.id==row.table).ok_or(Error::SchemaMismatch)?;for column in &table.columns {if !row.cells.iter().any(|c|c.column==column.id) {if column.not_null || column.primary_key>0 {return Err(Error::SchemaMismatch);}row.cells.push(Cell{column:column.id.clone(),value:Value::Null,version:Version::default()});}}}
                crate::table_replication::filter(incoming, &baseline.schema);
                incoming.schema=baseline.schema.clone();
            }
            let mut joined=merge::join_batch(&baseline,&incoming_states)?;
            auxiliary=crate::table_replication::filter_records(auxiliary, &o.schema)?;
            auxiliary=crate::high_low::join(o,auxiliary,&mut joined)?;
            auxiliary=crate::policy::join(o,auxiliary,&mut joined)?;
            auxiliary=join_auxiliary(o,auxiliary)?;
            crate::table_replication::preserve_local(&mut joined, &baseline);
            Ok((merge::prepare_delta(&baseline,joined,cursor)?,auxiliary,keys,disjoint))})?;
        let t_spool=t_apply.elapsed();
        if ticket.is_none(){ticket=Some(match database.inner.scheduler.admit_timeout(crate::scheduler::WriterClass::Remote,std::time::Duration::from_millis(100)){Ok(ticket)=>ticket,Err(Error::Busy)=>return Ok(false),Err(e)=>return Err(error(e))});}
        let t_admit2=t_apply.elapsed();

        let snapshot_complete=records.iter().any(|record|record.value.is_some() && record.key.starts_with(net::snapshot::RECEIPTS));
        let mut records=records;records.extend(auxiliary);
        let published=self.call(move|c|{let mut patch=patch;if disjoint && !c.advance_disjoint_patch(&mut patch,&keys)?{return Ok(false);}c.publish_network(patch,records,false)}).map_err(error)?;
        if !published{self.merge_deferrals.fetch_add(1,std::sync::atomic::Ordering::Relaxed);return Ok(false);}
        drop(ticket);
        self.merge_deferrals.store(0,std::sync::atomic::Ordering::Relaxed);
        // Durability is time-bounded, not per-batch. The durable staging
        // remains until the state, receipt, watermark and cleanup commit
        // together, so an acked-but-not-yet-flushed apply replays from its
        // staged records after a crash; the gate below bounds that replay
        // tail to ~1s of applies. A per-batch barrier serializes every
        // apply behind a full flush (~130ms colocated) for no additional
        // safety. Clean shutdown still flushes via Work::Stop.
        let t_publish=t_apply.elapsed();
        let now_ms=net::retention::now_millis();
        let last=self.last_apply_barrier_ms.load(std::sync::atomic::Ordering::Relaxed);
        if snapshot_complete || (now_ms.saturating_sub(last)>=1000 && self.last_apply_barrier_ms.compare_exchange_weak(last,now_ms,std::sync::atomic::Ordering::Relaxed,std::sync::atomic::Ordering::Relaxed).is_ok()) {
            let (tx, rx) = mpsc::channel();
            self.worker.send(Work::Barrier(tx)).map_err(|_| net::Error::Unavailable("storage worker stopped".into()))?;
            rx.recv().map_err(|_|net::Error::Unavailable("durability reply lost".into()))?.map_err(error)?;
        }
        if timing{let total=t_apply.elapsed();if total.as_millis()>=100{eprintln!("[backend-timing] total={}ms decode={}ms admit1={}ms spool={}ms admit2={}ms publish={}ms barrier={}ms",total.as_millis(),t_decode.as_millis(),t_admit1.saturating_sub(t_decode).as_millis(),t_spool.saturating_sub(t_admit1).as_millis(),t_admit2.saturating_sub(t_spool).as_millis(),t_publish.saturating_sub(t_admit2).as_millis(),total.saturating_sub(t_publish).as_millis());}}
        Ok(true)
    }
}
pub(crate) fn attach(database:&Database,config:net::Config)->Result<net::Handle>{let adapter=Arc::new(Adapter{merge_deferrals:std::sync::atomic::AtomicUsize::new(0),cancelled:Arc::new(std::sync::atomic::AtomicBool::new(false)),inner:Arc::downgrade(&database.inner),worker:database.inner.worker.clone(),identity:(database.database_id(),database.node_id()),last_reclaim_ms:std::sync::atomic::AtomicU64::new(0),last_apply_barrier_ms:std::sync::atomic::AtomicU64::new(0),progress_cache:std::sync::Mutex::new(None)});net::start(config,adapter).map_err(|e|Error::Invalid(e.to_string()))}
impl Core {
    pub(crate) fn publish_network(&mut self,patch:merge::Patch,records:Vec<Record>,remint:bool)->Result<bool>{
        {let s=self.status.lock().unwrap();if s.committed!=patch.cursor || s.committed!=self.captured_cursor || s.storage_error.is_some(){return Ok(false);}}
        let bytes=encode(&(&patch.rows,&patch.deleted,&patch.tombstones,patch.clock))?.len()+records.iter().map(|r|r.key.len()+r.value.as_ref().map_or(0,Vec::len)).sum::<usize>();self.admit(bytes)?;
        // Destroy the clean session: remote publication must never feed local capture.
        self.capture.take();self.connection.execute_batch("BEGIN IMMEDIATE")?;
        let result=(||->Result<()>{schema::delete_user_rows(&self.connection,&self.schema,&patch.deleted)?;schema::put_user_rows(&self.connection,&self.schema,&patch.rows)?;self.connection.execute_batch("COMMIT")?;Ok(())})();
        if result.is_err(){let _=self.connection.execute_batch("ROLLBACK");}self.capture=Some(schema::capture(&self.connection,&self.schema)?);result?;
        self.search.rows(&self.schema, &patch.rows);self.search.rows(&self.schema, &patch.deleted);
        self.status.lock().unwrap().search_indexes = self.search.status();
        let cursor=self.bump()?;self.collection_clock=self.collection_clock.max(patch.clock);
        let mut affected:Vec<String>=patch.rows.iter().chain(patch.deleted.iter()).filter_map(|r|self.schema.tables.iter().find(|t|t.id==r.table).map(|t|t.name.clone())).collect();affected.sort();affected.dedup();
        self.send(Event::ReplicaPatch{rows:patch.rows,deleted:patch.deleted,tombstones:patch.tombstones,clock:patch.clock,records:records.into_iter().map(|r|(r.key,r.value)).collect(),remint},cursor,bytes)?;self.captured_cursor=cursor;self.sub_mgr.notify_commit(cursor,&affected);Ok(true)
    }
}
pub(crate) fn sign_local(opened:&Opened,config:&net::Config,mut state:ReplicaState,records:Vec<Record>)->Result<Vec<spool::Mutation>>{
    let schema = state.schema.clone();
    crate::table_replication::filter(&mut state, &schema);
    let records = crate::table_replication::filter_records(records, &schema)?;
    state.schema=crate::table_replication::filter_schema(&schema);
    sign_content(opened,config,net::Content{sqlite:Some(encode(&state)?),records,origin:opened.identity.node},state.clock,&state.schema)
}
fn sign_content(opened:&Opened,config:&net::Config,content:net::Content,clock:Version,schema:&Schema)->Result<Vec<spool::Mutation>> {
    use net::protocol::*;
    let origin=opened.identity.node;let sequence=opened.store.get(&sequence_key(origin))?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0).checked_add(1).ok_or_else(||Error::Corrupt("origin sequence exhausted".into()))?;
    let mut tx=Transaction{database:opened.identity.database,origin,sequence,id:rand::random(),millis:clock.millis,logical:clock.logical,schema:sha_schema(schema)?,payload_digest:[0;32],payload:net::encode(&content).map_err(|e|Error::Storage(e.to_string()))?,signature:Vec::new()};if tx.payload.len()>config.max_transaction_bytes {return Err(Error::Limit("signed replication content exceeds max_transaction_bytes; reduce capture size or raise the configured limit".into()));}tx.sign(&config.signing_key).map_err(|e|Error::Storage(e.to_string()))?;
    let head_key=format!("07/replication/retained/{}",net::hex(&origin)).into_bytes();
    let mut head=opened.store.get(&head_key)?.map(|b|net::decode::<Progress>(&b).map_err(|e|Error::Corrupt(e.to_string()))).transpose()?.unwrap_or(Progress{origin,applied:sequence,observed:sequence,retained_from:sequence,retained_through:sequence});head.applied=sequence;head.observed=sequence;head.retained_through=sequence;
    Ok(vec![spool::Mutation::put(head_key,net::encode(&head).map_err(|e|Error::Storage(e.to_string()))?),spool::Mutation::put(log_key(origin,sequence),net::encode(&tx).map_err(|e|Error::Storage(e.to_string()))?),spool::Mutation::put(net::retention::time_key(origin,sequence),encode(&net::retention::now_millis())?),spool::Mutation::put(sequence_key(origin),encode(&sequence)?),spool::Mutation::put(progress_key(origin),encode(&sequence)?),spool::Mutation::put(receipt_key(origin,sequence),tx.digest().map_err(|e|Error::Storage(e.to_string()))?.to_vec())])
}
fn sha_schema(schema:&Schema)->Result<[u8;32]>{// The protocol uses SHA-256; keep hashing in the background crate.
    // Bind to the replicated subset so local-only tables never look divergent.
    Ok(net::protocol::schema_digest(&encode(&canonical_schema(&crate::table_replication::filter_schema(schema))?)?))
}

fn snapshot_content(o:&mut Opened)->Result<Vec<u8>> {
    let mut state=metadata::state(&o.store,o.schema.clone(),o.identity.database,o.clock)?;
    crate::table_replication::filter(&mut state,&o.schema);
    state.schema=crate::table_replication::filter_schema(&o.schema);
    let mut records=Vec::new();for prefix in [b"08/files/meta/".as_slice(),b"10/policy/",b"09/bridge/owner/",b"09/bridge/shadow/",b"09/bridge/provenance/"] {for key in o.store.keys_with_prefix(prefix) {if let Some(value)=o.store.get(&key)? {records.push(Record::put(key,value));}}}
    let records=crate::table_replication::filter_records(records,&o.schema)?;
    net::encode(&net::Content {sqlite:Some(encode(&state)?),records,origin:o.identity.node}).map_err(|e|Error::Storage(e.to_string()))
}
fn key_sequence(key:&[u8])->Result<u64> {
    std::str::from_utf8(key).ok().and_then(|key|key.rsplit('/').next()).and_then(|s|s.parse().ok())
        .ok_or_else(||Error::Corrupt("replication sequence key".into()))
}
// Application of a snapshot may race with a file publication that advances
// progress without changing the SQLite cursor. Resolve watermarks at commit time.
pub(crate) fn monotonic_progress(
    o: &Opened,
    records: &mut [(Vec<u8>, Option<Vec<u8>>)],
) -> Result<()> {
    let mut seen = std::collections::BTreeMap::<Vec<u8>, u64>::new();
    for (key, value) in records {
        if key.starts_with(b"07/replication/applied/") {
            if let Some(bytes) = value {
                let old = match seen.get(key) {
                    Some(v) => *v,
                    None => o
                        .store
                        .get(key)?
                        .map(|b| decode::<u64>(&b))
                        .transpose()?
                        .unwrap_or(0),
                };
                let next = decode::<u64>(bytes)?.max(old);
                *bytes = encode(&next)?;
                seen.insert(key.clone(), next);
            }
        }
    }
    Ok(())
}
fn durable_progress(o:&Opened)->Result<Vec<net::protocol::Progress>> {
    let mut entries=Vec::new();for key in o.store.keys_with_prefix(b"07/replication/applied/") {
        let name=std::str::from_utf8(&key).map_err(|_|Error::Corrupt("progress key".into()))?.rsplit('/').next().unwrap();
        if name.len()!=32 || !name.is_ascii() {return Err(Error::Corrupt("progress origin".into()));}
        let mut origin=[0;16];for (index,value) in origin.iter_mut().enumerate() {*value=u8::from_str_radix(&name[index*2..index*2+2],16).map_err(|_|Error::Corrupt("progress origin".into()))?;}
        let applied=decode(&o.store.get(&key)?.ok_or_else(||Error::Corrupt("progress value".into()))?)?;
        let mut head:net::protocol::Progress=o.store.get(&net::protocol::retained_key(origin))?.map(|bytes|net::decode(&bytes).map_err(|e|Error::Corrupt(e.to_string()))).transpose()?.unwrap_or(net::protocol::Progress {origin,applied,observed:applied,retained_from:0,retained_through:0});
        head.applied=applied;head.observed=head.observed.max(applied);entries.push(head);
    }Ok(entries)
}

fn validate_auxiliary(records:&[Record])->Result<()> {
    if records.len()>10000 {return Err(Error::Limit("auxiliary records".into()));}
    for record in records {
        if !record.key.starts_with(b"08/files/meta/") && !record.key.starts_with(b"10/policy/") && !record.key.starts_with(b"09/bridge/owner/") && !record.key.starts_with(b"09/bridge/shadow/") && !record.key.starts_with(b"09/bridge/provenance/") {return Err(Error::Corrupt("unauthorized auxiliary record".into()));}
        if record.value.is_none() {return Err(Error::Corrupt("auxiliary tombstones must be logical".into()));}
    }Ok(())
}
/// Decode a data payload, accepting pre-origin payloads staged before the
/// upgrade as unknown-origin (strict schema checks, never lenient).
pub(crate) fn decode_content(bytes:&[u8])->Result<net::Content> {
    match net::decode(bytes) {
        Ok(content) => Ok(content),
        Err(_) => {
            #[derive(serde::Deserialize)]
            struct Legacy { sqlite:Option<Vec<u8>>, records:Vec<net::Record> }
            let legacy: Legacy =
                postcard::from_bytes(bytes).map_err(|e| Error::Corrupt(e.to_string()))?;
            Ok(net::Content { sqlite: legacy.sqlite, records: legacy.records, origin: [0; 16] })
        }
    }
}
/// Drop payload tables the sender has retired (absent from its latest signed
/// schema manifest) that this node never held, so old transactions stay
/// applicable. Tables held locally always merge (strict column checks still
/// apply); tables the sender still declares, or no manifest at all, keep
/// strict checking so genuine divergence surfaces as SchemaMismatch.
fn drop_retired_tables(o:&Opened,incoming:&mut ReplicaState,origin:&[u8;16])->Result<()> {
    let key = net::protocol::schema_accepted_key(*origin);
    let Some(raw) = o.store.get(&key)? else { return Ok(()); };
    let (_, manifest): (u64, Vec<u8>) =
        net::decode(&raw).map_err(|e| Error::Corrupt(e.to_string()))?;
    let declared: Schema = decode(&manifest)?;
    let live: std::collections::BTreeSet<&str> =
        declared.tables.iter().map(|t| t.id.as_str()).collect();
    let local: std::collections::BTreeSet<&str> =
        o.schema.tables.iter().map(|t| t.id.as_str()).collect();
    let retired: Vec<String> = incoming
        .schema
        .tables
        .iter()
        .filter(|t| !live.contains(t.id.as_str()) && !local.contains(t.id.as_str()))
        .map(|t| t.id.clone())
        .collect();
    if retired.is_empty() {
        return Ok(());
    }
    incoming.schema.tables.retain(|t| live.contains(t.id.as_str()));
    incoming.rows.retain(|r| !retired.contains(&r.table));
    incoming.tombstones.retain(|t| !retired.contains(&t.table));
    Ok(())
}
fn join_auxiliary(o:&Opened,records:Vec<Record>)->Result<Vec<Record>> {
    let mut joined=std::collections::BTreeMap::<Vec<u8>,Vec<u8>>::new();
    for record in records {
        let value=record.value.unwrap();
        let previous=joined.get(&record.key).cloned().or(o.store.get(&record.key)?);
        if record.key.starts_with(b"08/files/meta/") {
            let incoming:net::files::Metadata=net::decode(&value).map_err(|e|Error::Corrupt(e.to_string()))?;
            let prior=previous.as_ref().map(|v|net::decode::<net::files::Metadata>(v).map_err(|e|Error::Corrupt(e.to_string()))).transpose()?;
            if prior.is_none_or(|p|(incoming.millis,incoming.node,incoming.digest,incoming.deleted)>(p.millis,p.node,p.digest,p.deleted)) {joined.insert(record.key,value);}
        }else {joined.insert(record.key,value);}
    }
    Ok(joined.into_iter().map(|(k,v)|Record::put(k,v)).collect())
}

/// Network identity ignores physical SQLite column order and DDL spelling.
fn canonical_schema(schema:&Schema)->Result<Schema> {
    let mut canonical=schema.clone();canonical.tables.sort_by(|a,b|a.id.cmp(&b.id));
    for table in &mut canonical.tables {table.columns.sort_by(|a,b|a.id.cmp(&b.id));table.sql=table_definition(table)?;}
    canonical.indexes.sort();canonical.history=[0;32];canonical.history=*blake3::hash(&encode(&canonical)?).as_bytes();Ok(canonical)
}
fn column_definition(column:&Column)->Result<String> {
    if column.name.is_empty() || column.declared_type.len()>256 || !column.declared_type.bytes().all(|c|c.is_ascii_alphanumeric() || b" (),_".contains(&c)) {return Err(Error::SchemaMismatch);}
    Ok(format!("{} {}{}",quote(&column.name),column.declared_type,if column.not_null{" NOT NULL"}else{""}))
}
fn table_definition(table:&Table)->Result<String> {
    if table.name.is_empty() || table.columns.is_empty() || table.columns.len()>2000 {return Err(Error::SchemaMismatch);}
    crate::primary_key::validate_table(table)?;
    let source=crate::primary_key::column_definitions(&table.sql)?;
    let mut definitions=table.columns.iter().map(|column| {
        let _=column_definition(column)?;
        source.get(&column.name).cloned().ok_or(Error::SchemaMismatch)
    }).collect::<Result<Vec<_>>>()?;
    let keys=schema::pk_columns(table);if keys.is_empty(){return Err(Error::SchemaMismatch);}
    definitions.push(format!("PRIMARY KEY ({})",keys.iter().map(|c|quote(&c.name)).collect::<Vec<_>>().join(",")));
    if let Some(key)=&table.application_key {
        definitions.push(format!("UNIQUE ({}){}",key.columns.iter().enumerate().map(|(i,n)|format!("{} COLLATE {} {}",quote(n),quote(&key.collations[i]),if key.descending[i]{"DESC"}else{"ASC"})).collect::<Vec<_>>().join(","),crate::primary_key::conflict(key)));
    }
    Ok(format!("CREATE TABLE {} ({}){}",quote(&table.name),definitions.join(","),match table.application_key.as_ref().map(|k|(k.without_rowid,k.strict)) {Some((true,true))=>" WITHOUT ROWID, STRICT",Some((true,false))=>" WITHOUT ROWID",Some((false,true))=>" STRICT",_=>""}))
}
impl Core {
    fn adopt_schema(&mut self,mut incoming:Schema)->Result<bool> {
        for table in &incoming.tables { if table.application_key.is_none(){ return Err(Error::SchemaMismatch); } incoming.table_replication.insert(table.id.clone(),true); }
        // Compare replicated subsets: local-only tables never force adoption.
        let local=crate::table_replication::filter_schema(&self.schema);
        let remote=crate::table_replication::filter_schema(&incoming);
        if canonical_schema(&local)?==canonical_schema(&remote)? {return Ok(true);}
        {let s=self.status.lock().unwrap();if s.committed!=self.captured_cursor || s.durable!=self.captured_cursor {return Ok(false);}}
        let mut target=self.schema.clone();let mut ddl=String::new();
        for incoming_table in &incoming.tables {
            // Locally-disabled tables are never touched by adoption.
            if self.schema.tables.iter().any(|t|t.id==incoming_table.id)
                && !crate::table_replication::enabled(&self.schema,&incoming_table.id) { continue; }
            if let Some(local)=target.tables.iter_mut().find(|t|t.id==incoming_table.id) {
                if local.name!=incoming_table.name || local.application_key!=incoming_table.application_key {return Err(Error::SchemaMismatch);}
                for column in &incoming_table.columns {
                    if let Some(existing)=local.columns.iter_mut().find(|c|c.id==column.id){
                        let mut expected=column.clone();expected.fts=existing.fts;expected.vector=existing.vector.clone();
                        if *existing!=expected{return Err(Error::SchemaMismatch);}
                        existing.fts=column.fts;existing.vector=column.vector.clone();
                    }
                    else {if column.primary_key>0 || column.not_null || local.columns.iter().any(|c|c.name==column.name){return Err(Error::SchemaMismatch);}let definitions=crate::primary_key::column_definitions(&incoming_table.sql)?;let definition=definitions.get(&column.name).ok_or(Error::SchemaMismatch)?;ddl.push_str(&format!("ALTER TABLE {} ADD COLUMN {};",quote(&local.name),definition));local.columns.push(column.clone());}
                }
            }else {if target.tables.iter().any(|t|t.name==incoming_table.name){return Err(Error::SchemaMismatch);}ddl.push_str(&table_definition(incoming_table)?);ddl.push(';');target.tables.push(incoming_table.clone());target.table_replication.insert(incoming_table.id.clone(),true);}
        }
        for index in &incoming.indexes {
            if !target.indexes.contains(index) {
                if let Some(table)=crate::schema::index_table(index) {
                    match incoming.tables.iter().find(|t|t.name.eq_ignore_ascii_case(&table)) {
                        Some(t) if self.schema.tables.iter().any(|l|l.id==t.id)
                            && !crate::table_replication::enabled(&self.schema,&t.id) => continue,
                        Some(_) => {}
                        // Honest senders never emit indexes outside their payload.
                        None => return Err(Error::SchemaMismatch),
                    }
                }
                let words=crate::schema::keywords(index);
                if words.first().map(String::as_str)!=Some("CREATE") || words.get(1).map(String::as_str)!=Some("INDEX") || words.iter().any(|s|["SELECT","INSERT","UPDATE","DELETE","DROP","TRIGGER","VIEW","PRAGMA","ATTACH","DETACH","UNIQUE"].contains(&s.as_str())) {return Err(Error::SchemaMismatch);}
                ddl.push_str(index);ddl.push(';');target.indexes.push(index.clone());
            }
        }
        if ddl.is_empty() && target == self.schema {return Ok(true);}
        let definitions = crate::search::schema_definitions(&[], &target)?;
        crate::search::Manager::register(&self.connection, &definitions)?;
        if target.tables.iter().any(|t| t.columns.iter().any(|c| c.fts || c.vector.is_some()))
            && self.connection.query_row("PRAGMA temp_store", [], |r| r.get::<_, i64>(0))? != 2 {
            self.connection.execute_batch("PRAGMA temp_store=MEMORY")?;
        }
        let metadata_only = ddl.is_empty();
        let before=if metadata_only {Vec::new()} else {schema::read_rows(&self.connection,&self.schema)?};let previous_search=self.search.clone();self.capture.take();self.connection.execute_batch("BEGIN IMMEDIATE")?;
        let result=(||->Result<(Schema,Vec<RowState>)>{self.connection.execute_batch(&ddl)?;let migration=Migration::new(ddl.clone());let mut actual=schema::inspect(&self.connection,&target,&migration)?;
            for table in &mut actual.tables {let intended=target.tables.iter().find(|t|t.name==table.name).ok_or(Error::SchemaMismatch)?;table.id=intended.id.clone();table.application_key=intended.application_key.clone();for column in &mut table.columns {let definition=intended.columns.iter().find(|c|c.name==column.name).ok_or(Error::SchemaMismatch)?;column.id=definition.id.clone();column.policy=definition.policy;column.fts=definition.fts;column.vector=definition.vector.clone();}}
            if let Some(path)=&self.table_config_path {crate::config::save_table_flags(path,&actual.table_replication)?;}
            actual.history=canonical_schema(&actual)?.history;self.search.sync_schema(&self.connection,&actual,true)?;self.search.refresh(&self.connection,&actual)?;let after=if metadata_only {Vec::new()} else {schema::read_rows(&self.connection,&actual)?};self.connection.execute_batch("COMMIT")?;Ok((actual,after))})();
        let (actual,after)=match result {Ok(v)=>v,Err(e)=>{let _=self.connection.execute_batch("ROLLBACK");self.search=previous_search;self.search.publish_guard();self.capture=Some(schema::capture(&self.connection,&self.schema)?);return Err(e);}};
        if let Some(config)=&self.table_config {config.write().unwrap().replication.tables=actual.table_replication.clone();}
        self.schema=actual.clone();crate::database::configure_authorizer(&self.connection,&self.schema,self.access.clone(),self.search.guard.clone())?;self.status.lock().unwrap().search_indexes=self.search.status();let bytes=encode(&before)?.len()+encode(&after)?.len()+ddl.len();let cursor=self.bump()?;self.capture=Some(schema::capture(&self.connection,&actual)?);
        self.send(Event::SchemaCaptured{migration:ddl,schema:actual.clone(),before,after,version:Version::default()},cursor,bytes)?;self.captured_cursor=cursor;
        Ok(true)
    }
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    #[cfg(not(feature = "vector"))]
    #[test]
    fn feature_disabled_adoption_preserves_the_existing_schema() -> Result<()> {
        let directory=tempfile::tempdir()?;
        let a=Database::open(directory.path().join("source"),[9;32],Options{mode:OpenMode::CreateNew,..Options::default()})?;
        let b=Database::open(directory.path().join("target"),[9;32],Options{mode:OpenMode::CreateNew,..Options::default()})?;
        a.migrate(Migration::new("CREATE TABLE docs(id INTEGER PRIMARY KEY,embedding BLOB)"))?;
        a.set_replication("docs",true)?;
        let mut incoming=a.schema()?;
        incoming.tables[0].columns.iter_mut().find(|c|c.name=="embedding").unwrap().vector=Some(crate::VectorColumnConfig{dimensions:2,distance:crate::DistanceMetric::L2});
        let before=b.schema()?;
        let error=b.call(move|c|c.adopt_schema(incoming)).unwrap_err();
        assert!(error.to_string().contains("vector Cargo feature"),"{error}");
        assert_eq!(b.schema()?,before);
        a.close()?;b.close()
    }

    fn prepared(database:&Database)->Result<(merge::Patch,std::collections::BTreeSet<(String,Vec<u8>)>)> {
        let baseline=database.export_state()?;let cursor=database.status().committed;
        let mut incoming=baseline.clone();let row=incoming.rows.iter_mut().find(|row|row.cells.iter().any(|cell|incoming.schema.tables[0].columns.iter().any(|c|c.id==cell.column&&c.name=="id")&&cell.value==Value::Integer(1))).unwrap();
        let column=incoming.schema.tables[0].columns.iter().find(|column|column.name=="n").unwrap().id.clone();
        let version=incoming.clock.next([7;16])?;let cell=row.cells.iter_mut().find(|cell|cell.column==column).unwrap();cell.value=Value::Integer(42);cell.version=version;incoming.clock=version;
        let keys=std::collections::BTreeSet::from([(row.table.clone(),encode(&row.key)?)]);
        Ok((merge::prepare_delta(&baseline,incoming,cursor)?,keys))
    }
    fn opened(path:&std::path::Path)->Result<Database> {
        let database=Database::open(path,[9;32],Options{mode:OpenMode::CreateNew,capture_interval:std::time::Duration::from_secs(3600),..Options::default()})?;
        database.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER NOT NULL)"))?;
        database.set_replication("items",true)?;
        database.transaction(|sql|{sql.execute("INSERT INTO items VALUES(1,0),(2,0)",[])?;Ok(())})?;database.flush()?;Ok(database)
    }
    #[test]
    fn canonical_managed_schema_is_idempotent() -> Result<()> {
        let temp=tempfile::tempdir()?;let database=opened(&temp.path().join("db"))?;
        database.migrate(Migration::new("ALTER TABLE items ADD COLUMN note TEXT DEFAULT 'kept'"))?;
        let first=canonical_schema(&database.schema()?)?;
        assert_eq!(canonical_schema(&first)?,first);
        database.close()
    }
    #[test]
    fn adopt_additive_managed_schema_preserves_ids_and_values() -> Result<()> {
        let temp=tempfile::tempdir()?;let a=opened(&temp.path().join("a"))?;let b=opened(&temp.path().join("b"))?;
        a.migrate(Migration::new("ALTER TABLE items ADD COLUMN m INTEGER DEFAULT 5"))?;a.flush()?;
        let incoming=canonical_schema(&crate::table_replication::filter_schema(&a.schema()?))?;
        assert!(b.call(move |c|c.adopt_schema(incoming))?);
        assert_eq!(b.read(|s|Ok(s.query_row("SELECT m FROM items WHERE id=1",[],|r|r.get::<_,i64>(0))?))?,5);
        a.close()?;b.close()
    }
    fn benchmark_cpu_ns()->Option<u128> {
        #[cfg(all(target_os="linux",target_pointer_width="64"))]
        {#[repr(C)]struct Ts{seconds:std::ffi::c_long,nanos:std::ffi::c_long}
        unsafe extern "C"{fn clock_gettime(clock:std::ffi::c_int,time:*mut Ts)->std::ffi::c_int;}
        let mut t=Ts{seconds:0,nanos:0};if unsafe{clock_gettime(2,&mut t)}==0{return Some(t.seconds as u128*1_000_000_000+t.nanos as u128);}}
        None
    }
    #[test]
    #[ignore="release foreground-call index scaling benchmark, not cluster acceptance"]
    fn foreground_call_index_scaling_benchmark()->Result<()> {
        let directory=tempfile::tempdir()?;let database=opened(&directory.path().join("db"))?;
        let adapter=Adapter{merge_deferrals:std::sync::atomic::AtomicUsize::new(0),cancelled:Arc::new(std::sync::atomic::AtomicBool::new(false)),inner:Arc::downgrade(&database.inner),worker:database.inner.worker.clone(),identity:(database.database_id(),database.node_id()),last_reclaim_ms:std::sync::atomic::AtomicU64::new(0),last_apply_barrier_ms:std::sync::atomic::AtomicU64::new(0),progress_cache:std::sync::Mutex::new(None)};
        let mut previous=0;
        for count in [1000,10_000,100_000] {
            let records=(previous..count).map(|i|Record::put(format!("07/replication/bench/{i:08}").into_bytes(),vec![9;8])).collect();
            adapter.commit(records).map_err(|e|Error::Storage(e.to_string()))?;previous=count;
            for _ in 0..100 {database.read(|sql|Ok(sql.query_row("SELECT 1",[],|row|row.get::<_,i64>(0))?))?;}
            let cpu_start=benchmark_cpu_ns();let start=std::time::Instant::now();
            for _ in 0..1000 {assert_eq!(database.read(|sql|Ok(sql.query_row("SELECT 1",[],|row|row.get::<_,i64>(0))?))?,1);}
            let wall_ns=start.elapsed().as_nanos();let cpu_ns=cpu_start.zip(benchmark_cpu_ns()).map(|(a,b)|b-a);
            eprintln!("FOREGROUND_INDEX keys={count} calls=1000 wall_ns={wall_ns} cpu_ns={cpu_ns:?}");
        }
        database.close()
    }
    #[test]
    #[ignore="release encrypted Spool worker benchmark, not cluster acceptance"]
    fn batched_spool_reads_benchmark()->Result<()> {
        let directory=tempfile::tempdir()?;let database=opened(&directory.path().join("db"))?;
        let adapter=Adapter{merge_deferrals:std::sync::atomic::AtomicUsize::new(0),cancelled:Arc::new(std::sync::atomic::AtomicBool::new(false)),inner:Arc::downgrade(&database.inner),worker:database.inner.worker.clone(),identity:(database.database_id(),database.node_id()),last_reclaim_ms:std::sync::atomic::AtomicU64::new(0),last_apply_barrier_ms:std::sync::atomic::AtomicU64::new(0),progress_cache:std::sync::Mutex::new(None)};
        let keys=(0..64).map(|i|format!("07/replication/bench/{i:04}").into_bytes()).collect::<Vec<_>>();
        adapter.commit(keys.iter().map(|key|Record::put(key.clone(),vec![9;256])).collect()).map_err(|e|Error::Storage(e.to_string()))?;
        let expected=adapter.get_many(&keys).map_err(|e|Error::Storage(e.to_string()))?;
        let rounds=500;let cpu_start=benchmark_cpu_ns();let start=std::time::Instant::now();
        for _ in 0..rounds {let values=keys.iter().map(|key|adapter.get(key).unwrap()).collect::<Vec<_>>();assert_eq!(values,expected);}
        let before=start.elapsed();let before_cpu=cpu_start.zip(benchmark_cpu_ns()).map(|(a,b)|b-a);let cpu_start=benchmark_cpu_ns();let start=std::time::Instant::now();
        for _ in 0..rounds {assert_eq!(adapter.get_many(&keys).unwrap(),expected);}
        let after=start.elapsed();let after_cpu=cpu_start.zip(benchmark_cpu_ns()).map(|(a,b)|b-a);eprintln!("batched_spool rounds={rounds} keys=64 value_bytes=256 before_ms={:.3} after_ms={:.3} before_cpu_ns={before_cpu:?} after_cpu_ns={after_cpu:?} before_worker_requests={} after_worker_requests={rounds} speedup={:.3}",before.as_secs_f64()*1000.,after.as_secs_f64()*1000.,rounds*keys.len(),before.as_secs_f64()/after.as_secs_f64());database.close()
    }
    #[test]
    fn point_publication_preserves_disjoint_pending_sql_and_reopens() -> Result<()> {
        let directory=tempfile::tempdir()?;let path=directory.path().join("db");let database=opened(&path)?;
        let (mut patch,keys)=prepared(&database)?;
        database.transaction(|sql|{sql.execute("UPDATE items SET n=20 WHERE id=2",[])?;Ok(())})?;
        assert!(database.call(move|core|{if !core.advance_disjoint_patch(&mut patch,&keys)?{return Ok(false);}core.publish_network(patch,Vec::new(),false)})?);
        database.flush()?;let state=database.export_state()?;database.close()?;
        let recovered=Database::open(&path,[9;32],Options::default())?;
        assert_eq!(recovered.read(|sql|Ok(sql.query_row("SELECT n FROM items WHERE id=1",[],|row|row.get::<_,i64>(0))?))?,42);
        assert_eq!(recovered.read(|sql|Ok(sql.query_row("SELECT n FROM items WHERE id=2",[],|row|row.get::<_,i64>(0))?))?,20);
        assert_eq!(recovered.export_state()?,state);recovered.close()
    }
    #[test]
    fn point_publication_retries_overlapping_or_already_captured_sql() -> Result<()> {
        let directory=tempfile::tempdir()?;let database=opened(&directory.path().join("db"))?;
        let (mut patch,keys)=prepared(&database)?;
        database.transaction(|sql|{sql.execute("UPDATE items SET n=99 WHERE id=1",[])?;Ok(())})?;
        assert!(!database.call(move|core|core.advance_disjoint_patch(&mut patch,&keys))?);
        assert_eq!(database.read(|sql|Ok(sql.query_row("SELECT n FROM items WHERE id=1",[],|row|row.get::<_,i64>(0))?))?,99);
        let (mut patch,keys)=prepared(&database)?;
        database.transaction(|sql|{sql.execute("DELETE FROM items WHERE id=1",[])?;Ok(())})?;
        assert!(!database.call(move|core|core.advance_disjoint_patch(&mut patch,&keys))?);
        let (mut patch,keys)={database.transaction(|sql|{sql.execute("INSERT INTO items VALUES(1,0)",[])?;Ok(())})?;prepared(&database)?};
        database.transaction(|sql|{sql.execute("UPDATE items SET n=30 WHERE id=2",[])?;Ok(())})?;database.flush()?;
        assert!(!database.call(move|core|core.advance_disjoint_patch(&mut patch,&keys))?);database.close()
    }
}

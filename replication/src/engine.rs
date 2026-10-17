use crate::{*, protocol::*, membership::{Member,Packet,State,View}};
use sha2::{Digest,Sha256};
use std::sync::{Mutex,atomic::{AtomicBool,Ordering}};
use tokio::sync::{mpsc,Semaphore};
// Disposable per-push working set; Spool remains authoritative. Separate byte
// limits cover decoded transactions and prepared frames, with bounded key counts.
const OUTGOING_CACHE_BYTES:usize=4<<20;
// Disposable admission accounting for durable staged keys. Spool owns their
// contents; mutations update the count under the existing stage/cache locks.
#[derive(Default)]
struct StagedSizes {sizes:BTreeMap<Vec<u8>,usize>,bytes:usize}
impl StagedSizes {
    fn insert(&mut self,key:Vec<u8>,size:usize)->Result<()> {
        let old=self.sizes.get(&key).copied().unwrap_or(0);
        let next=self.bytes.checked_sub(old).and_then(|bytes|bytes.checked_add(size)).ok_or(Error::Overloaded)?;
        self.sizes.insert(key,size);self.bytes=next;Ok(())
    }
    fn remove(&mut self,key:&[u8]) {
        if let Some(size)=self.sizes.remove(key){self.bytes-=size;}
    }
}
struct Outgoing {data:Arc<Vec<u8>>,tx:Arc<Transaction>}
/// Shared retained-log cache across push rounds. Log records are immutable,
/// so entries are disposable (Spool authoritative) and need only a TTL
/// against retention trimming plus a byte bound. FIFO eviction suits the
/// sliding unacked window (oldest entries are acked past and never reused).
/// Without this every push tick re-reads the whole window from Spool
/// (~400 reads/s), queueing the worker behind itself.
const OUTGOING_SHARED_BYTES:usize=8<<20;
const OUTGOING_SHARED_TTL:std::time::Duration=std::time::Duration::from_secs(10);
#[derive(Default)]
struct OutgoingShared {values:BTreeMap<(NodeId,u64),(Arc<Outgoing>,std::time::Instant,u64)>,order:std::collections::VecDeque<(NodeId,u64)>,bytes:usize}
impl OutgoingShared {
    fn get(&mut self,key:&(NodeId,u64),now:std::time::Instant)->Option<Arc<Outgoing>> {
        let fresh=self.values.get(key).map(|(value,at,_)|(value.clone(),now.saturating_duration_since(*at)<OUTGOING_SHARED_TTL));
        match fresh {
            Some((value,true))=>Some(value),
            Some((_,false))=>{if let Some((_,_,c))=self.values.remove(key){self.bytes=self.bytes.saturating_sub(c as usize);}None}
            None=>None,
        }
    }
    fn evict_one(&mut self)->bool {
        let Some(old)=self.order.pop_front() else{return false;};
        if let Some((_,_,c))=self.values.remove(&old){self.bytes=self.bytes.saturating_sub(c as usize);}
        true
    }
    fn insert(&mut self,key:(NodeId,u64),value:Arc<Outgoing>,charge:u64,now:std::time::Instant) {
        if self.values.contains_key(&key){return;}
        if charge as usize>OUTGOING_SHARED_BYTES{return;}
        while self.bytes.saturating_add(charge as usize)>OUTGOING_SHARED_BYTES {
            if !self.evict_one(){break;}
        }
        self.order.push_back(key);self.bytes=self.bytes.saturating_add(charge as usize);self.values.insert(key,(value,now,charge));
        while self.order.len()>16384 {if !self.evict_one(){break;}}
    }
}
#[derive(Default)]
struct OutgoingRound {frames:BTreeMap<Vec<(NodeId,u64)>,Arc<Prepared>>,frame_bytes:usize}
fn read_many(backend:&dyn Backend,keys:&[Vec<u8>])->Result<Vec<Option<Vec<u8>>>> {
    if keys.is_empty(){return Ok(Vec::new());}
    let values=backend.get_many(keys)?;
    if values.len()!=keys.len(){return Err(Error::Storage("get_many returned the wrong number of values".into()));}Ok(values)
}
fn read_bounded(backend:&dyn Backend,keys:&[Vec<u8>],max_bytes:usize)->Result<Vec<Option<Vec<u8>>>> {
    if keys.is_empty(){return Ok(Vec::new());}
    let values=backend.get_many_bounded(keys,max_bytes)?;
    if values.is_empty()||values.len()>keys.len(){return Err(Error::Storage("bounded read returned invalid prefix length".into()));}
    if values.len()>1&&values.iter().flatten().fold(0usize,|n,v|n.saturating_add(v.len()))>max_bytes{return Err(Error::Storage("bounded read exceeded byte budget".into()));}
    Ok(values)
}
impl Core {
    fn load_outgoing(&self,origin:NodeId,seqs:&[u64])->Result<Vec<Option<Arc<Outgoing>>>> {
        let now=std::time::Instant::now();
        let mut out:Vec<Option<Arc<Outgoing>>>=Vec::with_capacity(seqs.len());
        let mut missing_idx=Vec::new();
        {
            let mut shared=self.outgoing_cache.lock().unwrap();
            for (i,seq) in seqs.iter().enumerate() {
                match shared.get(&(origin,*seq),now) {
                    Some(value)=>out.push(Some(value)),
                    None=>{out.push(None);missing_idx.push(i);}
                }
            }
        }
        if !missing_idx.is_empty(){
            let keys=missing_idx.iter().map(|i|log_key(origin,seqs[*i])).collect::<Vec<_>>();
            let values=read_bounded(self.backend.as_ref(),&keys,1<<20)?;
            let mut shared=self.outgoing_cache.lock().unwrap();
            for (i,data) in missing_idx.into_iter().zip(values) {
                let seq=seqs[i];
                if let Some(data)=data {
                    let tx:Transaction=decode(&data)?;
                    if tx.origin!=origin||tx.sequence!=seq{return Err(Error::Protocol("retained transaction key mismatch".into()));}
                    let charge=(data.capacity().saturating_add(tx.payload.capacity()).saturating_add(tx.signature.capacity()).saturating_add(512)) as u64;
                    let value=Arc::new(Outgoing{data:Arc::new(data),tx:Arc::new(tx)});
                    shared.insert((origin,seq),value.clone(),charge,now);
                    out[i]=Some(value);
                }
            }
        }
        Ok(out)
    }
}
impl OutgoingRound {
    fn frame(&mut self,keys:Vec<(NodeId,u64)>,message:&Message)->Result<Arc<Prepared>> {
        if let Some(frame)=self.frames.get(&keys){return Ok(frame.clone());}
        let frame=Arc::new(Prepared::new(message)?);
        let charge=frame.bytes.capacity().saturating_add(keys.capacity()*std::mem::size_of::<(NodeId,u64)>()).saturating_add(256);
        if self.frames.len()<1024 && charge<=OUTGOING_CACHE_BYTES.saturating_sub(self.frame_bytes){self.frame_bytes+=charge;self.frames.insert(keys,frame.clone());}
        Ok(frame)
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplicationMetrics {
    pub pushed_transactions: u64,
    pub received_transactions: u64,
    pub applied_transactions: u64,
    pub staged_transactions: usize,
    pub snapshots_applied: u64,
    pub gossip_rounds: u64,
    pub probes_sent: u64,
    pub probes_failed: u64,
    pub anti_entropy_runs: u64,
    pub schema_pushes: u64,
    pub schema_adoptions: u64,
    pub retention_runs: u64,
    pub retention_pruned: u64,
    pub queue_available_bytes: usize,
    pub chunk_need_sent: u64,
    pub chunk_need_served: u64,
    pub apply_batches: u64,
    pub apply_bytes: u64,
    pub apply_nanos: u64,
}

/// In-memory chunk assembly progress. The manifest plus chunk keys in the
/// backend are the durable truth; this only paces repair requests.
struct ChunkProgress {
    received: u32,
    updated: std::time::Instant,
    repaired: std::time::Instant,
}
// Transfers with a live chunk-serve task. Manifest retransmits make the
// receiver re-Need aggressively; without dedupe every Need spawns a task
// that re-sends the whole transfer, clogging the data lane with
// duplicate chunks. Transfer ids are globally unique, so no donor key.
static SNAPSHOT_SERVING: std::sync::OnceLock<std::sync::Mutex<BTreeSet<NodeId>>> =
    std::sync::OnceLock::new();
struct ServeGuard(NodeId);
impl Drop for ServeGuard {
    fn drop(&mut self) {
        if let Some(m) = SNAPSHOT_SERVING.get() {
            m.lock().unwrap().remove(&self.0);
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerDiagnostics {
    pub node_id: NodeId,
    pub address: Option<SocketAddr>,
    pub connected: bool,
    pub last_seen_millis_ago: Option<u64>,
    pub sent_watermark: u64,
    pub schema_agreed: bool,
    pub push_mode: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Status {
    pub running: bool,
    pub address: Option<SocketAddr>,
    pub peers: usize,
    pub members: usize,
    pub staged_transactions: usize,
    pub applied_transactions: u64,
    pub last_error: Option<String>,
    pub deferred: bool,
    pub file_endpoint: Option<SocketAddr>,
    pub metrics: ReplicationMetrics,
    pub peers_diagnostics: Vec<PeerDiagnostics>,
}

struct Control {backend:Arc<dyn Backend>,stop:AtomicBool,status:Mutex<Status>,commands:mpsc::Sender<Command>,thread:Mutex<Option<std::thread::JoinHandle<()>>>,shutdown:tokio::sync::watch::Sender<bool>,objects:Option<Arc<files::Objects>>,bridge:Option<Arc<bridge::Bridge>>}
#[derive(Clone)]
pub struct Handle {control:Arc<Control>}
impl std::fmt::Debug for Handle {fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result {f.debug_struct("Replication").field("status",&self.status()).finish()}}
enum Command {Add(SocketAddr),Remove(NodeId),Retire(NodeId,std::sync::mpsc::Sender<Result<()>>),RetentionStatus(std::sync::mpsc::Sender<Result<retention::RetentionStatus>>),Collect(std::sync::mpsc::Sender<Result<u64>>),Sync,Stop}
impl Handle {
    pub(crate) fn start(config:Config,backend:Arc<dyn Backend>)->Result<Self> {
        config.validate()?;bind_signer(backend.as_ref(),&config)?;retention::migrate_peers(backend.as_ref())?;
        let objects=config.files.clone().map(|c|files::Objects::open(c,backend.clone()).map(Arc::new)).transpose()?;let bridge=config.bridge.clone().map(|c|bridge::Bridge::open(c,backend.clone()).map(Arc::new)).transpose()?;if let Some(bridge)=&bridge{bridge.attach_files(objects.clone());}
        let (commands,receiver)=mpsc::channel(128);let (shutdown,_)=tokio::sync::watch::channel(false);let control=Arc::new(Control{backend:backend.clone(),stop:AtomicBool::new(false),status:Mutex::new(Status::default()),commands,thread:Mutex::new(None),shutdown,objects,bridge});
        let (ready_tx,ready_rx)=std::sync::mpsc::channel();let state=control.clone();
        let thread=std::thread::Builder::new().name("murmur-replication".into()).spawn(move|| {
            let runtime=tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build();
            match runtime {Err(e)=>{let _=ready_tx.send(Err(Error::Io(e)));},Ok(rt)=>rt.block_on(async {
                let endpoint=match transport::endpoint(&config,backend.identity().1) {Ok(e)=>e,Err(e)=>{let _=ready_tx.send(Err(e));return;}};
                let address=endpoint.local_addr().unwrap();state.status.lock().unwrap().running=true;state.status.lock().unwrap().address=Some(address);
                let files=match state.objects.clone().map(|objects|crate::filefetch::Service::start(&config,backend.clone(),objects)).transpose() {Ok(f)=>f,Err(e)=>{let _=ready_tx.send(Err(e));return;}};
                state.status.lock().unwrap().file_endpoint=files.as_ref().and_then(|f|f.endpoint.local_addr().ok());
                let queue=Arc::new(Semaphore::new(config.queue_bytes-(1<<20)));
                let _=ready_tx.send(Ok(()));
                let core=Arc::new(Core{config,backend,endpoint,files,control:state.clone(),peers:tokio::sync::Mutex::new(BTreeMap::new()),view:tokio::sync::Mutex::new(View::new(Member{node:[0;16],address,incarnation:0,state:State::Alive})),sessions:Arc::new(Semaphore::new(1)),relays:tokio::sync::Mutex::new(BTreeMap::new()),stage_gate:tokio::sync::Mutex::new(()),stage_commit:tokio::sync::Mutex::new(()),retained_gate:tokio::sync::Mutex::new(()),retention_gate:tokio::sync::Mutex::new(()),queue,control_queue:Arc::new(Semaphore::new(1<<20)),tasks:Mutex::new(Vec::new()),apply_notify:tokio::sync::Notify::new(),staged_sizes:tokio::sync::Mutex::new(None),last_origin:Mutex::new([0;16]),incoming_hint:Mutex::new(None),incoming_none_at:Mutex::new(None),missing_cache:Mutex::new(None),adaptive_batch:std::sync::atomic::AtomicUsize::new(usize::MAX),verification:Mutex::new(crate::verification::Cache::default()),transfers:Arc::new(Semaphore::new(2)),schema_req_at:tokio::sync::Mutex::new(None),chunk_counters:tokio::sync::Mutex::new(None),transfers_active:Arc::new(tokio::sync::Mutex::new(BTreeSet::new())),chunk_seen:tokio::sync::Mutex::new(BTreeMap::new()),chunk_holders:tokio::sync::Mutex::new(BTreeMap::new()),pending_admit:tokio::sync::Mutex::new(BTreeMap::new()),admit_removed:tokio::sync::Mutex::new(BTreeMap::new()),admit_seq:std::sync::atomic::AtomicU64::new(1),need_at:Mutex::new(BTreeMap::new()),outgoing_cache:Mutex::new(OutgoingShared::default())});
                core.view.lock().await.local.node=core.backend.identity().1;
                core.view.lock().await.local.address=core.config.advertise.unwrap_or(address);let incarnation_key=format!("07/replication/incarnation/{}",hex(&core.backend.identity().1)).into_bytes();
                let incarnation=core.backend.get(&incarnation_key).ok().flatten().and_then(|b|decode::<u64>(&b).ok()).unwrap_or(0).saturating_add(1);
                core.view.lock().await.local.incarnation=incarnation;let _=core.backend.commit(vec![Record::put(incarnation_key,encode(&incarnation).unwrap())]);
                // The shared global session budget covers accepted and dialed sessions.
                core.sessions.add_permits(core.config.sessions.saturating_sub(1));
                for key in core.backend.keys(b"07/replication/excluded/").unwrap_or_default(){if let Ok(Some(b))=core.backend.get(&key){if let Ok(id)=decode::<NodeId>(&b){core.view.lock().await.excluded.insert(id);}}}
                let mut shutdown=state.shutdown.subscribe();
                tokio::select! { _=core.run(receiver)=>{}, _=shutdown.changed()=>{} }
                core.queue.close();core.control_queue.close();core.sessions.close();core.transfers.close();
                for peer in core.peers.lock().await.values(){peer.connection.close(0u32.into(),b"shutdown");}
                let tasks=std::mem::take(&mut *core.tasks.lock().unwrap());
                for task in &tasks {task.abort();}for task in tasks {let _=task.await;}
                if let Some(files)=&core.files {files.endpoint.close(0u32.into(),b"shutdown");}core.endpoint.close(0u32.into(),b"shutdown");let _=tokio::time::timeout(Duration::from_secs(2),core.endpoint.wait_idle()).await;
                state.status.lock().unwrap().running=false;
            })}
        })?;
        *control.thread.lock().unwrap()=Some(thread);
        if let Err(e)=ready_rx.recv().map_err(|_|Error::Unavailable("replication startup stopped".into()))? {if let Some(t)=control.thread.lock().unwrap().take(){let _=t.join();}return Err(e);}
        Ok(Self{control})
    }
    pub fn files(&self)->Option<Arc<files::Objects>> {self.control.objects.clone()}
    pub fn bridge(&self)->Option<Arc<bridge::Bridge>> {self.control.bridge.clone()}
    pub fn status(&self)->Status {self.control.status.lock().unwrap().clone()}
    pub fn add_peer(&self,address:SocketAddr)->Result<()> {self.control.commands.try_send(Command::Add(address)).map_err(|_|Error::Overloaded)}
    pub fn remove_peer(&self,node:NodeId)->Result<()> {self.control.commands.try_send(Command::Remove(node)).map_err(|_|Error::Overloaded)}
    /// Permanently exclude a peer and release its history retention obligation.
    pub fn retire_peer(&self,node:NodeId)->Result<()> {
        let(tx,rx)=std::sync::mpsc::channel();self.control.commands.try_send(Command::Retire(node,tx)).map_err(|_|Error::Overloaded)?;
        rx.recv().map_err(|_|Error::Unavailable("replication stopped".into()))?
    }
    pub fn retention_status(&self)->Result<retention::RetentionStatus> {
        let(tx,rx)=std::sync::mpsc::channel();self.control.commands.try_send(Command::RetentionStatus(tx)).map_err(|_|Error::Overloaded)?;
        rx.recv().map_err(|_|Error::Unavailable("replication stopped".into()))?
    }
    /// Run a bounded collection pass now using the configured retention rules.
    pub fn collect_retention(&self)->Result<u64> {
        let(tx,rx)=std::sync::mpsc::channel();self.control.commands.try_send(Command::Collect(tx)).map_err(|_|Error::Overloaded)?;
        rx.recv().map_err(|_|Error::Unavailable("replication stopped".into()))?
    }
    pub fn force_sync(&self)->Result<()> {self.control.commands.try_send(Command::Sync).map_err(|_|Error::Overloaded)}
    pub fn close(&self)->Result<()> {self.control.stop.store(true,Ordering::Release);self.control.shutdown.send_replace(true);self.control.backend.cancel_pending();let _=self.control.commands.try_send(Command::Stop);if let Some(thread)=self.control.thread.lock().unwrap().take(){thread.join().map_err(|_|Error::Unavailable("replication thread panicked".into()))?;}Ok(())}
}
struct Peer {connection:quinn::Connection,fetch:Option<SocketAddr>,send:crate::queue::Sender,progress:BTreeMap<NodeId,u64>,last_seen:std::time::Instant,push:bool,lazy:bool,outgoing:bool,sent:BTreeMap<NodeId,u64>,sent_at:BTreeMap<NodeId,std::time::Instant>,resent_at:BTreeMap<NodeId,std::time::Instant>,resent:BTreeMap<NodeId,u64>,ack_any_at:Option<std::time::Instant>,bulk_lag_since:Option<std::time::Instant>,push_origin:Option<NodeId>,schema_acked:Option<([u8;32],u64)>,schema_push_at:std::time::Instant,schema_sent:Option<([u8;32],u64)>,last_touch:Option<std::time::Instant>,snapshot_req_at:Option<std::time::Instant>}
struct Core {config:Config,backend:Arc<dyn Backend>,endpoint:quinn::Endpoint,files:Option<crate::filefetch::Service>,control:Arc<Control>,peers:tokio::sync::Mutex<BTreeMap<NodeId,Peer>>,view:tokio::sync::Mutex<View>,sessions:Arc<Semaphore>,relays:tokio::sync::Mutex<BTreeMap<(NodeId,u64),(NodeId,std::time::Instant)>>,stage_gate:tokio::sync::Mutex<()>,stage_commit:tokio::sync::Mutex<()>,retained_gate:tokio::sync::Mutex<()>,retention_gate:tokio::sync::Mutex<()>,queue:Arc<Semaphore>,control_queue:Arc<Semaphore>,tasks:Mutex<Vec<tokio::task::JoinHandle<()>>>,apply_notify:tokio::sync::Notify,staged_sizes:tokio::sync::Mutex<Option<StagedSizes>>,last_origin:Mutex<NodeId>,incoming_hint:Mutex<Option<NodeId>>,incoming_none_at:Mutex<Option<std::time::Instant>>,missing_cache:Mutex<Option<(NodeId,Vec<u32>)>>,adaptive_batch:std::sync::atomic::AtomicUsize,verification:Mutex<crate::verification::Cache>,transfers:Arc<Semaphore>,schema_req_at:tokio::sync::Mutex<Option<std::time::Instant>>,chunk_counters:tokio::sync::Mutex<Option<(usize,usize)>>,transfers_active:Arc<tokio::sync::Mutex<BTreeSet<(usize,NodeId,u64)>>>,chunk_seen:tokio::sync::Mutex<BTreeMap<(NodeId,u64),ChunkProgress>>,chunk_holders:tokio::sync::Mutex<BTreeMap<(NodeId,u64),(BTreeMap<NodeId,BTreeSet<u32>>,std::time::Instant)>>,pending_admit:tokio::sync::Mutex<BTreeMap<SocketAddr,u64>>,admit_removed:tokio::sync::Mutex<BTreeMap<NodeId,u64>>,admit_seq:std::sync::atomic::AtomicU64,need_at:Mutex<BTreeMap<(NodeId,u64),std::time::Instant>>,outgoing_cache:Mutex<OutgoingShared>}
impl Core {
    fn spawn(&self, future:impl std::future::Future<Output=()>+Send+'static) {
        let mut shutdown=self.control.shutdown.subscribe();
        let task=tokio::spawn(async move {if *shutdown.borrow(){return;}tokio::select! {_=future=>{},_=shutdown.changed()=>{}}});
        let mut tasks=self.tasks.lock().unwrap();tasks.retain(|task|!task.is_finished());tasks.push(task);
    }
    async fn apply_loop(self:Arc<Self>) {
        let mut retry=tokio::time::interval(self.config.incoming_interval.min(Duration::from_secs(1)));
        loop {
            tokio::select! {_=self.apply_notify.notified()=>{},_=retry.tick()=>{}}
            loop {match self.apply_pending().await {Ok(true)=>tokio::task::yield_now().await,Ok(false)=>break,Err(e)=>{self.error(&e);break;}}}
        }
    }

    fn error(&self,e:&Error){self.control.status.lock().unwrap().last_error=Some(e.to_string());}
    async fn run(self:&Arc<Self>,mut commands:mpsc::Receiver<Command>) {
        self.spawn(self.clone().apply_loop());let mut apply=tokio::time::interval(Duration::from_secs(1));let mut repair=tokio::time::interval(self.config.anti_entropy_interval);let mut push=tokio::time::interval(Duration::from_millis(100));let mut probes=tokio::time::interval(Duration::from_secs(1));let mut gossip=tokio::time::interval(Duration::from_millis(200));let mut rotation=tokio::time::interval(Duration::from_secs(5));let mut bootstrap=tokio::time::interval(Duration::from_secs(5));let mut rotation_count=0u64;
        // Skip, never burst: when the box is slow these arms overrun their
        // period, and burst catch-up would run them back-to-back, doubling
        // load exactly when the system is already behind.
        for tick in [&mut apply,&mut repair,&mut push,&mut probes,&mut gossip,&mut rotation,&mut bootstrap] {tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);}
        let mut retention=tokio::time::interval(self.config.retention_interval);
        retention.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut file_tick=tokio::time::interval(self.config.files.as_ref().map_or(Duration::from_secs(30),|f|f.fetch_interval));
        file_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut seeds=self.config.bootstrap.clone();
        let mut snapshot_receipt_cursor=0usize;
        loop {if self.control.stop.load(Ordering::Acquire){break;}
            tokio::select! {
                incoming=self.endpoint.accept()=>{if let Some(incoming)=incoming {if !transport::network_allowed(&self.config,incoming.remote_address()) || self.peers.lock().await.len()>=self.config.connections || self.sessions.available_permits()<=self.config.fanout+self.config.repairs {incoming.refuse();continue;}let this=self.clone();self.spawn(async move {match incoming.await {Ok(c)=>{let backup=c.clone();if let Err(e)=this.accept(c).await{this.error(&e);backup.close(1u32.into(),b"protocol rejected");}},Err(e)=>this.error(&Error::Protocol(e.to_string()))}});}},
                command=commands.recv()=>{match command {Some(Command::Stop)|None=>break,Some(Command::Add(address))=>{let known=self.view.lock().await.members.values().find(|m|m.address==address).map(|m|m.node);if let Some(id)=known {self.view.lock().await.excluded.remove(&id);if let Err(e)=self.backend.commit(vec![Record::delete(format!("07/replication/excluded/{}",hex(&id)).into_bytes())]){self.error(&e);}}else{let seq=self.admit_seq.fetch_add(1,std::sync::atomic::Ordering::Relaxed);let mut pending=self.pending_admit.lock().await;if pending.len()>=4096{if let Some(old)=pending.iter().min_by_key(|entry|*entry.1).map(|entry|*entry.0){pending.remove(&old);}}pending.insert(address,seq);}if seeds.len()<4096 && !seeds.contains(&address){seeds.push(address);}self.dial(address).await;},Some(Command::Remove(id))=>{self.view.lock().await.excluded.insert(id);{let seq=self.admit_seq.fetch_add(1,std::sync::atomic::Ordering::Relaxed);let mut removed=self.admit_removed.lock().await;if removed.len()>=4096{if let Some(old)=removed.iter().min_by_key(|entry|*entry.1).map(|entry|*entry.0){removed.remove(&old);}}removed.insert(id,seq);}if let Err(e)=self.backend.commit(vec![Record::put(format!("07/replication/excluded/{}",hex(&id)).into_bytes(),encode(&id).unwrap())]){self.error(&e);}if let Some(p)=self.peers.lock().await.remove(&id){p.connection.close(0u32.into(),b"administratively removed");}},Some(Command::Retire(id,reply))=>{let _gate=self.retention_gate.lock().await;let result=if id==self.backend.identity().1 {Err(Error::Invalid("cannot retire this node".into()))}else{retention::retire(self.backend.as_ref(),id)};if result.is_ok(){self.view.lock().await.excluded.insert(id);{let seq=self.admit_seq.fetch_add(1,std::sync::atomic::Ordering::Relaxed);let mut removed=self.admit_removed.lock().await;if removed.len()>=4096{if let Some(old)=removed.iter().min_by_key(|entry|*entry.1).map(|entry|*entry.0){removed.remove(&old);}}removed.insert(id,seq);}if let Some(peer)=self.peers.lock().await.remove(&id){peer.connection.close(0u32.into(),b"peer retired");}}let _=reply.send(result);},Some(Command::RetentionStatus(reply))=>{let _=reply.send(retention::status(self.backend.as_ref()));},Some(Command::Collect(reply))=>{let _=reply.send(self.collect_history().await);},Some(Command::Sync)=>{self.request_progress().await;}}},
                _=bootstrap.tick()=>{let stale_after=self.config.anti_entropy_interval.checked_mul(3).unwrap_or(Duration::from_secs(30)).max(Duration::from_secs(15));self.peers.lock().await.retain(|_,peer|{let live=peer.connection.close_reason().is_none() && peer.last_seen.elapsed()<stale_after;if !live {peer.connection.close(0u32.into(),b"stale session");}live});use rand::seq::SliceRandom;seeds.shuffle(&mut rand::thread_rng());for address in &seeds {if self.peers.lock().await.values().filter(|peer|peer.outgoing).count()>=self.config.sessions {break;}self.dial(*address).await;}let mut discovered:Vec<_>=self.view.lock().await.members.values().filter(|m|m.state==State::Alive).map(|m|m.address).take(4096).collect();discovered.shuffle(&mut rand::thread_rng());for address in discovered {if self.peers.lock().await.values().filter(|peer|peer.outgoing).count()>=self.config.sessions {break;}self.dial(address).await;}},
                _=apply.tick()=>{if let Err(e)=snapshot::maintain(self.backend.as_ref(),retention::now_millis()){self.error(&e);}if let Err(e)=self.resend_snapshot_receipts(&mut snapshot_receipt_cursor).await{self.error(&e);}if let Err(e)=self.repair_chunks().await{self.error(&e);}if let Some(bridge)=&self.control.bridge {if let Err(e)=bridge.capture().and_then(|_|bridge.process()){self.error(&e);}}},
                _=file_tick.tick(), if self.files.is_some()=>{if let Some(files)=&self.files {let sources=self.peers.lock().await.iter().filter_map(|(id,p)|p.fetch.map(|addr|(*id,addr))).collect();if let Err(e)=files.fetch_missing(sources).await{self.error(&e);}if let Err(e)=files.objects.rotate_keys(4){self.error(&e);}if let Err(e)=files.objects.collect(Duration::from_secs(7*86400)){self.error(&e);}}},
                _=repair.tick()=>{self.control.status.lock().unwrap().metrics.anti_entropy_runs+=1;self.request_progress().await;self.push_schema().await;},
                _=push.tick()=>{if let Err(e)=self.push_logs().await{self.error(&e);}},
                _=gossip.tick()=>{if self.config.membership {self.control.status.lock().unwrap().metrics.gossip_rounds+=1;self.gossip().await;}},
                _=probes.tick()=>{if self.config.membership {self.control.status.lock().unwrap().metrics.probes_sent+=1;self.probe().await;}},
                _=rotation.tick()=>{rotation_count=rotation_count.wrapping_add(1);self.rotate(rotation_count%6==0).await;},
                _=retention.tick()=>{if let Err(e)=self.collect_history().await {self.error(&e);}},
            }
            // Never hold the synchronous status lock across an async wait:
            // session tasks also publish status and must remain schedulable.
            let now = std::time::Instant::now();
            let (peer_count, diagnostics) = {
                let peers = self.peers.lock().await;
                let diagnostics = peers.iter().map(|(id, p)| PeerDiagnostics {
                    node_id: *id,
                    address: Some(p.connection.remote_address()),
                    connected: p.connection.close_reason().is_none(),
                    last_seen_millis_ago: Some(now.saturating_duration_since(p.last_seen).as_millis() as u64),
                    sent_watermark: p.sent.get(id).copied().unwrap_or(0),
                    schema_agreed: p.schema_acked.is_some(),
                    push_mode: p.push,
                }).collect();
                (peers.len(), diagnostics)
            };
            let member_count = self.view.lock().await.members.len();
            {
                let mut status = self.control.status.lock().unwrap();
                status.peers = peer_count;
                status.members = member_count;
                status.metrics.applied_transactions = status.applied_transactions;
                status.metrics.staged_transactions = status.staged_transactions;
                status.metrics.queue_available_bytes = self.queue.available_permits();
                status.peers_diagnostics = diagnostics;
            }
        }
        let members=self.view.lock().await.gossip();let(db,node)=self.backend.identity();
        for peer in self.peers.lock().await.values(){let mut members=members.clone();if let Some(m)=members.iter_mut().find(|m|m.node==node){m.state=State::Left;}if let Ok(data)=encode(&Packet::Ping{database:db,sender:node,nonce:0,members}){let _=peer.connection.send_datagram(data.into());}}
    }
    async fn collect_history(&self)->Result<u64> {
        let _retention=self.retention_gate.lock().await;
        // Serialize against applies (shared retained heads), but never
        // against staging or snapshots: retention must not stall the
        // connection read loop.
        let _retained=self.retained_gate.lock().await;
        let res = retention::collect(self.backend.as_ref(),&self.config);
        if let Ok(pruned) = res {
            let mut s = self.control.status.lock().unwrap();
            s.metrics.retention_runs += 1;
            s.metrics.retention_pruned += pruned;
        }
        res
    }
    async fn dial(self:&Arc<Self>,address:SocketAddr) {
        if !transport::network_allowed(&self.config,address) || address==self.endpoint.local_addr().unwrap() || self.peers.lock().await.values().any(|p|p.connection.remote_address()==address) {return;}
        let permit=match self.sessions.clone().try_acquire_owned(){Ok(p)=>p,Err(_)=>return};let this=self.clone();
        self.spawn(async move{let _permit=permit;let result=async {
            let connection=tokio::time::timeout(Duration::from_secs(5),this.endpoint.connect(address,"murmur.invalid").map_err(|e|Error::Protocol(e.to_string()))?).await.map_err(|_|Error::Unavailable("dial timeout".into()))?.map_err(|e|Error::Protocol(e.to_string()))?;
            let id=transport::peer_id(&connection,&this.config)?;
            // An explicit AddPeer re-admits whoever answers there: the id is
            // authenticated by the TLS handshake, but known only now, and a
            // restarted peer answers from a fresh address the member table
            // cannot resolve. Clear before the Hello/Welcome exchange: the
            // far side may still reject this connection (its own exclusion
            // clears when its dial reaches us), and its close would otherwise
            // win the race against our Welcome read. The sequence check keeps
            // a newer Remove/Retire authoritative (last command wins).
            if let Some(added)=this.pending_admit.lock().await.remove(&address){if this.admit_removed.lock().await.get(&id).copied().unwrap_or(0)<added{this.view.lock().await.excluded.remove(&id);if let Err(e)=this.backend.commit(vec![Record::delete(format!("07/replication/excluded/{}",hex(&id)).into_bytes())]){this.error(&e);}}}
            let (mut send,mut recv)=connection.open_bi().await.map_err(|e|Error::Protocol(e.to_string()))?;let(db,node)=this.backend.identity();
            write(&mut send,&Message::Hello{capabilities:CAPABILITIES,required:REQUIRED,version:VERSION,database:db,node,fetch:this.files.as_ref().and_then(|f|f.endpoint.local_addr().ok()),schema:this.backend.schema()?,mode:this.config.dissemination,max_transaction:this.config.max_transaction_bytes as u64}).await?;
            let welcome=tokio::time::timeout(Duration::from_secs(5),read(&mut recv)).await.map_err(|_|Error::Unavailable("handshake timeout".into()))??;
            let fetch=match &welcome {Message::Welcome{fetch,..}=>*fetch,_=>None};
            let peer_schema=match welcome {Message::Welcome{capabilities,required,database,node,schema,mode,..} if database==db && node==id && mode==this.config.dissemination && compatible_caps(capabilities,required)=>schema,_=>return Err(Error::Auth("incompatible welcome".into()))};
            this.check_handshake_schema(&peer_schema)?;
            this.session(connection,send,recv,id,true,fetch,peer_schema).await
        }.await;if let Err(e)=result {this.error(&e);} });
    }
    async fn accept(self:&Arc<Self>,connection:quinn::Connection)->Result<()> {
        let _permit=self.sessions.clone().try_acquire_owned().map_err(|_|Error::Overloaded)?;let id=transport::peer_id(&connection,&self.config)?;
        let (mut send,mut recv)=tokio::time::timeout(Duration::from_secs(5),connection.accept_bi()).await.map_err(|_|Error::Unavailable("stream timeout".into()))?.map_err(|e|Error::Protocol(e.to_string()))?;
        let hello=tokio::time::timeout(Duration::from_secs(5),read(&mut recv)).await.map_err(|_|Error::Unavailable("hello timeout".into()))??;let(db,node)=self.backend.identity();
        let fetch=match &hello {Message::Hello{fetch,..}=>*fetch,_=>None};
        let peer_schema=match hello {Message::Hello{capabilities,required,version,database,node,schema,mode,max_transaction,..} if version==VERSION && database==db && node==id && mode==self.config.dissemination && compatible_caps(capabilities,required) && max_transaction>0=>schema,_=>return Err(Error::Auth("incompatible hello".into()))};
        self.check_handshake_schema(&peer_schema)?;
        write(&mut send,&Message::Welcome{capabilities:CAPABILITIES,required:REQUIRED,database:db,node,fetch:self.files.as_ref().and_then(|f|f.endpoint.local_addr().ok()),schema:self.backend.schema()?,mode:self.config.dissemination}).await?;
        self.session(connection,send,recv,id,false,fetch,peer_schema).await
    }
    /// Handshake schema bytes are an unsigned mismatch hint only. Adoption
    /// happens exclusively through signed manifests inside the session.
    fn check_handshake_schema(&self,peer:&[u8])->Result<()> {
        if peer==self.backend.schema()?.as_slice() { return Ok(()); }
        if !self.config.accept_remote_schema { return Err(Error::Protocol("schema mismatch".into())); }
        Ok(())
    }
    fn is_master(&self)->bool { self.config.master && self.config.schema_master==Some(self.backend.identity().1) }
    /// Current outbound manifest. The sequence bumps only when our canonical
    /// schema bytes change, so retransmits stay idempotent for receivers.
    fn manifest(&self)->Result<SignedSchema> {
        let bytes=self.backend.schema()?;
        let digest:[u8;32]=Sha256::digest(&bytes).into();
        let key=schema_manifest_key();
        let stored:Option<([u8;32],u64)>=self.backend.get(&key)?.map(|b|decode(&b)).transpose()?;
        let sequence=match stored {
            Some((d,s)) if d==digest => s,
            Some((_,s)) => s.checked_add(1).ok_or_else(||Error::Protocol("schema sequence exhausted".into()))?,
            None => 1,
        };
        if stored!=Some((digest,sequence)) {
            self.backend.commit(vec![Record::put(key,encode(&(digest,sequence))?)])?;
        }
        let (database,node)=self.backend.identity();
        let mut signed=SignedSchema{database,signer:node,sequence,schema:bytes,signature:Vec::new()};
        signed.sign(&self.config.signing_key)?;
        Ok(signed)
    }
    async fn adopt_manifest(&self,_from:NodeId,signed:SignedSchema,send:&crate::queue::Sender)->Result<()> {
        match check_schema_manifest(&self.config,self.backend.as_ref(),&signed)? {
            SchemaDecision::Stale|SchemaDecision::Current|SchemaDecision::Record => {
                let _=send.try_send(Message::SchemaAck{digest:signed.digest(),sequence:signed.sequence});
            }
            SchemaDecision::Adopt => {
                let backend=self.backend.clone();let schema=signed.schema.clone();
                if !tokio::task::spawn_blocking(move||backend.accept_schema(schema)).await.map_err(|e|Error::Unavailable(e.to_string()))?? {
                    return Ok(()); // adopter busy (uncollected writes); sender retries
                }
                record_schema_accepted(self.backend.as_ref(),&signed)?;
                self.control.status.lock().unwrap().metrics.schema_adoptions+=1;
                let _=send.try_send(Message::SchemaAck{digest:signed.digest(),sequence:signed.sequence});
            }
        }
        Ok(())
    }
    async fn want_schema(&self,send:&crate::queue::Sender) {
        let mut last=self.schema_req_at.lock().await;
        let now=std::time::Instant::now();
        if last.is_some_and(|t|now.duration_since(t)<Duration::from_secs(5)) { return; }
        *last=Some(now);
        let _=send.try_send(Message::SchemaRequest);
    }
    /// The master pushes its signed schema to peers that have not acked it.
    async fn push_schema(&self) {
        if !self.is_master() { return; }
        let manifest=match self.manifest() {Ok(m)=>m,Err(e)=>{self.error(&e);return;}};
        let current=(manifest.digest(),manifest.sequence);
        let now=std::time::Instant::now();
        let mut pushed=0u64;
        for peer in self.peers.lock().await.values_mut() {
            if peer.schema_acked==Some(current) || (peer.schema_sent==Some(current) && now<peer.schema_push_at) { continue; }
            peer.schema_push_at=now+Duration::from_secs(30);
            if peer.send.try_send(Message::SchemaManifest(manifest.clone())).is_ok(){peer.schema_sent=Some(current);pushed+=1;}
        }
        if pushed>0{self.control.status.lock().unwrap().metrics.schema_pushes+=pushed;}
    }
    async fn session(self:&Arc<Self>,connection:quinn::Connection,mut send:quinn::SendStream,mut recv:quinn::RecvStream,id:NodeId,outgoing:bool,fetch:Option<SocketAddr>,peer_schema:Vec<u8>)->Result<()> {
        if id==self.backend.identity().1 || self.view.lock().await.excluded.contains(&id){return Err(Error::Auth("excluded peer".into()));}
        {let _gate=self.retention_gate.lock().await;retention::touch(self.backend.as_ref(),id,&[])?;}
        let(tx,mut rx)=crate::queue::Sender::with_control(self.queue.clone(),self.control_queue.clone(),self.config.peer_queue_bytes);
        // A connection that just completed TLS + Hello/Welcome is proven live.
        // When the existing entry is already closed, or has carried no inbound
        // traffic for a while (a zombie to a restarted peer's dead address:
        // live sessions exchange Ping/Pong every second), replace it instead
        // of tie-breaking: the deterministic direction rule would otherwise
        // keep the zombie and close every fresh handshake until the 30s stale
        // prune whenever the id order prefers it, stalling re-mesh. A genuine
        // simultaneous-dial race always finds a fresh entry, so the rule below
        // still resolves it deterministically.
        {let mut peers=self.peers.lock().await;if let Some(old)=peers.get(&id){let prefer_outgoing=self.backend.identity().1<id;let zombie=old.connection.close_reason().is_some()||old.last_seen.elapsed()>Duration::from_secs(5);if !zombie&&(old.outgoing==prefer_outgoing || outgoing!=prefer_outgoing) {connection.close(0u32.into(),b"duplicate connection");return Ok(());}old.connection.close(0u32.into(),b"duplicate connection");}let old=peers.remove(&id);let push=peers.values().filter(|p|p.push).count()<self.config.fanout;let (progress,sent,sent_at,resent_at,resent,ack_any_at,bulk_lag_since,push_origin,schema_acked)=match old {Some(o)=>(o.progress,o.sent,o.sent_at,o.resent_at,o.resent,o.ack_any_at,o.bulk_lag_since,o.push_origin,o.schema_acked),None=>(BTreeMap::new(),BTreeMap::new(),BTreeMap::new(),BTreeMap::new(),BTreeMap::new(),None,None,None,None)};peers.insert(id,Peer{connection:connection.clone(),fetch,send:tx.clone(),progress,last_seen:std::time::Instant::now(),push,lazy:false,outgoing,sent,sent_at,resent_at,resent,ack_any_at,bulk_lag_since,push_origin,schema_acked,schema_push_at:std::time::Instant::now()+Duration::from_secs(30),schema_sent:None,last_touch:None,snapshot_req_at:None});}
        self.view.lock().await.merge(Member{node:id,address:connection.remote_address(),incarnation:0,state:State::Alive});
        let this=self.clone();let dc=connection.clone();let datagrams=tokio::spawn(async move {while let Ok(data)=dc.read_datagram().await {if data.len()>1000 {continue;}if let Err(e)=this.packet(id,&dc,&data).await {this.error(&e);}}});
        let _=tx.try_send(Message::ProgressRequest{offset:0});
        // Schema sync starts inside the session: everyone asks (answers declare
        // each sender's replicated subset for payload filtering), and the
        // master additionally pushes so divergent peers converge without asking.
        if peer_schema!=self.backend.schema()? {
            let _=tx.try_send(Message::SchemaRequest);
            if self.is_master() {
                match self.manifest() {Ok(m)=>{let _=tx.try_send(Message::SchemaManifest(m));},Err(e)=>self.error(&e)}
            }
        }
        let wc=connection.clone();let config=self.config.clone();
        let mut writer=tokio::spawn(async move {while let Some(message)=rx.recv().await {
            if !transport::network_allowed(&config,wc.remote_address()){return Err(Error::Auth("migrated network denied".into()));}
            tokio::time::timeout(Duration::from_secs(5),async {if let Some(frame)=&message.prepared{frame.write(&mut send).await}else{write(&mut send,&message.message).await}}).await.map_err(|_|Error::Unavailable("write deadline".into()))??;
        }Ok(())});
        let result=loop {tokio::select! {
            result=&mut writer=>{break result.unwrap_or_else(|e|Err(Error::Unavailable(e.to_string())));},
            inbound=read(&mut recv)=>match inbound {Ok(message)=>{if let Some(peer)=self.peers.lock().await.get_mut(&id){peer.last_seen=std::time::Instant::now();}if transport::peer_id(&connection,&self.config).is_err(){break Err(Error::Auth("migrated peer denied".into()));}if let Err(e)=self.message(id,message,&tx).await{let _=tx.try_send(Message::Error{code:1,message:e.to_string()});self.error(&e);if matches!(e,Error::Auth(_)){break Err(e);}}},Err(e)=>break Err(e)},
            _=tokio::time::sleep(Duration::from_secs(1))=>{if self.control.stop.load(Ordering::Acquire){break Ok(());}let _=tx.try_send(Message::Ping(rand::random()));},
        }};
        writer.abort();datagrams.abort();{let mut peers=self.peers.lock().await;if peers.get(&id).is_some_and(|p|p.connection.stable_id()==connection.stable_id()){peers.remove(&id);}}connection.close(0u32.into(),b"session closed");result
    }
    fn progress(&self)->Result<Vec<Progress>> {self.backend.progress()}
    async fn request_progress(&self){for peer in self.peers.lock().await.values(){let _=peer.send.try_send(Message::ProgressRequest{offset:0});if self.config.membership {let _=peer.send.try_send(Message::MembersRequest{offset:0});}}}
    async fn message(&self,id:NodeId,message:Message,send:&crate::queue::Sender)->Result<()> {
        match message {
            Message::Batches(txs)=>{if txs.len()>64{return Err(Error::Overloaded);}let schema=schema_digest(&self.backend.schema()?);if txs.iter().any(|tx|tx.schema!=schema){self.want_schema(send).await;}self.stage_batch(txs).await?;},
            Message::PlumtreeData(tx)=>{let duplicate=self.backend.get(&receipt_key(tx.origin,tx.sequence))?.is_some() || self.backend.get(&stage_key(tx.origin,tx.sequence))?.is_some();self.stage(tx).await?;if duplicate {let _=send.try_send(Message::Prune);}},
            Message::Prune=>{if let Some(p)=self.peers.lock().await.get_mut(&id){p.lazy=true;}},
            Message::IHave{origin,sequence}=>{let applied=self.backend.get(&progress_key(origin))?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0);if sequence>applied {let _=send.try_send(Message::Graft{origin,sequence});}},
            Message::Graft{origin,sequence}=>{if let Some(p)=self.peers.lock().await.get_mut(&id){p.lazy=false;}if let Err(error)=self.send_transaction(origin,sequence,send).await {if matches!(error,Error::Unavailable(_)){let _=send.try_send(Message::Error{code:3,message:"history unavailable".into()});}else{return Err(error);}}},
            Message::Need{origin,from,through}=>{if from==0 || through<from {return Err(Error::Protocol("invalid missing range".into()));}if std::env::var("MURMUR_NEED_TIMING").is_ok(){static NEED_CTR:std::sync::atomic::AtomicU64=std::sync::atomic::AtomicU64::new(0);static NEED_N:std::sync::atomic::AtomicU64=std::sync::atomic::AtomicU64::new(0);NEED_N.fetch_add(through.min(from.saturating_add(15)).saturating_sub(from).saturating_add(1),std::sync::atomic::Ordering::Relaxed);if NEED_CTR.fetch_add(1,std::sync::atomic::Ordering::Relaxed)%100==0{eprintln!("[need-timing] served_per_100needs={}",NEED_N.swap(0,std::sync::atomic::Ordering::Relaxed));}}let mut served=from.saturating_sub(1);for seq in from..=through.min(from.saturating_add(15)){if let Err(error)=self.send_transaction(origin,seq,send).await {if matches!(error,Error::Unavailable(_)){let _=send.try_send(Message::Error{code:3,message:"history unavailable".into()});break;}else if matches!(error,Error::Overloaded){continue;}else{return Err(error);}}if seq==served.saturating_add(1){served=seq;}}if served>=from {if let Some(peer)=self.peers.lock().await.get_mut(&id){peer.sent.entry(origin).and_modify(|old|*old=(*old).max(served)).or_insert(served);}}},
            Message::Ack(entries)|Message::ProgressPage{entries,more:false,..}=>self.handle_progress(id,entries,send).await?,
            Message::ProgressPage{offset,entries,more:true}=>{self.handle_progress(id,entries,send).await?;let _=send.try_send(Message::ProgressRequest{offset:offset.saturating_add(128)});},
            Message::ProgressRequest{offset}=>{let entries=self.progress()?;let start=offset as usize;if start>entries.len(){return Err(Error::Protocol("progress offset".into()));}let end=(start+128).min(entries.len());let _=send.try_send(Message::ProgressPage{offset,more:end<entries.len(),entries:entries[start..end].to_vec()});},
            Message::TransactionChunk(chunk)=>self.stage_chunk(chunk).await?,
            Message::ChunkNeed{origin,sequence,indexes}=>self.serve_chunk_need(origin,sequence,indexes,send).await?,
            Message::ChunkAvailabilityRequest{offset}=>self.serve_chunk_availability(offset,send).await?,
            Message::ChunkAvailabilityPage{entries,..}=>self.merge_chunk_availability(id,entries).await?,
            Message::SnapshotRequest=>self.send_snapshot(id,send),
            Message::SnapshotResume{transfer}=>{
                let _gate=self.stage_gate.lock().await;
                if let Some(m)=snapshot::outgoing(self.backend.as_ref(),id,transfer,retention::now_millis())? {
                    send.try_send(Message::SnapshotManifest(m))?;
                }else{send.try_send(Message::SnapshotUnavailable{transfer})?;}
            },
            Message::SnapshotManifest(m)=>{
                let _gate=self.stage_gate.lock().await;
                eprintln!("[snap-rx] manifest from {:02x}{:02x} chunks={} transfer={:02x}{:02x}",id[0],id[1],m.chunks.len(),m.transfer[0],m.transfer[1]);
                snapshot::accept(self.backend.as_ref(),&self.config,id,m.clone(),retention::now_millis())?;
                *self.incoming_hint.lock().unwrap()=Some(m.transfer);*self.incoming_none_at.lock().unwrap()=None;
                self.request_snapshot_chunks(id,m.transfer,send)?;
            },
            Message::SnapshotChunk{transfer,index,data}=>{
                if !self.config.trusted_snapshot_sources.contains(&id) {return Err(Error::Auth("untrusted snapshot chunk source".into()));}
                let _gate=self.stage_gate.lock().await;
                snapshot::receive(self.backend.as_ref(),id,transfer,index,data,retention::now_millis())?;
                *self.missing_cache.lock().unwrap()=None;
            },
            Message::SnapshotNeed{transfer,indexes}=>self.send_snapshot_chunks(id,transfer,indexes,send).await?,
            Message::SnapshotDone{transfer}=>{
                let _gate=self.stage_gate.lock().await;
                self.request_snapshot_chunks(id,transfer,send)?;
            },
            Message::SnapshotApplied{transfer,digest}=>{
                let _gate=self.stage_gate.lock().await;
                self.backend.acknowledge_snapshot(id,transfer,digest)?;
            },
            Message::SnapshotCancel{transfer}=>{
                let _gate=self.stage_gate.lock().await;
                snapshot::cancel(self.backend.as_ref(),id,transfer)?;
                snapshot::maintain(self.backend.as_ref(),retention::now_millis())?;
            },
            Message::SnapshotUnavailable{transfer}=>{
                let _gate=self.stage_gate.lock().await;
                if let Some(r)=self.cached_incoming()? {
                    if r.manifest.donor!=id || r.manifest.transfer!=transfer {return Err(Error::Auth("snapshot unavailable identity".into()));}
                    send.try_send(Message::SnapshotCancel{transfer})?;
                    self.backend.commit(vec![Record::delete(snapshot::header(snapshot::IN,transfer))])?;
                    snapshot::maintain(self.backend.as_ref(),retention::now_millis())?;
                    self.request_snapshot_damped(id,send).await;
                }
            },
            Message::MembersRequest{offset}=>{if !self.config.membership {return Ok(());}let view=self.view.lock().await;let mut members=vec![view.local.clone()];members.extend(view.members.values().cloned());let start=offset as usize;if start>members.len(){return Err(Error::Protocol("membership offset".into()));}let end=(start+128).min(members.len());let _=send.try_send(Message::MembersPage{offset,more:end<members.len(),entries:members[start..end].to_vec()});},
            Message::MembersPage{offset,more,entries}=>{if entries.len()>128{return Err(Error::Overloaded);}for member in entries {self.view.lock().await.merge(member);}if more {let _=send.try_send(Message::MembersRequest{offset:offset.saturating_add(128)});}},
            Message::SchemaRequest=>{let manifest=self.manifest()?;let _=send.try_send(Message::SchemaManifest(manifest));},
            Message::SchemaManifest(signed)=>self.adopt_manifest(id,signed,send).await?,
            Message::SchemaAck{digest,sequence}=>{if let Some(p)=self.peers.lock().await.get_mut(&id){p.schema_acked=Some((digest,sequence));}},Message::Ping(n)=>{let _=send.try_send(Message::Pong(n));},Message::Pong(_)=>{},
            Message::Error{code:3,..}=>{if self.config.trusted_snapshot_sources.contains(&id){self.request_snapshot_damped(id,send).await;}},
            // Record peer errors locally without replying: answering an Error
            // with another Error ping-pongs between nodes, prefixing
            // "peer {code}: " every round trip until messages are megabytes.
            Message::Error{code,message}=>{let mut text=format!("peer {code}: {message}");text.truncate(512);self.error(&Error::Unavailable(text));},
            Message::Hello{..}|Message::Welcome{..}=>return Err(Error::Protocol("repeated handshake".into())),
        } Ok(())
    }
    async fn staged_totals(&self)->Result<(usize,usize)> {
        let mut sizes=self.staged_sizes.lock().await;
        if sizes.is_none() {let mut loaded=StagedSizes::default();let keys=self.backend.keys(b"07/replication/staged/")?;
            for window in keys.chunks(64){let mut cursor=0;while cursor<window.len(){let values=read_bounded(self.backend.as_ref(),&window[cursor..],1<<20)?;let count=values.len();for (key,value) in window[cursor..].iter().zip(values){if let Some(value)=value{loaded.insert(key.clone(),value.len())?;}}cursor+=count;}}
            *sizes=Some(loaded);
        }
        let cached=sizes.as_ref().unwrap();Ok((cached.sizes.len(),cached.bytes))
    }
    async fn staged(&self)->Result<BTreeMap<Vec<u8>,usize>> {
        self.staged_totals().await?;
        Ok(self.staged_sizes.lock().await.as_ref().unwrap().sizes.clone())
    }
    async fn stage(&self,tx:Transaction)->Result<()> {
        self.stage_batch(vec![tx]).await
    }
    async fn stage_batch(&self,transactions:Vec<Transaction>)->Result<()> {
        // Authenticate the complete bounded envelope before publishing anything.
        // Keep individual signatures and reject conflicting identities within it.
        let mut pending=BTreeMap::new();
        for tx in transactions {
            let verified=self.verification.lock().unwrap().verify(&tx,self.backend.identity().0,&self.config.trusted_origins,self.config.max_transaction_bytes)?;
            let digest=verified.digest;let key=stage_key(tx.origin,tx.sequence);
            if let Some((_,old,_))=pending.get(&key) {if old!=&digest{return Err(Error::Auth("conflicting staged transaction".into()));}continue;}
            pending.insert(key,(tx,digest,verified.bytes));
        }
        // Verification, existence probes and filtering run without holding
        // any gate: staging must not wait on applies, retention or
        // snapshots (that stalls the connection read loop and wedges
        // control traffic). Only the commit section below serializes,
        // briefly, against other staging commits, so the observed-max
        // read-modify-write, admission totals and conflict recheck stay
        // atomic. Stale pre-reads only admit work the recheck or the apply
        // loop drops.
        self.staged_totals().await?;
        let mut floors=BTreeMap::new();let mut clock=(0,0);
        // One backend round-trip for all receipt/staged existence probes in
        // this envelope instead of two per transaction.
        let order:Vec<(Vec<u8>,Transaction,[u8;32],Vec<u8>)>=pending.into_iter().map(|(key,(tx,digest,bytes))|(key,tx,digest,bytes)).collect();
        if order.is_empty(){return Ok(());}
        let mut probe=Vec::with_capacity(order.len()*2);
        for (key,tx,_,_) in &order {probe.push(receipt_key(tx.origin,tx.sequence));probe.push(key.clone());}
        let found=read_many(self.backend.as_ref(),&probe)?;
        let origins:Vec<_>=order.iter().map(|(_,tx,_,_)|tx.origin).collect::<BTreeSet<_>>().into_iter().collect();
        let floor_keys=origins.iter().map(|origin|retention::floor_key(*origin)).collect::<Vec<_>>();
        for (origin,value) in origins.iter().zip(read_many(self.backend.as_ref(),&floor_keys)?){floors.insert(*origin,value.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0));}
        let mut candidates=Vec::new();
        let stage_timing=std::env::var("MURMUR_STAGE_TIMING").is_ok();
        let (mut c_floor,mut c_applied,mut c_staged)=(0usize,0usize,0usize);
        for (i,(key,tx,digest,bytes)) in order.into_iter().enumerate() {
            let receipt=found.get(i*2).cloned().flatten();let old=found.get(i*2+1).cloned().flatten();
            let floor=floors.get(&tx.origin).copied().unwrap_or(0);
            if tx.sequence<=floor {c_floor+=1;continue;}
            if let Some(receipt)=receipt {if receipt!=digest{return Err(Error::Auth("conflicting transaction identity".into()));}c_applied+=1;continue;}
            if let Some(old)=old {if decode::<Transaction>(&old)?.digest()?!=digest{return Err(Error::Auth("conflicting staged transaction".into()));}c_staged+=1;continue;}
            clock=clock.max((tx.millis,tx.logical));
            candidates.push((key,tx,digest,bytes));
        }
        if stage_timing {static STAGE_CTR:std::sync::atomic::AtomicU64=std::sync::atomic::AtomicU64::new(0);static STAGE_NEW:std::sync::atomic::AtomicU64=std::sync::atomic::AtomicU64::new(0);static STAGE_APPLIED:std::sync::atomic::AtomicU64=std::sync::atomic::AtomicU64::new(0);static STAGE_STAGED:std::sync::atomic::AtomicU64=std::sync::atomic::AtomicU64::new(0);static STAGE_FLOOR:std::sync::atomic::AtomicU64=std::sync::atomic::AtomicU64::new(0);STAGE_NEW.fetch_add(candidates.len() as u64,std::sync::atomic::Ordering::Relaxed);STAGE_APPLIED.fetch_add(c_applied as u64,std::sync::atomic::Ordering::Relaxed);STAGE_STAGED.fetch_add(c_staged as u64,std::sync::atomic::Ordering::Relaxed);STAGE_FLOOR.fetch_add(c_floor as u64,std::sync::atomic::Ordering::Relaxed);if STAGE_CTR.fetch_add(1,std::sync::atomic::Ordering::Relaxed)%200==0{eprintln!("[stage-timing] new={} dup_applied={} dup_staged={} floor={}",STAGE_NEW.swap(0,std::sync::atomic::Ordering::Relaxed),STAGE_APPLIED.swap(0,std::sync::atomic::Ordering::Relaxed),STAGE_STAGED.swap(0,std::sync::atomic::Ordering::Relaxed),STAGE_FLOOR.swap(0,std::sync::atomic::Ordering::Relaxed));}}
        if candidates.is_empty(){return Ok(());}
        let mut final_added=Vec::new();
        {
            let _gate=self.stage_commit.lock().await;
            // Recheck against concurrent staging commits: receipts or staged
            // entries may have appeared since the first read.
            let mut re=Vec::with_capacity(candidates.len()*2);
            for (key,tx,_,_) in &candidates {re.push(receipt_key(tx.origin,tx.sequence));re.push(key.clone());}
            let fresh=read_many(self.backend.as_ref(),&re)?;
            let mut records=Vec::new();let mut staged_max=BTreeMap::<NodeId,u64>::new();
            for (i,(key,tx,digest,bytes)) in candidates.into_iter().enumerate() {
                let receipt=fresh.get(i*2).cloned().flatten();let old=fresh.get(i*2+1).cloned().flatten();
                if let Some(receipt)=receipt {if receipt!=digest{return Err(Error::Auth("conflicting transaction identity".into()));}continue;}
                if let Some(old)=old {if decode::<Transaction>(&old)?.digest()?!=digest{return Err(Error::Auth("conflicting staged transaction".into()));}continue;}
                staged_max.entry(tx.origin).and_modify(|m|*m=(*m).max(tx.sequence)).or_insert(tx.sequence);
                final_added.push((key.clone(),bytes.len()));records.push(Record::put(key,bytes));
            }
            if final_added.is_empty(){return Ok(());}
            // Fresh admission totals: concurrent commits may have filled the
            // cache since our first read.
            let cached=self.staged_sizes.lock().await;let (fresh_count,fresh_total)=(cached.as_ref().unwrap().sizes.len(),cached.as_ref().unwrap().bytes);drop(cached);
            if fresh_count+final_added.len()>=self.config.staging_transfers {return Err(Error::Overloaded);}
            let mut bytes=fresh_total;
            for (_,size) in &final_added {bytes=bytes.checked_add(*size).ok_or(Error::Overloaded)?;}
            if bytes>self.config.staging_bytes{return Err(Error::Overloaded);}
            // Fresh observed maxima merged with this batch's sequences.
            let obs_keys=staged_max.keys().map(|origin|observed_key(*origin)).collect::<Vec<_>>();
            for ((origin,seq),value) in staged_max.iter().zip(read_many(self.backend.as_ref(),&obs_keys)?){
                let before=value.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0);
                records.push(Record::put(observed_key(*origin),encode(&before.max(*seq))?));
            }
            self.backend.commit(records)?;
        }
        self.backend.observe(clock.0,clock.1)?;
        let count=final_added.len();{let mut cached=self.staged_sizes.lock().await;for (key,size) in final_added{cached.as_mut().unwrap().insert(key,size)?;}}
        self.control.status.lock().unwrap().metrics.received_transactions+=count as u64;
        self.apply_notify.notify_one();Ok(())
    }
    async fn resend_snapshot_receipts(&self,cursor:&mut usize)->Result<()> {
        // Completion receipts survive lost ACKs and either process restarting.
        let keys=self.backend.keys(snapshot::RECEIPTS)?;
        if *cursor>=keys.len(){*cursor=0;}
        let end=cursor.saturating_add(128).min(keys.len());
        for key in &keys[*cursor..end] {
            if let Some(bytes)=self.backend.get(key)? {
                let r:snapshot::Incoming=decode(&bytes)?;
                if let Some(peer)=self.peers.lock().await.get(&r.manifest.donor) {
                    let _=peer.send.try_send(Message::SnapshotApplied{transfer:r.manifest.transfer,digest:r.manifest.digest});
                }
            }
        }
        *cursor=end;
        Ok(())
    }
    /// Incoming snapshot header without a full key scan on every apply.
    /// `snapshot::incoming` lists every key under the incoming prefix
    /// (thousands of chunk keys while a snapshot is active), which costs
    /// seconds per apply under load. Accepts enforce a single incoming
    /// transfer, so a transfer hint plus one validating point read is
    /// equivalent: a live header decodes fresh from the read, a
    /// finished/cancelled/expired one misses and falls back to a rescan
    /// (which also re-arms the hint). The empty hint always rescans, so a
    /// newly accepted transfer is never missed.
    fn cached_incoming(&self)->Result<Option<snapshot::Incoming>> {
        if let Some(transfer)=*self.incoming_hint.lock().unwrap() {
            if let Some(bytes)=self.backend.get(&snapshot::header(snapshot::IN,transfer))? {
                return Ok(Some(decode(&bytes)?));
            }
        }
        // Negative cache: with no transfer active (the common case) every
        // apply batch would otherwise re-scan the snapshot keyspace (~100ms
        // under contention). Accept hints the new transfer directly, so the
        // TTL here is only a backstop and never delays a real transfer.
        let now=std::time::Instant::now();
        if self.incoming_none_at.lock().unwrap().is_some_and(|at|now.saturating_duration_since(at)<Duration::from_secs(5)) {
            return Ok(None);
        }
        let found=snapshot::incoming(self.backend.as_ref())?;
        *self.incoming_hint.lock().unwrap()=found.as_ref().map(|r|r.manifest.transfer);
        if found.is_none() {*self.incoming_none_at.lock().unwrap()=Some(now);}
        Ok(found)
    }
    /// Missing snapshot chunks without a key scan per apply. Every other
    /// caller consults this only after `cached_incoming` validates the
    /// header live, and maintenance never strips chunks under a live
    /// header (header and chunks expire in the same commit), so the only
    /// mutation that can stale the entry is a chunk receive, which clears
    /// it. Stale entries for finished transfers simply miss on the
    /// transfer key and recompute.
    fn cached_missing(&self,r:&snapshot::Incoming)->Result<Vec<u32>> {
        if let Some((transfer,missing))=self.missing_cache.lock().unwrap().clone() {
            if transfer==r.manifest.transfer {
                return Ok(missing);
            }
        }
        let missing=snapshot::missing(self.backend.as_ref(),r)?;
        *self.missing_cache.lock().unwrap()=Some((r.manifest.transfer,missing.clone()));
        Ok(missing)
    }
    /// Ask ONE peer for an apply-blocking hole, damped per range. The old
    /// broadcast asked every peer on every blocked batch; concurrent
    /// streamers stage out of order constantly, so every spurious hole
    /// fanned out N-way and the responses (which bypass the push cursor)
    /// arrived as already-applied duplicates, saturating CPU and queues.
    /// Out-of-order batches land within milliseconds; a hole that persists
    /// past the damping window is re-asked (possibly of another peer).
    async fn request_hole(&self,origin:NodeId,from:u64,through:u64) {
        let now=std::time::Instant::now();
        {
            let mut at=self.need_at.lock().unwrap();
            if at.len()>4096 {at.retain(|_,t|now.saturating_duration_since(*t)<Duration::from_secs(60));}
            if let Some(prev)=at.get(&(origin,from)) {if now.saturating_duration_since(*prev)<Duration::from_secs(1){return;}}
            at.insert((origin,from),now);
        }
        let peers=self.peers.lock().await;
        // Prefer a peer that reports having the range; fall back to the
        // best-known holder so a stale view still retries somewhere.
        let mut best:Option<crate::queue::Sender>=None;let mut best_have=0u64;
        for peer in peers.values() {
            let have=peer.progress.get(&origin).copied().unwrap_or(0);
            if have>=through {best=Some(peer.send.clone());break;}
            if have>=best_have {best_have=have;best=Some(peer.send.clone());}
        }
        if let Some(send)=best {let _=send.try_send(Message::Need{origin,from,through});}
    }
    async fn apply_pending(&self)->Result<bool> {
        let started=std::time::Instant::now();let timing=std::env::var("MURMUR_APPLY_TIMING").is_ok();let limit=if self.config.adaptive_sync {self.adaptive_batch.load(Ordering::Relaxed).min(self.config.batch_transactions).max(1)}else{self.config.batch_transactions};
        // Serialize against retention collection (shared retained heads),
        // but never against staging or snapshots: applies do slow SQLite
        // merge work and must not stall the connection read loop.
        let _gate=self.retained_gate.lock().await;
        let t_gate=started.elapsed();
        {
            if let Some(r)=self.cached_incoming()? {
                let sender=self.peers.lock().await.get(&r.manifest.donor).map(|p|p.send.clone());
                if let Some(send)=sender {
                    if self.cached_missing(&r)?.is_empty() {
                        self.request_snapshot_chunks(r.manifest.donor,r.manifest.transfer,&send)?;
                    }else{let _=send.try_send(Message::SnapshotResume{transfer:r.manifest.transfer});}
                }
            }
        }
        let t_snap=started.elapsed();
        let sizes=self.staged().await?;self.control.status.lock().unwrap().staged_transactions=sizes.len();
        let mut groups=BTreeMap::<NodeId,std::collections::VecDeque<Vec<u8>>>::new();
        for key in sizes.keys() {let origin=parse_id(std::str::from_utf8(key).map_err(|_|Error::Protocol("staged key".into()))?.split('/').nth(3).ok_or_else(||Error::Protocol("staged origin".into()))?)?;groups.entry(origin).or_default().push_back(key.clone());}
        let last=*self.last_origin.lock().unwrap();let mut origins:Vec<_>=groups.keys().copied().collect();
        let split=origins.partition_point(|origin|*origin<=last);origins.rotate_left(split);
        let mut keys=Vec::with_capacity(sizes.len());
        loop {let mut more=false;for origin in &origins {if let Some(key)=groups.get_mut(origin).unwrap().pop_front(){keys.push((*origin,key));more=true;}}if !more{break;}}
        let t_parse=started.elapsed();let mut t_data=Duration::ZERO;let mut t_verify=Duration::ZERO;
        let mut next=BTreeMap::new();let mut blocked=BTreeSet::new();let mut transactions=Vec::new();let mut size=0;let mut purges=Vec::new();
        // Chunked batched reads: one backend round-trip per 64 staged keys
        // instead of one per key. Keys are still processed in strict
        // round-robin order with identical blocked/progress/limit logic;
        // only the reads move ahead in bounded chunks.
        let progress_keys=origins.iter().map(|origin|progress_key(*origin)).collect::<Vec<_>>();
        for (origin,value) in origins.iter().zip(read_many(self.backend.as_ref(),&progress_keys)?){next.insert(*origin,value.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0));}
        let t_prog=started.elapsed();
        let mut cursor=0usize;
        'select: while cursor<keys.len() {
            if transactions.len()>=limit{break;}
            let mut idx=Vec::new();let mut batch=Vec::new();let mut j=cursor;let mut read_bytes=0usize;let mut planned=next.clone();
            while j<keys.len() && batch.len()<64 {let (origin,key)=&keys[j];if !blocked.contains(origin){
                    let sequence=std::str::from_utf8(key).ok().and_then(|key|key.rsplit('/').next()).and_then(|seq|seq.parse::<u64>().ok());
                    if let Some(sequence)=sequence{let expected=planned.get(origin).copied().unwrap_or(0).saturating_add(1);if sequence>expected{blocked.insert(*origin);self.request_hole(*origin,expected,sequence-1).await;j+=1;continue;}if sequence==expected{planned.insert(*origin,sequence);}}
                    let bytes=sizes.get(key).copied().unwrap_or(0);if !batch.is_empty()&&read_bytes.saturating_add(bytes)>self.config.batch_bytes{break;}read_bytes=read_bytes.saturating_add(bytes);idx.push(j);batch.push(key.clone());}j+=1;}
            cursor=j;
            if batch.is_empty(){break;}
            let t_r=std::time::Instant::now();let found=read_many(self.backend.as_ref(),&batch)?;t_data+=t_r.elapsed();
            for (k,b) in idx.into_iter().zip(found) {
                if transactions.len()>=limit{break 'select;}
                let (origin,key)=keys[k].clone();
                let applied=match next.get(&origin){Some(n)=>*n,None=>{let applied=self.backend.get(&progress_key(origin))?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0);next.insert(origin,applied);applied}};
                // Fast stale path: staged keys name their zero-padded
                // sequence, so already-applied keys drop without decoding
                // or verifying them. Unparseable keys fall through to the
                // decoded path; spool-missing keys (phantoms) evict.
                let stale=key.len()>=20&&key[key.len()-20..].iter().all(|c|c.is_ascii_digit())&&std::str::from_utf8(&key[key.len()-20..]).ok().and_then(|s|s.parse::<u64>().ok()).is_some_and(|seq|seq<=applied);
                let Some(b)=b else{purges.push(Record::delete(key));continue;};
                if stale {purges.push(Record::delete(key));continue;}
                let tx:Transaction=decode(&b)?;let t_v=std::time::Instant::now();let verified=crate::verification::bind(&tx,self.backend.identity().0,self.config.max_transaction_bytes)?;t_verify+=t_v.elapsed();
                let applied=if tx.origin==origin{applied}else{match next.get(&tx.origin){Some(n)=>*n,None=>{let applied=self.backend.get(&progress_key(tx.origin))?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0);next.insert(tx.origin,applied);applied}}};
                if tx.sequence<=applied {purges.push(Record::delete(key));continue;}
                if tx.sequence!=applied+1 {blocked.insert(tx.origin);self.request_hole(tx.origin,applied+1,tx.sequence-1).await;continue;}
                if !transactions.is_empty() && size+b.len()>self.config.batch_bytes {break 'select;}size+=b.len();next.insert(tx.origin,tx.sequence);transactions.push((tx,verified));
            }
        }
        let mut records=Vec::new();
        // Batched stale purge: one commit for every dropped key (piggybacked
        // on the apply below when applying) plus one cache pass, instead of
        // a commit and a lock per key.
        if !purges.is_empty() {
            let gone:Vec<Vec<u8>>=purges.iter().map(|record|record.key.clone()).collect();
            if transactions.is_empty(){self.backend.commit(purges)?;}
            else{records.extend(purges);}
            let mut cached=self.staged_sizes.lock().await;for key in gone{cached.as_mut().unwrap().remove(&key);}
        }
        if transactions.is_empty(){if timing{let total_ms=started.elapsed().as_millis();if total_ms>=100{eprintln!("[apply-timing] n=0 staged={} select={}ms backend=0ms gate={}ms snap={}ms parse={}ms prog={}ms data={}ms verify={}ms",sizes.len(),total_ms,t_gate.as_millis(),t_snap.saturating_sub(t_gate).as_millis(),t_parse.saturating_sub(t_snap).as_millis(),t_prog.saturating_sub(t_parse).as_millis(),t_data.as_millis(),t_verify.as_millis());}}return Ok(false);}let mut heads=BTreeMap::<NodeId,Progress>::new();
        let head_origins=transactions.iter().map(|(tx,_)|tx.origin).collect::<BTreeSet<_>>().into_iter().collect::<Vec<_>>();
        let head_keys=head_origins.iter().map(|origin|retained_key(*origin)).collect::<Vec<_>>();
        let retained=read_many(self.backend.as_ref(),&head_keys)?;
        for (origin,value) in head_origins.into_iter().zip(retained){if let Some(value)=value{heads.insert(origin,decode::<Progress>(&value)?);}}
        for (tx,verified) in &mut transactions {records.push(Record::put(log_key(tx.origin,tx.sequence),std::mem::take(&mut verified.bytes)));records.push(Record::put(retention::time_key(tx.origin,tx.sequence),encode(&retention::now_millis())?));
            let mut head=if let Some(head)=heads.remove(&tx.origin){head}else{self.backend.get(&retained_key(tx.origin))?.map(|b|decode::<Progress>(&b)).transpose()?.unwrap_or(Progress{origin:tx.origin,applied:tx.sequence,observed:tx.sequence,retained_from:tx.sequence,retained_through:tx.sequence})};head.applied=tx.sequence;head.observed=head.observed.max(tx.sequence);head.retained_through=tx.sequence;heads.insert(tx.origin,head);records.push(Record::put(receipt_key(tx.origin,tx.sequence),verified.digest.to_vec()));records.push(Record::delete(stage_key(tx.origin,tx.sequence)));}
        let acknowledged:Vec<_>=heads.values().cloned().collect();
        for (origin,head) in heads {records.push(Record::put(progress_key(origin),encode(&head.applied)?));records.push(Record::put(retained_key(origin),encode(&head)?));}
        let backend=self.backend.clone();let payloads=transactions.iter().map(|(tx,_)|tx.payload.clone()).collect();let select_ms=started.elapsed().as_millis();let applied=tokio::task::spawn_blocking(move||backend.apply(payloads,records)).await.map_err(|e|Error::Unavailable(e.to_string()))??;self.control.status.lock().unwrap().deferred=!applied;
        if timing{let total_ms=started.elapsed().as_millis();if total_ms>=100{eprintln!("[apply-timing] n={} staged={} select={}ms backend={}ms gate={}ms snap={}ms parse={}ms prog={}ms data={}ms verify={}ms",transactions.len(),sizes.len(),select_ms,total_ms-select_ms,t_gate.as_millis(),t_snap.saturating_sub(t_gate).as_millis(),t_parse.saturating_sub(t_snap).as_millis(),t_prog.saturating_sub(t_parse).as_millis(),t_data.as_millis(),t_verify.as_millis());}}
        if applied {
            let elapsed=started.elapsed();{let mut status=self.control.status.lock().unwrap();status.metrics.apply_batches+=1;status.metrics.apply_bytes+=size as u64;status.metrics.apply_nanos=status.metrics.apply_nanos.saturating_add(elapsed.as_nanos().min(u64::MAX as u128) as u64);}
            if self.config.adaptive_sync {let next=if elapsed>Duration::from_millis(10){(limit/2).max(1)}else if elapsed<Duration::from_millis(2){(limit*2).min(self.config.batch_transactions)}else{limit};self.adaptive_batch.store(next,Ordering::Relaxed);}
            for (tx,_) in &transactions {self.staged_sizes.lock().await.as_mut().unwrap().remove(&stage_key(tx.origin,tx.sequence));}*self.last_origin.lock().unwrap()=transactions.last().unwrap().0.origin;self.control.status.lock().unwrap().applied_transactions+=transactions.len() as u64;for peer in self.peers.lock().await.values(){for page in acknowledged.chunks(128){let _=peer.send.try_send(Message::Ack(page.to_vec()));}}}Ok(applied)
    }
    /// Stage one transaction chunk with O(1) backend operations. The proof
    /// signature verifies once per transfer (first chunk); later chunks bind
    /// to the durable manifest by digest, and reassembly reads each chunk
    /// exactly once at completion.
    async fn stage_chunk(&self, chunk: Chunk) -> Result<()> {
        // Cheap structural checks first: no crypto, no I/O. Saturating
        // math: the bounds below reject bogus indexes either way.
        let last_len = (chunk.total as usize)
            .saturating_sub((chunk.index as usize).saturating_mul(CHUNK_BYTES));
        if chunk.total as usize > self.config.max_transaction_bytes + 4096
            || chunk.count == 0
            || chunk.count as usize != (chunk.total as usize).div_ceil(CHUNK_BYTES)
            || chunk.index >= chunk.count
            || chunk.data.len()
                != if chunk.index + 1 == chunk.count {
                    last_len
                } else {
                    CHUNK_BYTES
                }
        {
            return Err(Error::Protocol("transaction chunk bounds".into()));
        }
        if !chunk.proof.payload.is_empty()
            || chunk.proof.origin != chunk.origin
            || chunk.proof.sequence != chunk.sequence
            || chunk.proof.id != chunk.id
        {
            return Err(Error::Auth("chunk proof identity".into()));
        }
        let floor = self
            .backend
            .get(&retention::floor_key(chunk.origin))?
            .map(|b| decode::<u64>(&b))
            .transpose()?
            .unwrap_or(0);
        if chunk.sequence <= floor {
            return Ok(());
        }
        let _gate = self.stage_gate.lock().await;
        let mkey = chunk_manifest_key(chunk.origin, chunk.sequence);
        let (manifest, manifest_is_new) = match self.backend.get(&mkey)? {
            Some(bytes) => (decode::<ChunkManifest>(&bytes)?, false),
            None => {
                // Duplicate chunks of a completed transaction are dropped
                // without re-verifying or re-staging anything.
                if self.backend.get(&receipt_key(chunk.origin, chunk.sequence))?.is_some()
                    || self.backend.get(&stage_key(chunk.origin, chunk.sequence))?.is_some()
                {
                    return Ok(());
                }
                chunk
                    .proof
                    .verify_header(self.backend.identity().0, &self.config.trusted_origins)?;
                (
                    ChunkManifest {
                        id: chunk.id,
                        count: chunk.count,
                        total: chunk.total,
                        digest: chunk.digest,
                    },
                    true,
                )
            }
        };
        if manifest.id != chunk.id
            || manifest.count != chunk.count
            || manifest.total != chunk.total
            || manifest.digest != chunk.digest
        {
            return Err(Error::Auth("chunk manifest mismatch".into()));
        }
        let key = chunk_key(chunk.origin, chunk.sequence, chunk.index);
        let is_new = match self.backend.get(&key)? {
            Some(old) => {
                if old != encode(&chunk)? {
                    return Err(Error::Auth("conflicting transaction chunk".into()));
                }
                false
            }
            None => true,
        };
        let manifest_bytes = encode(&manifest)?;
        if manifest_is_new {
            self.charge_chunks(1, manifest_bytes.len()).await?;
        }
        if is_new {
            self.charge_chunks(1, chunk.data.len()).await?;
        }
        let mut records = Vec::new();
        if manifest_is_new {
            records.push(Record::put(mkey, manifest_bytes));
        }
        if is_new {
            records.push(Record::put(key, encode(&chunk)?));
        }
        if !records.is_empty() {
            self.backend.commit(records)?;
        }
        let now = std::time::Instant::now();
        let mut seen = self.chunk_seen.lock().await;
        let received = match seen.get_mut(&(chunk.origin, chunk.sequence)) {
            Some(p) => {
                if is_new {
                    p.received += 1;
                }
                p.updated = now;
                p.received
            }
            None => {
                // First sighting this process: reseed the count from the
                // durable keys (covers restart mid-transfer). A transfer
                // already underway elsewhere counts as stale so the repair
                // sweep picks it up promptly.
                let have = self.count_staged_chunks(chunk.origin, chunk.sequence)? as u32;
                let updated = if manifest_is_new {
                    now
                } else {
                    now.checked_sub(Duration::from_secs(3600)).unwrap_or(now)
                };
                seen.insert(
                    (chunk.origin, chunk.sequence),
                    ChunkProgress {
                        received: have,
                        updated,
                        repaired: now.checked_sub(Duration::from_secs(3600)).unwrap_or(now),
                    },
                );
                have
            }
        };
        drop(seen);
        if received < manifest.count {
            return Ok(());
        }
        // Complete: single reassembly pass, then hand off to staging.
        let mut data = Vec::with_capacity(manifest.total as usize);
        let mut cleanup = Vec::with_capacity(manifest.count as usize + 1);
        for index in 0..manifest.count {
            let Some(bytes) = self.backend.get(&chunk_key(chunk.origin, chunk.sequence, index))? else {
                // Lost a chunk between counting and reading; recount and
                // wait for repair instead of failing the transfer.
                let have = self.count_staged_chunks(chunk.origin, chunk.sequence)? as u32;
                if let Some(p) = self.chunk_seen.lock().await.get_mut(&(chunk.origin, chunk.sequence)) {
                    p.received = have;
                }
                return Ok(());
            };
            let part: Chunk = decode(&bytes)?;
            if part.digest != manifest.digest
                || part.id != manifest.id
                || part.total != manifest.total
                || part.count != manifest.count
            {
                return Err(Error::Auth("mixed transaction chunks".into()));
            }
            data.extend(part.data);
            cleanup.push(Record::delete(chunk_key(chunk.origin, chunk.sequence, index)));
        }
        if data.len() != manifest.total as usize
            || <[u8;32]>::from(Sha256::digest(&data)) != manifest.digest
        {
            return Err(Error::Auth("transaction chunk digest".into()));
        }
        let tx: Transaction = decode(&data)?;
        if tx.origin != chunk.origin || tx.sequence != chunk.sequence || tx.id != chunk.id {
            return Err(Error::Auth("chunk transaction identity".into()));
        }
        cleanup.push(Record::delete(chunk_manifest_key(chunk.origin, chunk.sequence)));
        let manifest_len = encode(&manifest)?.len();
        drop(_gate);
        self.stage(tx).await?;
        self.backend.commit(cleanup)?;
        self.release_chunks(manifest.count as usize + 1, manifest.total as usize + manifest_len)
            .await;
        self.chunk_seen.lock().await.remove(&(chunk.origin, chunk.sequence));
        self.chunk_holders.lock().await.remove(&(chunk.origin, chunk.sequence));
        Ok(())
    }

    /// Count staged chunk keys (not the manifest) for one transfer.
    fn count_staged_chunks(&self, origin: NodeId, seq: u64) -> Result<usize> {
        let prefix = chunk_prefix(origin, seq);
        Ok(self
            .backend
            .keys(prefix.as_bytes())?
            .iter()
            .filter(|k| !k.ends_with(b"manifest"))
            .count())
    }

    /// Reserve staging capacity for chunk bytes. Counters rebuild from a
    /// single key scan on first use after (re)start, conservatively
    /// estimating values at one full chunk each (fail-closed direction).
    async fn charge_chunks(&self, keys: usize, bytes: usize) -> Result<()> {
        let mut counters = self.chunk_counters.lock().await;
        if counters.is_none() {
            let n = self.backend.keys(b"07/replication/chunks/")?.len();
            *counters = Some((n, n.saturating_mul(CHUNK_BYTES)));
        }
        let (count, total) = counters.as_mut().unwrap();
        *count += keys;
        *total += bytes;
        if *total > self.config.staging_bytes || *count > self.config.staging_transfers * 1024 {
            *count -= keys;
            *total -= bytes;
            return Err(Error::Overloaded);
        }
        Ok(())
    }

    async fn release_chunks(&self, keys: usize, bytes: usize) {
        if let Some((count, total)) = self.chunk_counters.lock().await.as_mut() {
            *count = count.saturating_sub(keys);
            *total = total.saturating_sub(bytes);
        }
    }

    /// Serve requested chunk indexes, from the complete transaction when
    /// retained, else from staged chunks as a repair relay. Holding nothing
    /// is a quiet miss: repair requests are broadcast, so most peers won't
    /// have the data.
    async fn serve_chunk_need(
        &self,
        origin: NodeId,
        sequence: u64,
        indexes: Vec<u32>,
        send: &crate::queue::Sender,
    ) -> Result<()> {
        if indexes.len() > 128 {
            return Err(Error::Overloaded);
        }
        if let Some(b) = self.backend.get(&log_key(origin, sequence))? {
            let tx: Transaction = decode(&b)?;
            let data = encode(&tx)?;
            let digest: [u8; 32] = Sha256::digest(data.as_slice()).into();
            let count = data.len().div_ceil(CHUNK_BYTES);
            for index in &indexes {
                if *index as usize >= count {
                    return Err(Error::Protocol("chunk index".into()));
                }
                let start = *index as usize * CHUNK_BYTES;
                let _ = send.try_send(Message::TransactionChunk(Chunk {
                    proof: proof(&tx),
                    origin,
                    sequence,
                    id: tx.id,
                    index: *index,
                    count: count as u32,
                    total: data.len() as u64,
                    digest,
                    data: data[start..(start + CHUNK_BYTES).min(data.len())].to_vec(),
                }));
            }
            self.control.status.lock().unwrap().metrics.chunk_need_served += indexes.len() as u64;
            return Ok(());
        }
        let mkey = chunk_manifest_key(origin, sequence);
        let Some(manifest_bytes) = self.backend.get(&mkey)? else {
            return Ok(());
        };
        let manifest: ChunkManifest = decode(&manifest_bytes)?;
        let mut served = 0u64;
        for index in &indexes {
            if *index >= manifest.count {
                return Err(Error::Protocol("chunk index".into()));
            }
            if let Some(bytes) = self.backend.get(&chunk_key(origin, sequence, *index))? {
                let part: Chunk = decode(&bytes)?;
                let _ = send.try_send(Message::TransactionChunk(part));
                served += 1;
            }
        }
        self.control.status.lock().unwrap().metrics.chunk_need_served += served;
        Ok(())
    }

    /// Transfers with a durable chunk manifest (partially received).
    /// Key form: `07/replication/chunks/<origin-hex>/<seq20>/manifest`.
    fn staged_transfers(&self) -> Result<Vec<(NodeId, u64)>> {
        let mut out = Vec::new();
        for key in self.backend.keys(b"07/replication/chunks/")? {
            let parts: Vec<&str> = std::str::from_utf8(&key)
                .unwrap_or("")
                .split('/')
                .collect();
            if parts.len() == 6 && parts[5] == "manifest" {
                if let (Ok(origin), Ok(seq)) = (parse_id(parts[3]), parts[4].parse::<u64>()) {
                    out.push((origin, seq));
                }
            }
        }
        Ok(out)
    }

    /// Indexes currently staged for one transfer.
    fn present_indexes(&self, origin: NodeId, seq: u64) -> Result<BTreeSet<u32>> {
        let mut out = BTreeSet::new();
        for key in self.backend.keys(chunk_prefix(origin, seq).as_bytes())? {
            if key.ends_with(b"manifest") {
                continue;
            }
            if let Some(suffix) = key.rsplit(|b| *b == b'/').next() {
                if let Ok(index) = std::str::from_utf8(suffix).unwrap_or("").parse::<u32>() {
                    out.insert(index);
                }
            }
        }
        Ok(out)
    }

    /// Answer "what chunks do you hold": staged partial transfers with
    /// their present indexes, 64 transfers per page. Complete transactions
    /// are omitted by design: holders serve broadcast repair requests from
    /// the log without advertising.
    async fn serve_chunk_availability(
        &self,
        offset: u32,
        send: &crate::queue::Sender,
    ) -> Result<()> {
        let mut transfers = self.staged_transfers()?;
        transfers.sort();
        let pages: Vec<_> = transfers
            .into_iter()
            .skip(offset as usize)
            .collect::<Vec<_>>()
            .chunks(64)
            .map(|c| c.to_vec())
            .collect();
        let total = pages.len();
        for (i, page) in pages.into_iter().enumerate() {
            let mut entries = Vec::with_capacity(page.len());
            for (origin, seq) in page {
                let present: Vec<u32> = self.present_indexes(origin, seq)?.into_iter().collect();
                entries.push((origin, seq, present));
            }
            let _ = send.try_send(Message::ChunkAvailabilityPage {
                entries,
                more: i + 1 < total,
            });
        }
        if total == 0 {
            let _ = send.try_send(Message::ChunkAvailabilityPage {
                entries: Vec::new(),
                more: false,
            });
        }
        Ok(())
    }

    /// Merge a holder bitmap page. Entries are capped: a 64 MiB transaction
    /// has 1024 chunks, so 4096 indexes per entry is ample headroom.
    async fn merge_chunk_availability(
        &self,
        from: NodeId,
        entries: Vec<(NodeId, u64, Vec<u32>)>,
    ) -> Result<()> {
        if entries.len() > 64 {
            return Err(Error::Overloaded);
        }
        let now = std::time::Instant::now();
        let mut holders = self.chunk_holders.lock().await;
        for (origin, seq, indexes) in entries {
            if indexes.len() > 4096 {
                return Err(Error::Overloaded);
            }
            let entry = holders
                .entry((origin, seq))
                .or_insert_with(|| (BTreeMap::new(), now));
            entry.0.insert(from, indexes.into_iter().collect());
            entry.1 = now;
        }
        Ok(())
    }

    /// Drop staged chunks plus manifest for one transfer and release the
    /// reserved capacity.
    async fn cleanup_transfer(&self, origin: NodeId, seq: u64) -> Result<()> {
        let mkey = chunk_manifest_key(origin, seq);
        let manifest: Option<ChunkManifest> =
            self.backend.get(&mkey)?.map(|b| decode(&b)).transpose()?;
        let mut records = Vec::new();
        for key in self.backend.keys(chunk_prefix(origin, seq).as_bytes())? {
            records.push(Record::delete(key));
        }
        if !records.is_empty() {
            self.backend.commit(records)?;
        }
        if let Some(m) = manifest {
            self.release_chunks(m.count as usize + 1, m.total as usize + encode(&m)?.len())
                .await;
        }
        self.chunk_seen.lock().await.remove(&(origin, seq));
        self.chunk_holders.lock().await.remove(&(origin, seq));
        Ok(())
    }

    /// Periodic gap repair for incomplete chunked transfers. Recovers
    /// restart state from durable manifests, re-requests missing indexes
    /// (directed by holder bitmaps when known, broadcast otherwise), and
    /// drops chunk garbage for transactions that completed another way.
    async fn repair_chunks(&self) -> Result<()> {
        let now = std::time::Instant::now();
        // Seed in-memory progress from durable manifests. Only needed when
        // the map is empty (restart): live transfers insert on arrival, and
        // the insert sits in the same critical section as the manifest
        // commit with no await between, so no other gap is possible.
        if self.chunk_seen.lock().await.is_empty() {
            for (origin, seq) in self.staged_transfers()? {
                let have = self.count_staged_chunks(origin, seq)? as u32;
                self.chunk_seen.lock().await.insert(
                    (origin, seq),
                    ChunkProgress {
                        received: have,
                        updated: now.checked_sub(Duration::from_secs(3600)).unwrap_or(now),
                        repaired: now.checked_sub(Duration::from_secs(3600)).unwrap_or(now),
                    },
                );
            }
        }
        self.chunk_holders
            .lock()
            .await
            .retain(|_, (_, at)| now.duration_since(*at) < Duration::from_secs(60));
        let incomplete: Vec<(NodeId, u64)> = self
            .chunk_seen
            .lock()
            .await
            .keys()
            .copied()
            .collect();
        for (origin, seq) in incomplete {
            let mkey = chunk_manifest_key(origin, seq);
            let Some(manifest_bytes) = self.backend.get(&mkey)? else {
                self.chunk_seen.lock().await.remove(&(origin, seq));
                continue;
            };
            let manifest: ChunkManifest = decode(&manifest_bytes)?;
            // Completed through another path (full re-send won the race)?
            // Drop the leftover chunks.
            if self.backend.get(&receipt_key(origin, seq))?.is_some()
                || self.backend.get(&stage_key(origin, seq))?.is_some()
                || self.backend.get(&log_key(origin, seq))?.is_some()
            {
                self.cleanup_transfer(origin, seq).await?;
                continue;
            }
            let (updated, repaired) = match self.chunk_seen.lock().await.get(&(origin, seq)) {
                Some(p) => (p.updated, p.repaired),
                None => continue,
            };
            // Chunks still flowing, or recently asked: leave it alone.
            if now.duration_since(updated) < Duration::from_secs(5)
                || now.duration_since(repaired) < Duration::from_secs(10)
            {
                continue;
            }
            let present = self.present_indexes(origin, seq)?;
            if let Some(p) = self.chunk_seen.lock().await.get_mut(&(origin, seq)) {
                p.received = present.len() as u32;
            }
            let missing: Vec<u32> = (0..manifest.count)
                .filter(|i| !present.contains(i))
                .collect();
            if missing.is_empty() {
                // Bitmap complete but reassembly never ran (completion
                // crashed): ask for a full re-send via a transaction Need
                // and let the duplicate chunks trigger reassembly.
                let peers = self.peers.lock().await;
                if peers.is_empty() {
                    continue;
                }
                for peer in peers.values() {
                    let _ = peer.send.try_send(Message::Need {
                        origin,
                        from: seq,
                        through: seq,
                    });
                }
                drop(peers);
                if let Some(p) = self.chunk_seen.lock().await.get_mut(&(origin, seq)) {
                    p.repaired = now;
                }
                continue;
            }
            let sent = self.request_missing_chunks(origin, seq, &missing).await;
            // Only re-arm the flood gate when a request actually went out;
            // otherwise the next sweep must retry (e.g. peers not yet
            // connected) instead of sleeping through the full interval.
            if sent > 0 {
                if let Some(p) = self.chunk_seen.lock().await.get_mut(&(origin, seq)) {
                    p.repaired = now;
                }
            }
        }
        Ok(())
    }

/// Direct each missing index at a holder round-robin; indexes with no
/// known holder fall back to broadcast.
fn select_holders(
    missing: &[u32],
    holders: &BTreeMap<NodeId, BTreeSet<u32>>,
) -> (BTreeMap<NodeId, Vec<u32>>, Vec<u32>) {
    let mut directed: BTreeMap<NodeId, Vec<u32>> = BTreeMap::new();
    let mut orphaned = Vec::new();
    for (i, index) in missing.iter().enumerate() {
        let having: Vec<NodeId> = holders
            .iter()
            .filter(|(_, set)| set.contains(index))
            .map(|(id, _)| *id)
            .collect();
        if having.is_empty() {
            orphaned.push(*index);
        } else {
            directed
                .entry(having[i % having.len()])
                .or_default()
                .push(*index);
        }
    }
    (directed, orphaned)
}

    /// Ask for missing chunk indexes, directed at known holders when
    /// holder bitmaps exist, broadcast otherwise (with an availability
    /// query attached so the next round can be directed).
    async fn request_missing_chunks(&self, origin: NodeId, seq: u64, missing: &[u32]) -> usize {
        let peers: Vec<(NodeId, crate::queue::Sender)> = self
            .peers
            .lock()
            .await
            .iter()
            .map(|(id, p)| (*id, p.send.clone()))
            .collect();
        if peers.is_empty() {
            return 0;
        }
        let holders = self
            .chunk_holders
            .lock()
            .await
            .get(&(origin, seq))
            .map(|(m, _)| m.clone())
            .unwrap_or_default();
        let want: Vec<u32> = missing.iter().take(128).copied().collect();
        let mut sent = 0usize;
        if holders.is_empty() {
            for (_, send) in &peers {
                let _ = send.try_send(Message::ChunkNeed {
                    origin,
                    sequence: seq,
                    indexes: want.clone(),
                });
                let _ = send.try_send(Message::ChunkAvailabilityRequest { offset: 0 });
                self.control.status.lock().unwrap().metrics.chunk_need_sent += 1;
                sent += 1;
            }
            return sent;
        }
        let (directed, orphaned) = Self::select_holders(&want, &holders);
        for (peer, indexes) in &directed {
            if let Some((_, send)) = peers.iter().find(|(id, _)| id == peer) {
                let _ = send.try_send(Message::ChunkNeed {
                    origin,
                    sequence: seq,
                    indexes: indexes.clone(),
                });
                self.control.status.lock().unwrap().metrics.chunk_need_sent += 1;
                sent += 1;
            }
        }
        if !orphaned.is_empty() {
            for (_, send) in &peers {
                let _ = send.try_send(Message::ChunkNeed {
                    origin,
                    sequence: seq,
                    indexes: orphaned.clone(),
                });
                self.control.status.lock().unwrap().metrics.chunk_need_sent += 1;
                sent += 1;
            }
        }
        sent
    }
    async fn handle_progress(&self,id:NodeId,entries:Vec<Progress>,send:&crate::queue::Sender)->Result<()> {
        if entries.len()>128{return Err(Error::Overloaded);}
        for p in &entries {if p.observed<p.applied || p.retained_from>p.retained_through && p.retained_through!=0{return Err(Error::Protocol("invalid progress".into()));}}
        // In-memory peer progress (drives push windows) updates on every Ack
        // without touching the retention gate. Durable peer-ACK floors
        // persist at most once per second per peer: a Spool commit per Ack
        // would serialize behind the worker and lump ACK processing into
        // multi-second bursts. Unknown peers always touch (retire check).
        let now=std::time::Instant::now();
        let due=if let Some(peer)=self.peers.lock().await.get_mut(&id){for p in &entries {if p.applied>peer.progress.get(&p.origin).copied().unwrap_or(0){peer.ack_any_at=Some(now);}peer.progress.insert(p.origin,p.applied);}peer.last_seen=now;if peer.last_touch.is_none_or(|t|now.saturating_duration_since(t)>=Duration::from_secs(1)){peer.last_touch=Some(now);true}else{false}}else{true};
        if due {let _gate=self.retention_gate.lock().await;retention::touch(self.backend.as_ref(),id,&entries)?;}
        let keys=entries.iter().map(|p|progress_key(p.origin)).collect::<Vec<_>>();
        let values=read_many(self.backend.as_ref(),&keys)?;
        let mut max_lag=0u64;
        for (p,value) in entries.into_iter().zip(values) {if p.observed<p.applied || p.retained_from>p.retained_through && p.retained_through!=0{return Err(Error::Protocol("invalid progress".into()));}
            let applied=value.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0);
            let next=applied.saturating_add(1);
            max_lag=max_lag.max(p.retained_through.saturating_sub(applied));
            let missing_history=p.retained_from>next || (p.applied>applied && p.retained_through<=applied);
            if missing_history {
                if self.config.trusted_snapshot_sources.contains(&id) {self.request_snapshot_damped(id,send).await;}
                else {self.error(&Error::Unavailable("catch-up requires an explicitly trusted snapshot source".into()));}
            }else if p.retained_through>applied {let _=send.try_send(Message::Need{origin:p.origin,from:next,through:p.retained_through.min(applied.saturating_add(16))});}
        }
        // Bulk catch-up: a large STABLE per-origin lag is cheaper via
        // snapshot than per-tx repair even when all history is retained
        // (outage/partition victims face 10k+ gaps that grind for many
        // minutes transaction by transaction). Stability (30s) filters
        // transient bursts; trust gates acceptance; the request itself is
        // 5s-damped and donors reuse fresh per-recipient builds, so
        // repeats are cheap. Silent when untrusted (opportunistic).
        if max_lag>SNAP_BULK_LAG_TX {
            let mut fire=false;
            if let Some(peer)=self.peers.lock().await.get_mut(&id) {
                match peer.bulk_lag_since {
                    None=>{peer.bulk_lag_since=Some(now);}
                    Some(since)=>{if now.saturating_duration_since(since)>=SNAP_BULK_LAG_STABLE{fire=true;}}
                }
            }
            if fire && self.config.trusted_snapshot_sources.contains(&id) && self.cached_incoming()?.is_none() {eprintln!("[snap-bulk] requesting from {:02x}{:02x} lag={max_lag}",id[0],id[1]);self.request_snapshot_damped(id,send).await;}
        }else if let Some(peer)=self.peers.lock().await.get_mut(&id) {peer.bulk_lag_since=None;}
        Ok(())
    }
    async fn send_transaction(&self,origin:NodeId,seq:u64,send:&crate::queue::Sender)->Result<()> {
        let Some(data)=self.backend.get(&log_key(origin,seq))? else {return Err(Error::Unavailable("retained transaction missing".into()));};let tx:Transaction=decode(&data)?;
        self.send_loaded_transaction(origin,seq,Arc::new(Outgoing{data:Arc::new(data),tx:Arc::new(tx)}),send).await
    }
    async fn send_loaded_transaction(&self,origin:NodeId,seq:u64,value:Arc<Outgoing>,send:&crate::queue::Sender)->Result<()> {
        let data=value.data.clone();let tx=value.tx.clone();
        if data.len()<1<<20 {send.try_send(if self.config.dissemination==Dissemination::Plumtree{Message::PlumtreeData((*tx).clone())}else{Message::Batches(vec![(*tx).clone()])}).map_err(|_|Error::Overloaded)?;}
        else {
            let digest: [u8; 32] = Sha256::digest(data.as_slice()).into();
            let count = data.len().div_ceil(CHUNK_BYTES);
            // One stream per transaction: duplicate requests while a
            // transfer is in flight are acknowledged without re-sending.
            {
                let mut active = self.transfers_active.lock().await;
                if !active.insert((send.id(),origin, seq)) {
                    return Ok(());
                }
            }
            let permit = match self.transfers.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    self.transfers_active.lock().await.remove(&(send.id(),origin, seq));
                    return Err(Error::Overloaded);
                }
            };
            let send = send.clone();
            let active = self.transfers_active.clone();
            self.spawn(async move {
                let _permit = permit;
                for (index, part) in data.chunks(CHUNK_BYTES).enumerate() {
                    if send
                        .send(Message::TransactionChunk(Chunk {
                            proof: proof(&tx),
                            origin,
                            sequence: seq,
                            id: tx.id,
                            index: index as u32,
                            count: count as u32,
                            total: data.len() as u64,
                            digest,
                            data: part.to_vec(),
                        }))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                active.lock().await.remove(&(send.id(),origin, seq));
            });
        }
        Ok(())
    }
    async fn push_logs(&self)->Result<()> {
        let push_start=std::time::Instant::now();let push_timing=std::env::var("MURMUR_PUSH_TIMING").is_ok();
        let mut progress=self.progress()?;progress.sort_by_key(|p|p.origin);
        let targets:Vec<_>=self.peers.lock().await.iter().filter(|(_,p)|p.push).map(|(id,p)|(*id,p.send.clone(),p.progress.clone(),p.sent.clone(),p.sent_at.clone(),p.resent_at.clone(),p.resent.clone(),p.ack_any_at,p.lazy,p.push_origin)).collect();
        let push_log=std::env::var("MURMUR_PUSH_LOG").is_ok();
        let mut reuse=OutgoingRound::default();let mut push_sent_total=0usize;let push_targets=targets.len();let mut push_streamed=0usize;let mut push_walked=0usize;let mut push_gated=0usize;
        for (id,send,known,sent,sent_at,resent_at,resent,ack_any_at,lazy,push_origin) in targets {
            let mut batch=Vec::new();let mut batch_bytes=0;let mut sent_entries=Vec::new();let mut quiet_sent=Vec::new();let mut resent_walk:Vec<(NodeId,u64)>=Vec::new();
            for p in push_order(&progress,push_origin) {
                // Retransmit unacked ranges once pushing goes quiet: `sent`
                // dedupes the stream while new transactions flow, but without
                // a resend timer a follower that fell behind would only ever
                // trickle back via 10 s anti-entropy Needs (minutes of tail
                // for seconds of backlog). Resends are idempotent downstream.
                // A quiet resend walks forward through the unacked range at
                // full tick rate (fast recovery); only a full-range retry
                // waits out the interval, and resends never refresh the
                // quiet timer (a partial resend would defer the rest).
                // Repeating the same 16 every tick instead buries receivers
                // in duplicates and saturates small boxes.
                let acked=known.get(&p.origin).copied().unwrap_or(0);
                let now=std::time::Instant::now();
                let quiet=push_quiet(sent_at.get(&p.origin).copied(),now);
                let mut restarted=false;
                let from=if !quiet {
                    push_streamed+=1;
                    push_from(acked,sent.get(&p.origin).copied().unwrap_or(0),sent_at.get(&p.origin).copied(),now)
                } else {
                    // Resend only into total ack silence: while the peer
                    // applies anything at all, per-origin recovery runs via
                    // explicit Needs (holes) and streaming (new data), and
                    // re-walking idle origins just buries the receiver in
                    // already-applied duplicates. Those duplicates congest
                    // the session enough to drop Acks, which would trigger
                    // more walks: gating on peer-level progress breaks that
                    // collapse. A fresh peer (no acks yet) still walks.
                    if ack_any_at.is_some_and(|at|now.saturating_duration_since(at)<=RESEND_AFTER_QUIET) {push_gated+=1;continue;}
                    push_walked+=1;
                    let mut start=resent.get(&p.origin).copied().map(|r|acked.max(r).saturating_add(1)).unwrap_or(acked.saturating_add(1));
                    if start>p.retained_through {
                        if resent_at.get(&p.origin).copied().is_some_and(|at|now.saturating_duration_since(at)<=RESEND_AFTER_QUIET) {continue;}
                        start=acked.saturating_add(1);
                        restarted=true;
                    }
                    start
                };
                let before=sent_entries.len();
                // One backend round-trip for the whole lookahead: per-key reads
                // here saturate a worker-threaded backend at catch-up scale.
                // Reads may run past a hole (bounded 16); iteration below
                // keeps the exact stop-at-first-missing semantics.
                let end=p.retained_through.min(from.saturating_add(15)).min(acked.saturating_add(PUSH_INFLIGHT_WINDOW));
                if push_log && from<=end {
                    eprintln!("[push] peer={:02x}{:02x} origin={:02x}{:02x} from={from} end={end} acked={acked} sent={} quiet={} resent={:?} retained={}",id[0],id[1],p.origin[0],p.origin[1],sent.get(&p.origin).copied().unwrap_or(0),quiet as u8,resent.get(&p.origin).copied(),p.retained_through);
                }
                let seqs:Vec<u64>=(from..=end).take(64usize.saturating_sub(sent_entries.len())).collect();
                let bullet=self.load_outgoing(p.origin,&seqs)?;
                for (seq,data) in seqs.into_iter().zip(bullet) {
                    if sent_entries.len()>=64 {break;}
                    let Some(value)=data else{break;};let data=&value.data;
                    if data.len()>=1<<20 {
                        match self.send_loaded_transaction(p.origin,seq,value.clone(),&send).await {Ok(())=>sent_entries.push((p.origin,seq)),Err(Error::Overloaded)=>break,Err(e)=>return Err(e)}
                    }else if lazy && self.config.dissemination==Dissemination::Plumtree {
                        if send.try_send(Message::IHave{origin:p.origin,sequence:seq}).is_err(){break;}sent_entries.push((p.origin,seq));
                    }else if self.config.dissemination==Dissemination::Plumtree {
                        let message=Message::PlumtreeData((*value.tx).clone());let frame=reuse.frame(vec![(p.origin,seq)],&message)?;if send.try_send_prepared(message,frame).is_err(){break;}sent_entries.push((p.origin,seq));
                    }else {
                        if batch_bytes+data.len()>1<<20 {break;}
                        batch_bytes+=data.len();batch.push((*value.tx).clone());sent_entries.push((p.origin,seq));
                    }
                }
                if quiet && sent_entries.len()>before {if let Some(top)=sent_entries[before..].iter().map(|entry|entry.1).max() {resent_walk.push((p.origin,top));}if restarted {quiet_sent.push(p.origin);}}
                if sent_entries.len()>=64 || batch_bytes>=1<<20 {break;}
            }
            if !batch.is_empty() {
                let queued:Vec<_>=batch.iter().map(|tx|(tx.origin,tx.sequence)).collect();
                let message=Message::Batches(batch);let frame=reuse.frame(queued.clone(),&message)?;
                if send.try_send_prepared(message,frame).is_err(){sent_entries.retain(|entry|!queued.contains(entry));}
            }
            if !sent_entries.is_empty() {
                push_sent_total+=sent_entries.len();
                self.control.status.lock().unwrap().metrics.pushed_transactions+=sent_entries.len() as u64;
                // Only advancing pushes refresh the quiet timer; resends of
                // already-sent ranges must not suppress further resends.
                // Quiet resends stamp their own timer so the next resend
                // waits a full interval instead of firing every tick.
                if let Some(peer)=self.peers.lock().await.get_mut(&id).filter(|peer|peer.send.id()==send.id()) {peer.push_origin=sent_entries.last().map(|entry|entry.0);let now=std::time::Instant::now();let live:std::collections::BTreeSet<_>=sent_entries.iter().map(|entry|entry.0).collect();for origin in quiet_sent {if live.contains(&origin) {peer.resent_at.insert(origin,now);}}for (origin,top) in resent_walk {if live.contains(&origin) {peer.resent.entry(origin).and_modify(|old|*old=(*old).max(top)).or_insert(top);}}for (origin,sequence) in sent_entries {let advanced=sequence>peer.sent.get(&origin).copied().unwrap_or(0);peer.sent.entry(origin).and_modify(|old|*old=(*old).max(sequence)).or_insert(sequence);if advanced {peer.sent_at.insert(origin,now);}}}
            }
        }
        if push_timing {static PUSH_CTR:std::sync::atomic::AtomicU64=std::sync::atomic::AtomicU64::new(0);if PUSH_CTR.fetch_add(1,std::sync::atomic::Ordering::Relaxed)%50==0 {eprintln!("[push-timing] targets={} sent={} stream={} walk={} gated={} elapsed={}ms",push_targets,push_sent_total,push_streamed,push_walked,push_gated,push_start.elapsed().as_millis());}}
        Ok(())
    }
    async fn rotate(&self,legacy_due:bool) {
        use rand::seq::SliceRandom;
        // Catch-up targeting: a behind peer that never holds a push slot
        // repairs only via 10 s anti-entropy (a minute of tail for seconds
        // of backlog), and the legacy single swap per 30 s cycles five
        // starved peers far too slowly for short verdicts. Promote every
        // behind non-push peer immediately, evicting caught-up push peers
        // first (peer send state persists across flag flips, so churn is
        // free: no replay blasts). The legacy availability swap stays for
        // steady-state coverage when nobody is behind.
        let retained:std::collections::BTreeMap<NodeId,u64>=self.progress().unwrap_or_default().into_iter().map(|p|(p.origin,p.retained_through)).collect();
        let lag=|p:&Peer|->u64 {retained.iter().map(|(origin,through)|through.saturating_sub(p.progress.get(origin).copied().unwrap_or(0))).max().unwrap_or(0)};
        let mut peers=self.peers.lock().await;
        let mut incoming:Vec<NodeId>=peers.iter().filter(|(_,p)|!p.push && p.connection.close_reason().is_none() && lag(p)>16).map(|(id,_)|*id).collect();
        incoming.shuffle(&mut rand::thread_rng());
        incoming.sort_by_key(|id|std::cmp::Reverse(peers.get(id).map(lag).unwrap_or(0)));
        let mut outgoing:Vec<NodeId>=peers.iter().filter(|(_,p)|p.push).map(|(id,_)|*id).collect();
        outgoing.sort_by_key(|id|peers.get(id).map(lag).unwrap_or(0));
        let mut swapped=false;
        let mut swaps=Vec::new();
        for (new,old) in incoming.into_iter().zip(outgoing.into_iter()) {
            // Never evict a more-behind peer for a less-behind one.
            let (lag_new,lag_old)=(peers.get(&new).map(lag).unwrap_or(0),peers.get(&old).map(lag).unwrap_or(0));
            if lag_old>=lag_new && lag_old>16 {break;}
            peers.get_mut(&old).unwrap().push=false;peers.get_mut(&new).unwrap().push=true;swapped=true;
            swaps.push((new,old,lag_new,lag_old));
        }
        if std::env::var("MURMUR_PUSH_LOG").is_ok() && (!swaps.is_empty() || legacy_due) {
            let lags:Vec<(String,u64,bool)>=peers.iter().map(|(id,p)|{let mut h=String::new();for b in id.iter().take(2){h.push_str(&format!("{b:02x}"));} (h,lag(p),p.push)}).collect();
            eprintln!("[rotate] swaps={swaps:?} lags={lags:?}");
        }
        if swapped {return;}
        if !legacy_due {return;}
        let mut candidates:Vec<_>=peers.iter().filter(|(_,p)|!p.push && p.connection.close_reason().is_none()).map(|(id,p)|(*id,p.send.available())).collect();
        candidates.shuffle(&mut rand::thread_rng());
        let new=candidates.into_iter().take(2).max_by_key(|(_,available)|*available);
        let old=peers.iter().filter(|(_,p)|p.push).min_by_key(|(_,p)|p.send.available()).map(|(id,_)|*id);
        if let (Some((new,_)),Some(old))=(new,old) {peers.get_mut(&old).unwrap().push=false;peers.get_mut(&new).unwrap().push=true;}
    }
    async fn probe(&self) {let(db,node)=self.backend.identity();let lost=self.view.lock().await.expire();use rand::seq::IteratorRandom;let target=self.peers.lock().await.keys().copied().choose(&mut rand::thread_rng());if let Some(target)=target {let nonce=rand::random();let members={let mut view=self.view.lock().await;view.pending.entry(target).or_insert((std::time::Instant::now(),nonce,false));view.gossip()};let connection=self.peers.lock().await.get(&target).map(|peer|peer.connection.clone());if let Some(connection)=connection{if let Ok(data)=encode(&Packet::Ping{database:db,sender:node,nonce,members}){if data.len()<=1000{let _=connection.send_datagram(data.into());}}}}for(target,nonce) in lost {for peer in self.peers.lock().await.values().take(3){if let Ok(data)=encode(&Packet::PingRequest{database:db,sender:node,target,nonce}){let _=peer.connection.send_datagram(data.into());}}}}
    async fn gossip(&self) {let(db,node)=self.backend.identity();let members=self.view.lock().await.gossip();let data=match encode(&Packet::Gossip{database:db,sender:node,members}) {Ok(b) if b.len()<=1000=>b,_=>return};use rand::seq::IteratorRandom;let peers=self.peers.lock().await;for peer in peers.values().choose_multiple(&mut rand::thread_rng(),3){let _=peer.connection.send_datagram(data.clone().into());}}
    async fn packet(&self,id:NodeId,connection:&quinn::Connection,data:&[u8])->Result<()> {
        if transport::peer_id(connection,&self.config)?!=id{return Err(Error::Auth("membership identity".into()));}let packet:Packet=decode(data)?;let(db,node)=self.backend.identity();
        match packet {Packet::Gossip{database,sender,members}=>{if database!=db || sender!=id || members.len()>4{return Err(Error::Auth("membership envelope".into()));}let mut view=self.view.lock().await;for member in members{view.merge(member);}},Packet::IndirectAck{database,sender,target,nonce}=>{if database!=db || sender!=id{return Err(Error::Auth("indirect acknowledgement".into()));}self.view.lock().await.acknowledged(target,nonce);},Packet::Ping{database,sender,nonce,members}|Packet::Ack{database,sender,nonce,members}=>{if database!=db || sender!=id || members.len()>4{return Err(Error::Auth("membership envelope".into()));}let mut view=self.view.lock().await;for member in members{view.merge(member);}view.acknowledged(id,nonce);let reply=view.gossip();drop(view);
                if let Some((requester,_))=self.relays.lock().await.remove(&(id,nonce)) {if let Some(peer)=self.peers.lock().await.get(&requester){let bytes=encode(&Packet::IndirectAck{database:db,sender:node,target:id,nonce})?;let _=peer.connection.send_datagram(bytes.into());}}
                if matches!(decode::<Packet>(data)?,Packet::Ping{..}){let bytes=encode(&Packet::Ack{database:db,sender:node,nonce,members:reply})?;if bytes.len()<=1000{let _=connection.send_datagram(bytes.into());}}},Packet::PingRequest{database,sender,target,nonce}=>{if database!=db || sender!=id{return Err(Error::Auth("membership envelope".into()));}{let mut relays=self.relays.lock().await;relays.retain(|_,(_,at)|at.elapsed()<Duration::from_secs(5));if relays.len()<4096 {relays.insert((target,nonce),(sender,std::time::Instant::now()));}}
                let relay=self.peers.lock().await.get(&target).map(|peer|peer.connection.clone());if let Some(peer)=relay{let bytes=encode(&Packet::Ping{database:db,sender:node,nonce,members:self.view.lock().await.gossip()})?;if bytes.len()<=1000{let _=peer.send_datagram(bytes.into());}}}}
        Ok(())
    }
    // Snapshot builds checkpoint and re-scan the spool (seconds under load),
    // so they run as background tasks: awaiting a build inline would stall
    // this connection's read loop until it finishes, wedging control
    // traffic exactly when catch-up needs it. Builds are paced
    // process-wide: colocated nodes racing to serve the same rejoin would
    // otherwise dogpile the CPU and slow every build several-fold. Queued
    // builds run their reuse check after waking, so losers reuse the
    // winner's committed build instead of rescanning. Failures are silent;
    // the requester retries damped.
    fn send_snapshot(&self,id:NodeId,send:&crate::queue::Sender) {
        let backend=self.backend.clone();let config=self.config.clone();let send=send.clone();
        eprintln!("[snap-donor] request from {:02x}{:02x}",id[0],id[1]);
        self.spawn(async move {
            static BUILD_GATE: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
            let Ok(_permit) = BUILD_GATE.get_or_init(|| tokio::sync::Semaphore::new(2)).acquire().await else { eprintln!("[snap-donor] gate closed");return };
            eprintln!("[snap-donor] building for {:02x}{:02x}",id[0],id[1]);
            let _=snapshot::maintain(backend.as_ref(),retention::now_millis());
            let manifest=tokio::task::spawn_blocking(move||backend.create_snapshot(id,&config)).await;
            let manifest=match manifest {Ok(r)=>r,Err(_)=>{eprintln!("[snap-donor] build panicked");return}};
            match manifest {Ok(m)=>{eprintln!("[snap-donor] manifest built chunks={} transfer={:02x}{:02x}",m.chunks.len(),m.transfer[0],m.transfer[1]);let _=send.try_send(Message::SnapshotManifest(m));}Err(e)=>{eprintln!("[snap-donor] build failed: {e}");}}
        });
    }
    /// SnapshotRequest with per-peer damping: missing history fires on every
    /// progress round, and undamped repeats make every donor rebuild and
    /// re-manifest continuously while the victim spawns a duplicate chunk
    /// serve task per manifest, clogging the data lane with duplicate
    /// chunks. First requests stay immediate (failover across donors is
    /// unaffected); repeats to the same peer wait 5s.
    async fn request_snapshot_damped(&self,id:NodeId,send:&crate::queue::Sender){
        let now=std::time::Instant::now();
        let mut peers=self.peers.lock().await;
        let send_it=match peers.get_mut(&id){
            Some(p)=>{let due=p.snapshot_req_at.is_none_or(|t|now.saturating_duration_since(t)>=Duration::from_secs(5));if due{p.snapshot_req_at=Some(now);}due}
            None=>true,
        };
        drop(peers);
        if send_it{let _=send.try_send(Message::SnapshotRequest);}
    }
    fn request_snapshot_chunks(&self,id:NodeId,transfer:NodeId,send:&crate::queue::Sender)->Result<()> {
        let now=retention::now_millis();
        if let Some(bytes)=self.backend.get(&snapshot::receipt_key(transfer))? {
            let receipt:snapshot::Incoming=decode(&bytes)?;
            if receipt.manifest.donor!=id {return Err(Error::Auth("snapshot receipt identity".into()));}
            return send.try_send(Message::SnapshotApplied{transfer,digest:receipt.manifest.digest});
        }
        let Some(r)=self.cached_incoming()? else{return Ok(());};
        if r.manifest.transfer!=transfer || r.manifest.donor!=id {return Err(Error::Auth("snapshot request identity".into()));}
        if r.manifest.schema!=schema_digest(&self.backend.schema()?) {
            send.try_send(Message::SnapshotCancel{transfer})?;
            self.backend.commit(vec![Record::delete(snapshot::header(snapshot::IN,transfer))])?;
            // Existing signed schema negotiation must complete before a fresh capture.
            let _=send.try_send(Message::SchemaRequest);
            return Ok(());
        }
        let missing=self.cached_missing(&r)?;
        let install_key=format!("{}{}/install",String::from_utf8_lossy(snapshot::IN),hex(&transfer)).into_bytes();let before=self.backend.get(&install_key)?;
        if !missing.is_empty() {
            eprintln!("[snap-rx] transfer {:02x}{:02x} missing {}/{} chunks",transfer[0],transfer[1],missing.len(),r.manifest.chunks.len());
            send.try_send(Message::SnapshotNeed{transfer,indexes:missing})?;
        }else if snapshot::finish(self.backend.as_ref(),&self.config,id,transfer,now)? {
            eprintln!("[snap-rx] transfer {:02x}{:02x} INSTALLED",transfer[0],transfer[1]);
            self.control.status.lock().unwrap().metrics.snapshots_applied+=1;
            send.try_send(Message::SnapshotApplied{transfer,digest:r.manifest.digest})?;
        }else if self.backend.get(&install_key)?!=before {self.apply_notify.notify_one();}
        Ok(())
    }
    async fn send_snapshot_chunks(&self,id:NodeId,transfer:NodeId,indexes:Vec<u32>,send:&crate::queue::Sender)->Result<()> {
        if indexes.is_empty() || indexes.len()>128 || indexes.iter().copied().collect::<BTreeSet<_>>().len()!=indexes.len() {return Err(Error::Protocol("snapshot request indexes".into()));}
        let Some(manifest)=snapshot::outgoing(self.backend.as_ref(),id,transfer,retention::now_millis())? else {
            return send.try_send(Message::SnapshotUnavailable{transfer});
        };
        if indexes.iter().any(|i|*i as usize>=manifest.chunks.len()) {return Err(Error::Protocol("snapshot chunk index".into()));}
        if !SNAPSHOT_SERVING.get_or_init(||std::sync::Mutex::new(BTreeSet::new())).lock().unwrap().insert(transfer) {
            return Ok(());
        }
        let serve_guard=ServeGuard(transfer);
        // Duplicate page requests or transaction transfers can temporarily use
        // the shared budget. The receiver's durable retry loop will ask again.
        let permit=match self.transfers.clone().try_acquire_owned(){Ok(p)=>p,Err(_)=>return Ok(())};
        let backend=self.backend.clone();let send=send.clone();
        self.spawn(async move {
            let _serve_guard=serve_guard;
            let _permit=permit;
            for index in indexes {
                match backend.get(&snapshot::chunk(snapshot::OUT,transfer,index)) {
                    Ok(Some(data))=>if send.send(Message::SnapshotChunk{transfer,index,data}).await.is_err(){return;},
                    _=>{let _=send.send(Message::SnapshotUnavailable{transfer}).await;return;}
                }
            }
            let _=send.send(Message::SnapshotDone{transfer}).await;
        });
        Ok(())
    }

}
/// Outcome of vetting a received schema manifest, without adopting it.
/// Every verified manifest is recorded (per signer) so payload filtering can
/// drop tables the sender has retired; only the configured master (or any
/// trusted origin in legacy mode) is adopted. Stale duplicates stay idempotent
/// success so retransmits never kill sessions.
#[derive(Debug,PartialEq,Eq)]
pub(crate) enum SchemaDecision { Current, Adopt, Stale, Record }
pub(crate) fn record_schema_accepted(backend:&dyn Backend,signed:&SignedSchema)->Result<()> {
    backend.commit(vec![Record::put(schema_accepted_key(signed.signer),encode(&(signed.sequence,signed.schema.clone()))?)])
}
pub(crate) fn check_schema_manifest(config:&Config,backend:&dyn Backend,signed:&SignedSchema)->Result<SchemaDecision> {
    let (database,node)=backend.identity();
    signed.verify(database,signed.signer,&config.trusted_origins,config.max_transaction_bytes)?;
    let adoptable=match config.schema_master {
        Some(m)=>signed.signer==m && !(config.master && config.schema_master==Some(node)),
        None=>true,
    };
    let key=schema_accepted_key(signed.signer);
    let accepted:(u64,Vec<u8>)=backend.get(&key)?.map(|b|decode(&b)).transpose()?.unwrap_or((0,Vec::new()));
    if signed.sequence<=accepted.0 { return Ok(SchemaDecision::Stale); }
    if signed.schema==backend.schema()? {
        record_schema_accepted(backend,signed)?;
        return Ok(SchemaDecision::Current);
    }
    if !adoptable || !config.accept_remote_schema {
        // Recorded for filtering, never adopted.
        record_schema_accepted(backend,signed)?;
        return Ok(SchemaDecision::Record);
    }
    Ok(SchemaDecision::Adopt)
}
fn proof(tx:&Transaction)->Transaction {let mut header=tx.clone();header.payload.clear();header}
pub(crate) fn parse_id(s:&str)->Result<NodeId>{if s.len()!=32 || !s.is_ascii(){return Err(Error::Protocol("node id".into()));}let mut id=[0;16];for(i,b)in id.iter_mut().enumerate(){*b=u8::from_str_radix(&s[i*2..i*2+2],16).map_err(|_|Error::Protocol("node id".into()))?;}Ok(id)}

fn compatible_caps(capabilities:u64,required:u64)->bool {required & !CAPABILITIES==0 && capabilities & REQUIRED==REQUIRED}
/// Retransmit unacked ranges once pushing an origin goes quiet for this
/// long. `sent` dedupes the stream while new transactions flow, but without
/// a resend timer a follower that fell behind would only ever trickle back
/// via 10 s anti-entropy Needs (minutes of tail for seconds of backlog).
const RESEND_AFTER_QUIET:std::time::Duration=std::time::Duration::from_secs(5);
/// Cap unacknowledged in-flight sequences per origin per peer. Without
/// backpressure the sender floods a slow joiner with its whole unacked
/// range; fresh transactions then queue behind hundreds of stale
/// duplicates and catch-up stalls for seconds per cycle. The window holds
/// ~6s of catch-up flow at measured apply rates; quiet resends keep the
/// head (acked+1..) fresh while in-flight-full.
const PUSH_INFLIGHT_WINDOW:u64=1024;
const SNAP_BULK_LAG_TX:u64=1000;
const SNAP_BULK_LAG_STABLE:std::time::Duration=std::time::Duration::from_secs(30);
/// Next sequence to push for an origin: past the sent frontier while the
/// stream advances (dedup), back at the acked frontier once quiet (ARQ).
// Sorted durable heads, rotated independently for each peer. A bounded round
// must not let permanently backlogged early origins consume every batch slot.
fn push_order(progress:&[Progress],after:Option<NodeId>)->impl Iterator<Item=&Progress> {
    let split=after.map_or(0,|origin|progress.partition_point(|p|p.origin<=origin));
    progress[split..].iter().chain(progress[..split].iter())
}
fn push_quiet(sent_at:Option<std::time::Instant>,now:std::time::Instant)->bool {
    sent_at.map(|at|now.saturating_duration_since(at)>RESEND_AFTER_QUIET).unwrap_or(true)
}
fn push_from(acked:u64,sent:u64,sent_at:Option<std::time::Instant>,now:std::time::Instant)->u64 {
    if push_quiet(sent_at,now) {acked} else {acked.max(sent)}.saturating_add(1)
}

#[cfg(test)]
mod schema_tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn continuously_backlogged_origins_share_bounded_push_rounds() {
        // Ten origins with endless work, sixteen available transactions each,
        // and sixty-four slots: restarting at origin zero starves six origins.
        let progress:Vec<_>=(0..10u8).map(|i|Progress{origin:[i;16],applied:10_000,
            observed:10_000,retained_from:1,retained_through:10_000}).collect();
        let mut cursor=None;let mut counts=BTreeMap::new();
        for _ in 0..10 {
            let served:Vec<_>=push_order(&progress,cursor).take(4).map(|p|p.origin).collect();
            for origin in &served {*counts.entry(*origin).or_insert(0)+=1;}
            cursor=served.last().copied();
        }
        assert_eq!(counts.len(),10);
        assert!(counts.values().all(|count|*count==4));
        // A second peer begins independently; retired/missing cursor origins
        // and wraparound must not skip a retained head.
        assert_eq!(push_order(&progress,None).next().unwrap().origin,[0;16]);
        assert_eq!(push_order(&progress,Some([255;16])).next().unwrap().origin,[0;16]);
        assert_eq!(push_order(&progress,Some([4;16])).next().unwrap().origin,[5;16]);
        assert_eq!(push_order(&[],Some([4;16])).count(),0);
    }

    struct Stub {
        db: NodeId,
        node: NodeId,
        store: std::sync::Mutex<HashMap<Vec<u8>, Vec<u8>>>,
        schema: Vec<u8>,
        commits: std::sync::atomic::AtomicUsize,
        reads: std::sync::atomic::AtomicUsize,
        clocks: Mutex<Vec<(u64,u32)>>,
    }
    impl Stub {
        fn new(node: NodeId, schema: Vec<u8>) -> Self {
            Self {
                db: [7; 16],
                node,
                store: std::sync::Mutex::new(HashMap::new()),
                schema,
                commits: std::sync::atomic::AtomicUsize::new(0),
                reads: std::sync::atomic::AtomicUsize::new(0),
                clocks: Mutex::new(Vec::new()),
            }
        }
    }
    impl Backend for Stub {
        fn identity(&self) -> (NodeId, NodeId) {
            (self.db, self.node)
        }
        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.reads.fetch_add(1,Ordering::Relaxed);
            Ok(self.store.lock().unwrap().get(key).cloned())
        }
        fn keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
            Ok(self
                .store
                .lock()
                .unwrap()
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect())
        }
        fn commit(&self, records: Vec<Record>) -> Result<()> {
            self.commits.fetch_add(1,Ordering::Relaxed);
            let mut store = self.store.lock().unwrap();
            for r in records {
                match r.value {
                    Some(v) => {
                        store.insert(r.key, v);
                    }
                    None => {
                        store.remove(&r.key);
                    }
                }
            }
            Ok(())
        }
        fn import_bridge(&self, _h: crate::bridge::Header, _p: Vec<u8>) -> Result<bool> {
            Ok(false)
        }
        fn release_ownership(&self, _t: String, _k: Vec<u8>, _c: Option<String>) -> Result<bool> {
            Ok(false)
        }
        fn publish(&self, _r: Vec<Record>) -> Result<()> {
            Ok(())
        }
        fn snapshot(&self) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }
        fn snapshot_with_progress(&self) -> Result<(Vec<u8>, Vec<crate::protocol::Progress>)> {
            Ok((Vec::new(), Vec::new()))
        }
        fn retention_checkpoint(&self) -> Result<()> {
            Ok(())
        }
        fn prune_history(&self, _o: NodeId, _t: u64) -> Result<u64> {
            Ok(0)
        }
        fn reclaim_retention(&self) -> Result<()> {
            Ok(())
        }
        fn schema(&self) -> Result<Vec<u8>> {
            Ok(self.schema.clone())
        }
        fn apply(&self, _p: Vec<Vec<u8>>, records: Vec<Record>) -> Result<bool> {
            self.commit(records)?;Ok(true)
        }
        fn observe(&self, m: u64, l: u32) -> Result<()> {
            self.clocks.lock().unwrap().push((m,l));
            Ok(())
        }
        fn accept_schema(&self, _s: Vec<u8>) -> Result<bool> {
            Ok(true)
        }
    }

    const MASTER_SEED: [u8; 32] = [11; 32];
    const OTHER_SEED: [u8; 32] = [22; 32];
    const MASTER: NodeId = [1; 16];
    const OTHER: NodeId = [2; 16];

    fn trust() -> BTreeMap<NodeId, [u8; 32]> {
        BTreeMap::from([
            (MASTER, crate::protocol::public_key(&MASTER_SEED)),
            (OTHER, crate::protocol::public_key(&OTHER_SEED)),
        ])
    }
    fn config(node_trust: BTreeMap<NodeId, [u8; 32]>) -> Config {
        let mut c = Config::new(
            "127.0.0.1:0".parse().unwrap(),
            Credentials {
                chain: vec![vec![1]],
                private_key: zeroize::Zeroizing::new(vec![]),
                roots: vec![vec![2]],
            },
            OTHER_SEED,
        );
        c.trusted_origins = node_trust;
        c
    }
    fn manifest(seed: &[u8; 32], signer: NodeId, sequence: u64, schema: Vec<u8>) -> SignedSchema {
        let mut m = SignedSchema {
            database: [7; 16],
            signer,
            sequence,
            schema,
            signature: Vec::new(),
        };
        m.sign(seed).unwrap();
        m
    }

    #[test]
    fn push_from_dedupes_while_streaming_and_retransmits_once_quiet() {
        let now = std::time::Instant::now();
        let fresh = now - Duration::from_secs(1);
        let stale = now - Duration::from_secs(60);
        // Streaming: past the sent frontier (dedup while new data flows).
        assert_eq!(push_from(100, 150, Some(fresh), now), 151);
        // Quiet: back at the acked frontier (retransmit the backlog).
        assert_eq!(push_from(100, 150, Some(stale), now), 101);
        // Never pushed: start past the acked frontier.
        assert_eq!(push_from(100, 0, None, now), 101);
        // Caught up: next sequence either way.
        assert_eq!(push_from(150, 150, Some(fresh), now), 151);
        assert_eq!(push_from(150, 150, Some(stale), now), 151);
        // Peer ahead via another path: follow the ack, not the sent mark.
        assert_eq!(push_from(160, 150, Some(fresh), now), 161);
    }

    #[test]
    fn envelope_verify_accepts_master_and_rejects_forgery() {
        let trust = trust();
        let good = manifest(&MASTER_SEED, MASTER, 3, b"schema-bytes".to_vec());
        assert!(good.verify([7; 16], MASTER, &trust, 64 << 20).is_ok());
        // Tampered payload.
        let mut bad = good.clone();
        bad.schema.push(b'x');
        assert!(bad.verify([7; 16], MASTER, &trust, 64 << 20).is_err());
        // Wrong expected master, wrong database, unknown signer, zero seq, oversize.
        assert!(good.verify([7; 16], OTHER, &trust, 64 << 20).is_err());
        assert!(good.verify([8; 16], MASTER, &trust, 64 << 20).is_err());
        assert!(
            good.verify([7; 16], MASTER, &BTreeMap::new(), 64 << 20)
                .is_err()
        );
        let mut zero = good.clone();
        zero.sequence = 0;
        zero.sign(&MASTER_SEED).unwrap();
        assert!(zero.verify([7; 16], MASTER, &trust, 64 << 20).is_err());
        assert!(good.verify([7; 16], MASTER, &trust, 4).is_err());
    }

    #[test]
    fn check_adopts_current_master_and_dedups_replays() {
        let mut c = config(trust());
        c.schema_master = Some(MASTER);
        let backend = Stub::new(OTHER, b"old".to_vec());
        let fresh = manifest(&MASTER_SEED, MASTER, 1, b"new".to_vec());
        assert_eq!(
            check_schema_manifest(&c, &backend, &fresh).unwrap(),
            SchemaDecision::Adopt
        );
        // Record acceptance like the handler does after a successful adopt.
        record_schema_accepted(&backend, &fresh).unwrap();
        assert_eq!(
            check_schema_manifest(&c, &backend, &fresh).unwrap(),
            SchemaDecision::Stale
        );
        // Newer sequence, already-converged bytes: recorded without adopting.
        let converged = Stub::new(OTHER, b"new".to_vec());
        record_schema_accepted(&converged, &fresh).unwrap();
        let newer = manifest(&MASTER_SEED, MASTER, 2, b"new".to_vec());
        assert_eq!(
            check_schema_manifest(&c, &converged, &newer).unwrap(),
            SchemaDecision::Current
        );
        assert_eq!(
            check_schema_manifest(&c, &converged, &newer).unwrap(),
            SchemaDecision::Stale
        );
    }

    #[test]
    fn check_records_but_never_adopts_non_master_signers() {
        let mut c = config(trust());
        c.schema_master = Some(MASTER);
        let backend = Stub::new(OTHER, b"old".to_vec());
        // Signed by a trusted node that is not the master: recorded for
        // payload filtering, never adopted.
        let rogue = manifest(&OTHER_SEED, OTHER, 1, b"new".to_vec());
        assert_eq!(
            check_schema_manifest(&c, &backend, &rogue).unwrap(),
            SchemaDecision::Record
        );
        // The recording holds the signer's declared bytes for filtering.
        let raw = backend.get(&schema_accepted_key(OTHER)).unwrap().unwrap();
        let (seq, bytes): (u64, Vec<u8>) = decode(&raw).unwrap();
        assert_eq!((seq, bytes), (1, b"new".to_vec()));
        assert_eq!(
            check_schema_manifest(&c, &backend, &rogue).unwrap(),
            SchemaDecision::Stale
        );
        // The master itself never adopts remote schemas either.
        let mut master_cfg = config(trust());
        master_cfg.master = true;
        master_cfg.schema_master = Some(MASTER);
        let master_backend = Stub::new(MASTER, b"old".to_vec());
        let m = manifest(&MASTER_SEED, MASTER, 9, b"new".to_vec());
        assert_eq!(
            check_schema_manifest(&master_cfg, &master_backend, &m).unwrap(),
            SchemaDecision::Record
        );
        // Forged bytes still fail closed.
        let mut forged = manifest(&MASTER_SEED, MASTER, 10, b"new".to_vec());
        forged.schema.push(b'x');
        assert!(matches!(
            check_schema_manifest(&c, &backend, &forged),
            Err(Error::Auth(_))
        ));
    }

    #[test]
    fn check_legacy_mode_accepts_any_trusted_origin() {
        let c = config(trust()); // schema_master: None
        let backend = Stub::new(OTHER, b"old".to_vec());
        let m = manifest(&OTHER_SEED, OTHER, 1, b"new".to_vec());
        assert_eq!(
            check_schema_manifest(&c, &backend, &m).unwrap(),
            SchemaDecision::Adopt
        );
        let stranger = manifest(&[33; 32], [3; 16], 1, b"new".to_vec());
        assert!(matches!(
            check_schema_manifest(&c, &backend, &stranger),
            Err(Error::Auth(_))
        ));
    }

    #[tokio::test]
    async fn duplicate_floor_discards_old_signed_transactions_and_chunks_without_staging() {
        let backend = Arc::new(Stub::new(OTHER, vec![]));
        backend
            .commit(vec![Record::put(
                retention::floor_key(MASTER),
                encode(&7u64).unwrap(),
            )])
            .unwrap();
        let endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = endpoint.local_addr().unwrap();
        let (commands, _) = mpsc::channel(1);
        let core = Core {
            config: config(trust()),
            backend: backend.clone(),
            endpoint,
            files: None,
            control: Arc::new(Control {
                backend:backend.clone(),
                stop: AtomicBool::new(false),
                status: Mutex::new(Status::default()),
                commands,
                thread: Mutex::new(None),
                shutdown: tokio::sync::watch::channel(false).0,
                objects: None,
                bridge: None,
            }),
            peers: tokio::sync::Mutex::new(BTreeMap::new()),
            view: tokio::sync::Mutex::new(View::new(Member {
                node: OTHER,
                address,
                incarnation: 0,
                state: State::Alive,
            })),
            sessions: Arc::new(Semaphore::new(1)),
            relays: tokio::sync::Mutex::new(BTreeMap::new()),
            stage_gate: tokio::sync::Mutex::new(()), stage_commit: tokio::sync::Mutex::new(()), retained_gate: tokio::sync::Mutex::new(()),
            retention_gate: tokio::sync::Mutex::new(()),
            queue: Arc::new(Semaphore::new(1)),
            control_queue: Arc::new(Semaphore::new(1<<20)),
            tasks:Mutex::new(Vec::new()),apply_notify:tokio::sync::Notify::new(),staged_sizes:tokio::sync::Mutex::new(None),last_origin:Mutex::new([0;16]),incoming_hint:Mutex::new(None),incoming_none_at:Mutex::new(None),missing_cache:Mutex::new(None),adaptive_batch:std::sync::atomic::AtomicUsize::new(usize::MAX),verification:Mutex::new(crate::verification::Cache::default()),
            transfers: Arc::new(Semaphore::new(1)),
            schema_req_at: tokio::sync::Mutex::new(None),
            chunk_counters: tokio::sync::Mutex::new(None),
            transfers_active: Arc::new(tokio::sync::Mutex::new(BTreeSet::new())),
            chunk_seen: tokio::sync::Mutex::new(BTreeMap::new()),
            chunk_holders: tokio::sync::Mutex::new(BTreeMap::new()),
            pending_admit: tokio::sync::Mutex::new(BTreeMap::new()),
            admit_removed: tokio::sync::Mutex::new(BTreeMap::new()),
            admit_seq: std::sync::atomic::AtomicU64::new(1),
            need_at: Mutex::new(BTreeMap::new()),
            outgoing_cache: Mutex::new(OutgoingShared::default()),
        };
        for sequence in [1, 7] {
            let mut tx = Transaction {
                database: [7; 16],
                origin: MASTER,
                sequence,
                id: [8; 16],
                millis: 10,
                logical: 0,
                schema: [0; 32],
                payload_digest: [0; 32],
                payload: vec![1, 2, 3],
                signature: vec![],
            };
            tx.sign(&MASTER_SEED).unwrap();
            core.stage(tx.clone()).await.unwrap();
            let data = encode(&tx).unwrap();
            let digest = Sha256::digest(&data).into();
            core.stage_chunk(Chunk {
                proof: proof(&tx),
                origin: MASTER,
                sequence,
                id: tx.id,
                index: 0,
                count: 1,
                total: data.len() as u64,
                digest,
                data,
            })
            .await
            .unwrap();
        }
        assert!(backend.keys(b"07/replication/stage/").unwrap().is_empty());
        assert!(backend.keys(b"07/replication/chunks/").unwrap().is_empty());
        assert_eq!(core.control.status.lock().unwrap().applied_transactions, 0);
    }

    fn chunk_repair_core(backend: Arc<Stub>) -> Core {
        let endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = endpoint.local_addr().unwrap();
        let (commands, _) = mpsc::channel(1);
        Core {
            config: config(trust()),
            backend: backend.clone(),
            endpoint,
            files: None,
            control: Arc::new(Control {
                backend:backend.clone(),
                stop: AtomicBool::new(false),
                status: Mutex::new(Status::default()),
                commands,
                thread: Mutex::new(None),
                shutdown: tokio::sync::watch::channel(false).0,
                objects: None,
                bridge: None,
            }),
            peers: tokio::sync::Mutex::new(BTreeMap::new()),
            view: tokio::sync::Mutex::new(View::new(Member {
                node: OTHER,
                address,
                incarnation: 0,
                state: State::Alive,
            })),
            sessions: Arc::new(Semaphore::new(1)),
            relays: tokio::sync::Mutex::new(BTreeMap::new()),
            stage_gate: tokio::sync::Mutex::new(()), stage_commit: tokio::sync::Mutex::new(()), retained_gate: tokio::sync::Mutex::new(()),
            retention_gate: tokio::sync::Mutex::new(()),
            queue: Arc::new(Semaphore::new(1)),
            control_queue: Arc::new(Semaphore::new(1<<20)),
            tasks:Mutex::new(Vec::new()),apply_notify:tokio::sync::Notify::new(),staged_sizes:tokio::sync::Mutex::new(None),last_origin:Mutex::new([0;16]),incoming_hint:Mutex::new(None),incoming_none_at:Mutex::new(None),missing_cache:Mutex::new(None),adaptive_batch:std::sync::atomic::AtomicUsize::new(usize::MAX),verification:Mutex::new(crate::verification::Cache::default()),
            transfers: Arc::new(Semaphore::new(1)),
            schema_req_at: tokio::sync::Mutex::new(None),
            chunk_counters: tokio::sync::Mutex::new(None),
            transfers_active: Arc::new(tokio::sync::Mutex::new(BTreeSet::new())),
            chunk_seen: tokio::sync::Mutex::new(BTreeMap::new()),
            chunk_holders: tokio::sync::Mutex::new(BTreeMap::new()),
            pending_admit: tokio::sync::Mutex::new(BTreeMap::new()),
            admit_removed: tokio::sync::Mutex::new(BTreeMap::new()),
            admit_seq: std::sync::atomic::AtomicU64::new(1),
            need_at: Mutex::new(BTreeMap::new()),
            outgoing_cache: Mutex::new(OutgoingShared::default()),
        }
    }

    fn signed_chunked_tx(sequence: u64, payload_len: usize) -> (Transaction, Vec<u8>) {
        let mut tx = Transaction {
            database: [7; 16],
            origin: MASTER,
            sequence,
            id: [8; 16],
            millis: 10,
            logical: 0,
            schema: [0; 32],
            payload_digest: [0; 32],
            payload: vec![0xABu8; payload_len],
            signature: vec![],
        };
        tx.sign(&MASTER_SEED).unwrap();
        let data = encode(&tx).unwrap();
        (tx, data)
    }

    fn chunk_message(tx: &Transaction, data: &[u8], index: u32) -> Message {
        let count = data.len().div_ceil(crate::protocol::CHUNK_BYTES) as u32;
        let start = index as usize * crate::protocol::CHUNK_BYTES;
        Message::TransactionChunk(Chunk {
            proof: proof(tx),
            origin: tx.origin,
            sequence: tx.sequence,
            id: tx.id,
            index,
            count,
            total: data.len() as u64,
            digest: Sha256::digest(data).into(),
            data: data[start..(start + crate::protocol::CHUNK_BYTES).min(data.len())].to_vec(),
        })
    }

    #[tokio::test]
    async fn serve_chunk_need_from_complete_log() {
        let backend = Arc::new(Stub::new(OTHER, vec![]));
        let (tx, data) = signed_chunked_tx(3, 200_000);
        backend
            .commit(vec![Record::put(log_key(MASTER, 3), data.clone())])
            .unwrap();
        let core = chunk_repair_core(backend);
        let (send, mut recv) = crate::queue::Sender::new(Arc::new(Semaphore::new(64 << 20)), 4 << 20);
        core.serve_chunk_need(MASTER, 3, vec![0, 2], &send).await.unwrap();
        let mut got = Vec::new();
        while let Ok(q) = recv.try_recv() {
            let Message::TransactionChunk(c) = q.message else {
                panic!("chunk message");
            };
            got.push((c.index, c.data.len()));
        }
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0, 0);
        assert_eq!(got[1].0, 2);
        assert_eq!(core.control.status.lock().unwrap().metrics.chunk_need_served, 2);
        // Out-of-range index is a protocol error.
        let count = data.len().div_ceil(crate::protocol::CHUNK_BYTES) as u32;
        let err = core
            .serve_chunk_need(MASTER, 3, vec![count], &send)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
        let _ = tx;
    }

    #[tokio::test]
    async fn serve_chunk_need_relays_staged_chunks() {
        let backend = Arc::new(Stub::new(OTHER, vec![]));
        let (tx, data) = signed_chunked_tx(5, 200_000);
        let count = data.len().div_ceil(crate::protocol::CHUNK_BYTES) as u32;
        let digest: [u8; 32] = Sha256::digest(&data).into();
        let manifest = ChunkManifest {
            id: tx.id,
            count,
            total: data.len() as u64,
            digest,
        };
        // Stage only chunks 0 and 2 (index 1 missing).
        let mut records = vec![Record::put(chunk_manifest_key(MASTER, 5), encode(&manifest).unwrap())];
        for index in [0u32, 2] {
            let Message::TransactionChunk(c) = chunk_message(&tx, &data, index) else {
                panic!("chunk message");
            };
            records.push(Record::put(chunk_key(MASTER, 5, index), encode(&c).unwrap()));
        }
        backend.commit(records).unwrap();
        let core = chunk_repair_core(backend);
        let (send, mut recv) = crate::queue::Sender::new(Arc::new(Semaphore::new(64 << 20)), 4 << 20);
        core.serve_chunk_need(MASTER, 5, vec![0, 1, 2], &send)
            .await
            .unwrap();
        let mut got = Vec::new();
        while let Ok(q) = recv.try_recv() {
            let Message::TransactionChunk(c) = q.message else {
                panic!("chunk message");
            };
            got.push(c.index);
        }
        assert_eq!(got, vec![0, 2], "present chunks served, missing skipped");
        assert_eq!(core.control.status.lock().unwrap().metrics.chunk_need_served, 2);
    }

    #[tokio::test]
    async fn serve_chunk_need_missing_is_quiet() {
        let backend = Arc::new(Stub::new(OTHER, vec![]));
        let core = chunk_repair_core(backend);
        let (send, mut recv) = crate::queue::Sender::new(Arc::new(Semaphore::new(64 << 20)), 4 << 20);
        core.serve_chunk_need(MASTER, 9, vec![0], &send).await.unwrap();
        assert!(recv.try_recv().is_err(), "quiet miss sends nothing");
        assert_eq!(core.control.status.lock().unwrap().metrics.chunk_need_served, 0);
    }

    #[tokio::test]
    async fn serve_chunk_availability_lists_staged_holdings() {
        let backend = Arc::new(Stub::new(OTHER, vec![]));
        let (tx, data) = signed_chunked_tx(5, 200_000);
        let count = data.len().div_ceil(crate::protocol::CHUNK_BYTES) as u32;
        let digest: [u8; 32] = Sha256::digest(&data).into();
        let manifest = ChunkManifest {
            id: tx.id,
            count,
            total: data.len() as u64,
            digest,
        };
        let mut records = vec![Record::put(chunk_manifest_key(MASTER, 5), encode(&manifest).unwrap())];
        for index in [0u32, 3] {
            let Message::TransactionChunk(c) = chunk_message(&tx, &data, index) else {
                panic!("chunk message");
            };
            records.push(Record::put(chunk_key(MASTER, 5, index), encode(&c).unwrap()));
        }
        backend.commit(records).unwrap();
        let core = chunk_repair_core(backend);
        let (send, mut recv) = crate::queue::Sender::new(Arc::new(Semaphore::new(64 << 20)), 4 << 20);
        core.serve_chunk_availability(0, &send).await.unwrap();
        let page = recv.try_recv().unwrap();
        let Message::ChunkAvailabilityPage { entries, more } = page.message else {
            panic!("availability page");
        };
        assert!(!more);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, MASTER);
        assert_eq!(entries[0].1, 5);
        assert_eq!(entries[0].2, vec![0, 3]);
    }

    #[tokio::test]
    async fn merge_chunk_availability_enforces_caps() {
        let backend = Arc::new(Stub::new(OTHER, vec![]));
        let core = chunk_repair_core(backend);
        core.merge_chunk_availability(OTHER, vec![(MASTER, 5, vec![0, 1])])
            .await
            .unwrap();
        let holders = core.chunk_holders.lock().await;
        assert_eq!(
            holders.get(&(MASTER, 5)).unwrap().0.get(&OTHER).unwrap(),
            &BTreeSet::from([0, 1])
        );
        drop(holders);
        let too_many: Vec<(NodeId, u64, Vec<u32>)> =
            (0..65).map(|i| (MASTER, i, vec![0])).collect();
        let err = core
            .merge_chunk_availability(OTHER, too_many)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Overloaded));
        let too_wide = vec![(MASTER, 5, vec![0u32; 4097])];
        let err = core
            .merge_chunk_availability(OTHER, too_wide)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Overloaded));
    }

    #[tokio::test]
    async fn bulk_staging_does_not_rewrite_observed_heads_for_duplicate_origins() {
        let backend=Arc::new(Stub::new(OTHER,vec![]));let core=chunk_repair_core(backend.clone());
        let (old,_)=signed_chunked_tx(1,32);backend.commit(vec![Record::put(receipt_key(MASTER,1),old.digest().unwrap().to_vec())]).unwrap();
        let (mut fresh,_)=signed_chunked_tx(1,32);fresh.origin=OTHER;fresh.sign(&OTHER_SEED).unwrap();
        core.stage_batch(vec![old,fresh]).await.unwrap();
        assert!(backend.get(&observed_key(MASTER)).unwrap().is_none());
        assert_eq!(decode::<u64>(&backend.get(&observed_key(OTHER)).unwrap().unwrap()).unwrap(),1);
    }
    #[tokio::test]
    async fn outgoing_round_reuses_reads_decoding_and_wire_frames() {
        let backend=Arc::new(Stub::new(OTHER,vec![]));let seqs=(1..=16).collect::<Vec<_>>();
        for seq in &seqs{let(_,data)=signed_chunked_tx(*seq,256);backend.commit(vec![Record::put(log_key(MASTER,*seq),data)]).unwrap();}
        let core=chunk_repair_core(backend.clone());
        let first=core.load_outgoing(MASTER,&seqs).unwrap();let reads=backend.reads.load(Ordering::Relaxed);
        let second=core.load_outgoing(MASTER,&seqs).unwrap();assert_eq!(backend.reads.load(Ordering::Relaxed),reads);
        for (a,b) in first.iter().zip(&second){assert!(Arc::ptr_eq(a.as_ref().unwrap(),b.as_ref().unwrap()));}
        let mut round=OutgoingRound::default();
        let message=Message::Batches(first.into_iter().map(|v|(*v.unwrap().tx).clone()).collect());let keys=seqs.iter().map(|seq|(MASTER,*seq)).collect::<Vec<_>>();
        let a=round.frame(keys.clone(),&message).unwrap();let b=round.frame(keys,&message).unwrap();assert!(Arc::ptr_eq(&a,&b));assert_eq!(a.bytes,Prepared::new(&message).unwrap().bytes);
        assert!(core.outgoing_cache.lock().unwrap().bytes<=OUTGOING_SHARED_BYTES&&round.frame_bytes<=OUTGOING_CACHE_BYTES);
    }
    #[tokio::test]
    async fn outgoing_reads_are_bounded_and_allow_one_oversized_transaction() {
        let backend=Arc::new(Stub::new(OTHER,vec![]));
        for seq in 1..=3{let(_,data)=signed_chunked_tx(seq,4<<20);backend.commit(vec![Record::put(log_key(MASTER,seq),data)]).unwrap();}
        let core=chunk_repair_core(backend.clone());
        let values=core.load_outgoing(MASTER,&[1,2,3]).unwrap();assert!(values[0].is_some());assert!(values[1].is_none()&&values[2].is_none());assert!(core.outgoing_cache.lock().unwrap().values.is_empty());
    }
    #[test]
    fn outgoing_shared_cache_evicts_fifo_within_byte_bound() {
        let mut shared=OutgoingShared::default();
        let now=std::time::Instant::now();
        let mk=|seq:u64|Arc::new(Outgoing{data:Arc::new(vec![0;1024]),tx:Arc::new(signed_chunked_tx(seq,0).0)});
        let mut seq=0u64;
        while shared.bytes<=OUTGOING_SHARED_BYTES {
            seq+=1;
            shared.insert(([9;16],seq),mk(seq),2048,now);
            if seq>100000{break;}
        }
        assert!(shared.bytes<=OUTGOING_SHARED_BYTES);
        assert!(shared.get(&([9;16],1),now).is_none());
        assert!(shared.get(&([9;16],seq),now).is_some());
        let stale=now+OUTGOING_SHARED_TTL+std::time::Duration::from_secs(1);
        assert!(shared.get(&([9;16],seq),stale).is_none());
    }
    fn benchmark_cpu_ns()->Option<u128> {
        #[cfg(all(target_os="linux",target_pointer_width="64"))]
        {#[repr(C)]struct Ts{seconds:std::ffi::c_long,nanos:std::ffi::c_long}
        unsafe extern "C"{fn clock_gettime(clock:std::ffi::c_int,time:*mut Ts)->std::ffi::c_int;}
        let mut t=Ts{seconds:0,nanos:0};if unsafe{clock_gettime(2,&mut t)}==0{return Some(t.seconds as u128*1_000_000_000+t.nanos as u128);}}
        None
    }
    #[test]
    #[ignore = "release component benchmark, not distributed scalability acceptance"]
    fn outgoing_reuse_benchmark() {
        let backend=Arc::new(Stub::new(OTHER,vec![]));let seqs=(1..=64).collect::<Vec<_>>();let keys=seqs.iter().map(|seq|(MASTER,*seq)).collect::<Vec<_>>();
        for seq in &seqs{let(_,data)=signed_chunked_tx(*seq,256);backend.commit(vec![Record::put(log_key(MASTER,*seq),data)]).unwrap();}
        let core=chunk_repair_core(backend.clone());
        let rounds=200;let peers=4;backend.reads.store(0,Ordering::Relaxed);let cpu_start=benchmark_cpu_ns();let start=std::time::Instant::now();let mut reference_bytes=0usize;
        for _ in 0..rounds{for _ in 0..peers{let values=backend.get_many(&keys.iter().map(|(o,s)|log_key(*o,*s)).collect::<Vec<_>>()).unwrap();let message=Message::Batches(values.into_iter().map(|v|decode::<Transaction>(&v.unwrap()).unwrap()).collect());std::hint::black_box(encode(&message).unwrap());reference_bytes+=std::hint::black_box(Prepared::new(&message).unwrap()).bytes.len();}}
        let before=start.elapsed();let before_cpu=cpu_start.zip(benchmark_cpu_ns()).map(|(a,b)|b-a);let before_reads=backend.reads.load(Ordering::Relaxed);backend.reads.store(0,Ordering::Relaxed);let cpu_start=benchmark_cpu_ns();let start=std::time::Instant::now();let mut reuse_bytes=0usize;
        for _ in 0..rounds{let mut round=OutgoingRound::default();for _ in 0..peers{let values=core.load_outgoing(MASTER,&seqs).unwrap();let message=Message::Batches(values.into_iter().map(|v|(*v.unwrap().tx).clone()).collect());reuse_bytes+=std::hint::black_box(round.frame(keys.clone(),&message).unwrap()).bytes.len();}}
        let after=start.elapsed();let after_cpu=cpu_start.zip(benchmark_cpu_ns()).map(|(a,b)|b-a);let after_reads=backend.reads.load(Ordering::Relaxed);assert_eq!(reference_bytes,reuse_bytes);assert_eq!(before_reads,after_reads*rounds*peers);
        eprintln!("outgoing_reuse rounds={rounds} peers={peers} transactions=64 payload_bytes=256 before_ms={:.3} after_ms={:.3} before_cpu_ns={before_cpu:?} after_cpu_ns={after_cpu:?} before_reads={before_reads} after_reads={after_reads} speedup={:.3}",before.as_secs_f64()*1000.,after.as_secs_f64()*1000.,before.as_secs_f64()/after.as_secs_f64());
    }
    #[test]
    fn select_holders_round_robin_with_orphan_fallback() {
        let holders = BTreeMap::from([
            ([1; 16], BTreeSet::from([0, 1, 2])),
            ([2; 16], BTreeSet::from([1, 2, 3])),
        ]);
        let (directed, orphaned) = Core::select_holders(&[0, 1, 2, 3, 9], &holders);
        // 0: only [1] has it. 1,2: both have them (round-robin by
        // position). 3: only [2]. 9: nobody.
        assert_eq!(directed.get(&[1; 16]).unwrap(), &vec![0, 2]);
        assert_eq!(directed.get(&[2; 16]).unwrap(), &vec![1, 3]);
        assert_eq!(orphaned, vec![9]);
    }
    #[tokio::test]
    async fn missing_origin_sequence_does_not_scan_its_backlog_or_block_other_origins() {
        let backend=Arc::new(Stub::new(OTHER,vec![]));let core=chunk_repair_core(backend.clone());
        let mut records=Vec::new();
        for sequence in 2..=101 {let(tx,bytes)=signed_chunked_tx(sequence,10);records.push(Record::put(stage_key(tx.origin,sequence),bytes));}
        let (mut healthy,_)=signed_chunked_tx(1,10);healthy.origin=OTHER;healthy.sign(&OTHER_SEED).unwrap();
        records.push(Record::put(stage_key(OTHER,1),encode(&healthy).unwrap()));backend.commit(records).unwrap();
        core.staged().await.unwrap();backend.reads.store(0,Ordering::Relaxed);
        assert!(core.apply_pending().await.unwrap());
        assert!(backend.reads.load(Ordering::Relaxed)<12,"gap caused reads proportional to blocked backlog");
        assert_eq!(decode::<u64>(&backend.get(&progress_key(OTHER)).unwrap().unwrap()).unwrap(),1);
        assert_eq!(core.staged().await.unwrap().len(),100);
        assert!(!core.apply_pending().await.unwrap());
    }

    #[tokio::test]
    async fn cached_staging_authentication_does_not_accept_changed_durable_bytes() {
        let backend=Arc::new(Stub::new(OTHER,vec![]));let core=chunk_repair_core(backend.clone());
        let (tx,_)=signed_chunked_tx(1,10);core.stage(tx.clone()).await.unwrap();
        let mut changed=tx;changed.payload[0]^=1;
        backend.commit(vec![Record::put(stage_key(MASTER,1),encode(&changed).unwrap())]).unwrap();
        assert!(matches!(core.apply_pending().await,Err(Error::Auth(_))));
        assert!(backend.get(&progress_key(MASTER)).unwrap().is_none());
        let restarted=chunk_repair_core(backend.clone());
        assert!(matches!(restarted.apply_pending().await,Err(Error::Auth(_))));
        assert!(backend.get(&progress_key(MASTER)).unwrap().is_none());
    }

    #[test]
    fn staged_size_accounting_preserves_totals_and_rejects_overflow() {
        let mut sizes=StagedSizes::default();
        for i in 0..500u32 {sizes.insert(i.to_be_bytes().to_vec(),i as usize+1).unwrap();}
        for i in 0..500u32 {
            let key=i.to_be_bytes();
            if i%3==0 {sizes.remove(&key);} else {sizes.insert(key.to_vec(),7).unwrap();}
            assert_eq!(sizes.bytes,sizes.sizes.values().sum::<usize>());
        }
        sizes.remove(b"missing");
        let mut overflow=StagedSizes::default();overflow.insert(vec![1],usize::MAX).unwrap();
        assert!(matches!(overflow.insert(vec![2],1),Err(Error::Overloaded)));
        assert_eq!(overflow.sizes.len(),1);assert_eq!(overflow.bytes,usize::MAX);
        overflow.insert(vec![1],10).unwrap();assert_eq!(overflow.bytes,10);
    }

    #[test]
    #[ignore = "release component benchmark"]
    fn staged_admission_accounting_benchmark() {
        let mut sizes=StagedSizes::default();
        for i in 0..4096u32 {sizes.insert(i.to_be_bytes().to_vec(),1024).unwrap();}
        let start=std::time::Instant::now();
        for _ in 0..10000 {let cloned=std::hint::black_box(&sizes.sizes).clone();std::hint::black_box((cloned.len(),cloned.values().sum::<usize>()));}
        let before=start.elapsed();let start=std::time::Instant::now();
        for _ in 0..10000 {let cached=std::hint::black_box(&sizes);std::hint::black_box((cached.sizes.len(),cached.bytes));}
        eprintln!("staged_admission keys=4096 checks=10000 before_us={} after_us={}",before.as_micros(),start.elapsed().as_micros());
    }

    #[tokio::test]
    async fn staging_batch_commits_once_and_preserves_duplicate_conflict_checks() {
        let backend=Arc::new(Stub::new(OTHER,vec![]));let core=chunk_repair_core(backend.clone());
        let (first,_)=signed_chunked_tx(1,10);let (mut second,_)=signed_chunked_tx(2,10);
        second.millis=20;second.logical=3;second.sign(&MASTER_SEED).unwrap();
        core.stage_batch(vec![second.clone(),first.clone(),first.clone()]).await.unwrap();
        assert_eq!(backend.commits.load(Ordering::Relaxed),1);
        assert_eq!(*backend.clocks.lock().unwrap(),vec![(20,3)]);
        assert_eq!(core.staged().await.unwrap().len(),2);
        let expected_bytes=encode(&first).unwrap().len()+encode(&second).unwrap().len();
        assert_eq!(core.staged_totals().await.unwrap(),(2,expected_bytes));
        let restarted=chunk_repair_core(backend.clone());
        assert_eq!(restarted.staged_totals().await.unwrap(),(2,expected_bytes));
        assert_eq!(decode::<u64>(&backend.get(&observed_key(MASTER)).unwrap().unwrap()).unwrap(),2);
        core.stage_batch(vec![first.clone(),second]).await.unwrap();
        assert_eq!(backend.commits.load(Ordering::Relaxed),1);
        let mut conflict=first.clone();conflict.payload[0]^=1;conflict.sign(&MASTER_SEED).unwrap();
        assert!(matches!(core.stage_batch(vec![first,conflict]).await,Err(Error::Auth(_))));
        assert_eq!(backend.commits.load(Ordering::Relaxed),1);
        assert!(core.apply_pending().await.unwrap());
        assert_eq!(decode::<u64>(&backend.get(&progress_key(MASTER)).unwrap().unwrap()).unwrap(),2);
        assert_eq!(core.staged_totals().await.unwrap(),(0,0));
    }

    #[tokio::test]
    async fn staging_batch_rejects_over_budget_without_partial_publication() {
        let backend=Arc::new(Stub::new(OTHER,vec![]));let mut core=chunk_repair_core(backend.clone());
        core.config.staging_transfers=1;
        let (first,_)=signed_chunked_tx(1,10);let (second,_)=signed_chunked_tx(2,10);
        assert!(matches!(core.stage_batch(vec![first.clone(),second.clone()]).await,Err(Error::Overloaded)));
        assert_eq!(backend.commits.load(Ordering::Relaxed),0);
        assert!(backend.clocks.lock().unwrap().is_empty());assert!(core.staged().await.unwrap().is_empty());
        core.config.staging_transfers=2;core.config.staging_bytes=encode(&first).unwrap().len();
        assert!(matches!(core.stage_batch(vec![first,second]).await,Err(Error::Overloaded)));
        assert_eq!(backend.commits.load(Ordering::Relaxed),0);assert!(core.staged().await.unwrap().is_empty());
    }

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn continuous_apply_drains_multiple_batches_without_waiting_for_ticks() {
        let backend=Arc::new(Stub::new(OTHER,vec![]));
        let mut configured=chunk_repair_core(backend.clone());configured.config.batch_transactions=1;let core=Arc::new(configured);
        let mut records=Vec::new();
        for sequence in 1..=16 {let(tx,_)=signed_chunked_tx(sequence,10);records.push(Record::put(stage_key(MASTER,sequence),encode(&tx).unwrap()));}
        backend.commit(records).unwrap();let worker=core.clone();let task=tokio::spawn(async move{worker.apply_loop().await});core.apply_notify.notify_one();
        tokio::time::timeout(Duration::from_secs(3),async {loop {if core.control.status.lock().unwrap().applied_transactions==16{break;}tokio::time::sleep(Duration::from_millis(1)).await;}}).await.expect("apply still limited by one-second tick");
        task.abort();assert_eq!(decode::<u64>(&backend.get(&progress_key(MASTER)).unwrap().unwrap()).unwrap(),16);
    }

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn one_large_transaction_can_transfer_to_multiple_recipients() {
        let backend=Arc::new(Stub::new(OTHER,vec![]));let (_,data)=signed_chunked_tx(1,2<<20);
        backend.commit(vec![Record::put(log_key(MASTER,1),data)]).unwrap();let core=chunk_repair_core(backend);core.transfers.add_permits(1);
        let (first,mut a)=crate::queue::Sender::new(Arc::new(Semaphore::new(64<<20)),4<<20);
        let (second,mut b)=crate::queue::Sender::new(Arc::new(Semaphore::new(64<<20)),4<<20);
        core.send_transaction(MASTER,1,&first).await.unwrap();core.send_transaction(MASTER,1,&second).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2),async{assert!(matches!(a.recv().await.unwrap().message,Message::TransactionChunk(_)));assert!(matches!(b.recv().await.unwrap().message,Message::TransactionChunk(_)));}).await.unwrap();
        for task in std::mem::take(&mut *core.tasks.lock().unwrap()){task.abort();}
    }

}

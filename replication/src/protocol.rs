//! Canonical SQLite replication profile. Deliberately distinct from RIME v6.
use crate::*;
use ed25519_dalek::{Signer, Signature, SigningKey, VerifyingKey};
use sha2::{Sha256,Digest};
pub const VERSION:u16=13;
pub const ALPN:&[u8]=b"murmur-sqlite/1";
pub const FRAME_BYTES:usize=8<<20;
pub const CHUNK_BYTES:usize=64<<10;
pub const CAP_SIGNED_SCHEMA:u64=1<<7;
pub const CAPABILITIES:u64=(1<<1)|(1<<2)|(1<<3)|(1<<4)|(1<<5)|(1<<6)|CAP_SIGNED_SCHEMA;
pub const REQUIRED:u64=(1<<3)|(1<<4)|(1<<5)|CAP_SIGNED_SCHEMA;
pub const SIGN_DOMAIN:&[u8]=b"murmur/sqlite-origin-transaction/v1";
pub const SCHEMA_SIGN_DOMAIN:&[u8]=b"murmur/sqlite-schema/v1";
#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct Transaction {
    pub database:NodeId, pub origin:NodeId, pub sequence:u64, pub id:NodeId,
    pub millis:u64, pub logical:u32, pub schema:[u8;32], pub payload_digest:[u8;32], pub payload:Vec<u8>, pub signature:Vec<u8>,
}
impl Transaction {
    fn signed_bytes(&self)->Result<Vec<u8>> {
        let mut b=SIGN_DOMAIN.to_vec();
        b.extend(encode(&(self.database,self.origin,self.sequence,self.id,self.millis,self.logical,self.schema,self.payload_digest))?); Ok(b)
    }
    pub fn sign(&mut self,key:&[u8;32])->Result<()> { self.payload_digest=Sha256::digest(&self.payload).into(); self.signature=SigningKey::from_bytes(key).sign(&self.signed_bytes()?).to_bytes().to_vec(); Ok(()) }
    pub fn verify(&self,database:NodeId,trusted:&BTreeMap<NodeId,[u8;32]>,max:usize)->Result<()> {
        if self.database!=database || self.sequence==0 || self.payload.len()>max { return Err(Error::Protocol("transaction identity or size".into())); }
        if self.payload_digest!=<[u8;32]>::from(Sha256::digest(&self.payload)) {return Err(Error::Auth("payload digest mismatch".into()));}
        self.verify_header(database, trusted)
    }
    pub fn verify_header(&self,database:NodeId,trusted:&BTreeMap<NodeId,[u8;32]>)->Result<()> {
        if self.database!=database || self.sequence==0 {return Err(Error::Auth("transaction identity".into()));}
        let public=trusted.get(&self.origin).ok_or_else(||Error::Auth("untrusted transaction origin".into()))?;
        let key=VerifyingKey::from_bytes(public).map_err(|e|Error::Auth(e.to_string()))?;
        let signature=Signature::from_slice(&self.signature).map_err(|e|Error::Auth(e.to_string()))?;
        key.verify_strict(&self.signed_bytes()?,&signature).map_err(|e|Error::Auth(e.to_string()))
    }
    pub fn digest(&self)->Result<[u8;32]> { Ok(Sha256::digest(encode(self)?).into()) }
}
/// Master-signed schema promise. `schema` carries canonical replicated-subset
/// bytes; local-only tables are excluded by the sender before signing.
#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct SignedSchema {
    pub database:NodeId, pub signer:NodeId, pub sequence:u64,
    pub schema:Vec<u8>, pub signature:Vec<u8>,
}
impl SignedSchema {
    fn signed_bytes(&self)->Result<Vec<u8>> {
        let mut b=SCHEMA_SIGN_DOMAIN.to_vec();
        b.extend(encode(&(self.database,self.signer,self.sequence,Sha256::digest(&self.schema).to_vec()))?); Ok(b)
    }
    pub fn sign(&mut self,key:&[u8;32])->Result<()> { self.signature=SigningKey::from_bytes(key).sign(&self.signed_bytes()?).to_bytes().to_vec(); Ok(()) }
    /// Verify against one expected signer: the configured schema master, or in
    /// legacy mode the sender itself (any trusted origin).
    pub fn verify(&self,database:NodeId,master:NodeId,trusted:&BTreeMap<NodeId,[u8;32]>,max:usize)->Result<()> {
        if self.database!=database || self.signer!=master || self.sequence==0 || self.schema.len()>max {
            return Err(Error::Auth("schema envelope identity or size".into()));
        }
        let public=trusted.get(&self.signer).ok_or_else(||Error::Auth("untrusted schema signer".into()))?;
        let key=VerifyingKey::from_bytes(public).map_err(|e|Error::Auth(e.to_string()))?;
        let signature=Signature::from_slice(&self.signature).map_err(|e|Error::Auth(e.to_string()))?;
        key.verify_strict(&self.signed_bytes()?,&signature).map_err(|e|Error::Auth(e.to_string()))
    }
    pub fn digest(&self)->[u8;32] { Sha256::digest(&self.schema).into() }
}
#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct Chunk {pub proof:Transaction,pub origin:NodeId,pub sequence:u64,pub id:NodeId,pub index:u32,pub count:u32,pub total:u64,pub digest:[u8;32],pub data:Vec<u8>}
/// Durable per-transfer chunk manifest. Stored on the first chunk after the
/// proof signature verifies; later chunks bind to it by digest instead of
/// re-verifying. Survives restarts, so repair can resume without the sender.
#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct ChunkManifest {pub id:NodeId,pub count:u32,pub total:u64,pub digest:[u8;32]}
pub fn chunk_prefix(origin:NodeId,seq:u64)->String {format!("07/replication/chunks/{}/{seq:020}/",hex(&origin))}
pub fn chunk_key(origin:NodeId,seq:u64,index:u32)->Vec<u8> {format!("{}{index:08}",chunk_prefix(origin,seq)).into_bytes()}
pub fn chunk_manifest_key(origin:NodeId,seq:u64)->Vec<u8> {format!("{}manifest",chunk_prefix(origin,seq)).into_bytes()}
#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct Progress {pub origin:NodeId,pub applied:u64,pub observed:u64,pub retained_from:u64,pub retained_through:u64}
#[derive(Clone,Debug,Serialize,Deserialize)]
pub enum Message {
    Hello {capabilities:u64,required:u64,version:u16,database:NodeId,node:NodeId,fetch:Option<SocketAddr>,schema:Vec<u8>,mode:Dissemination,max_transaction:u64},
    Welcome {capabilities:u64,required:u64,database:NodeId,node:NodeId,fetch:Option<SocketAddr>,schema:Vec<u8>,mode:Dissemination},
    Batches(Vec<Transaction>),Ack(Vec<Progress>),Need{origin:NodeId,from:u64,through:u64},
    SnapshotRequest,
    SnapshotManifest(crate::snapshot::Manifest),
    SnapshotChunk{transfer:NodeId,index:u32,data:Vec<u8>},SnapshotDone{transfer:NodeId},
    SnapshotResume{transfer:NodeId},SnapshotNeed{transfer:NodeId,indexes:Vec<u32>},
    SnapshotApplied{transfer:NodeId,digest:[u8;32]},SnapshotUnavailable{transfer:NodeId},SnapshotCancel{transfer:NodeId},
    Ping(u64),Pong(u64),Error{code:u16,message:String},SchemaRequest,SchemaManifest(SignedSchema),SchemaAck{digest:[u8;32],sequence:u64},
    PlumtreeData(Transaction),IHave{origin:NodeId,sequence:u64},Prune,Graft{origin:NodeId,sequence:u64},
    ProgressRequest{offset:u32},ProgressPage{offset:u32,more:bool,entries:Vec<Progress>},TransactionChunk(Chunk),
    MembersRequest{offset:u32},MembersPage{offset:u32,more:bool,entries:Vec<crate::membership::Member>},
    ChunkNeed{origin:NodeId,sequence:u64,indexes:Vec<u32>},ChunkAvailabilityRequest{offset:u32},ChunkAvailabilityPage{entries:Vec<(NodeId,u64,Vec<u32>)>,more:bool},
}
impl Message {fn kind(&self)->u16 {match self {Self::Hello{..}=>1,Self::Welcome{..}=>2,Self::Batches(_)=>3,Self::Ack(_)=>4,Self::Need{..}=>5,Self::SnapshotRequest=>6,Self::SnapshotManifest(..)=>7,Self::SnapshotChunk{..}=>8,Self::SnapshotDone{..}=>9,Self::Ping(_)=>10,Self::Pong(_)=>11,Self::Error{..}=>12,Self::SchemaRequest=>13,Self::SchemaManifest(_)=>14,Self::SchemaAck{..}=>15,Self::PlumtreeData(_)=>16,Self::IHave{..}=>17,Self::Prune=>18,Self::Graft{..}=>19,Self::ProgressRequest{..}=>20,Self::ProgressPage{..}=>21,Self::TransactionChunk(_)=>22,Self::ChunkNeed{..}=>23,Self::ChunkAvailabilityRequest{..}=>24,Self::ChunkAvailabilityPage{..}=>25,Self::SnapshotResume{..}=>28,Self::SnapshotNeed{..}=>29,Self::SnapshotApplied{..}=>30,Self::SnapshotUnavailable{..}=>31,Self::SnapshotCancel{..}=>32,Self::MembersRequest{..}=>26,Self::MembersPage{..}=>27}} }
// Reusable immutable wire frame. Queue accounting uses the uncompressed size.
pub(crate) struct Prepared { pub(crate) bytes:Vec<u8>, pub(crate) charge:usize }
impl Prepared {
    pub(crate) fn new(message:&Message)->Result<Self> {
        let data=encode(message)?;if data.len()>FRAME_BYTES{return Err(Error::Overloaded);}
        let charge=data.len()+12;
        let (data,flags)=if data.len()>1024 {let compressed=zstd::stream::encode_all(data.as_slice(),3)?;if compressed.len()+4<data.len(){let mut bytes=(data.len() as u32).to_be_bytes().to_vec();bytes.extend(compressed);(bytes,0x8002u16)}else{(data,0u16)}}else{(data,0u16)};
        let mut bytes=vec![0u8;12];bytes[..2].copy_from_slice(b"RD");bytes[2..4].copy_from_slice(&VERSION.to_be_bytes());bytes[4..6].copy_from_slice(&message.kind().to_be_bytes());bytes[6..8].copy_from_slice(&flags.to_be_bytes());bytes[8..12].copy_from_slice(&(data.len() as u32).to_be_bytes());bytes.extend(data);
        Ok(Self{bytes,charge})
    }
    pub(crate) async fn write(&self,stream:&mut quinn::SendStream)->Result<()> {stream.write_all(&self.bytes).await.map_err(|e|Error::Protocol(e.to_string()))}
}
pub async fn write(stream:&mut quinn::SendStream,message:&Message)->Result<()> {Prepared::new(message)?.write(stream).await}
pub async fn read(stream:&mut quinn::RecvStream)->Result<Message> {
    let mut h=[0u8;12]; stream.read_exact(&mut h).await.map_err(|e|Error::Protocol(e.to_string()))?;
    let n=u32::from_be_bytes(h[8..12].try_into().unwrap()) as usize;
    if &h[..2]!=b"RD" || u16::from_be_bytes(h[2..4].try_into().unwrap())!=VERSION || ![0u16,0x8002].contains(&u16::from_be_bytes(h[6..8].try_into().unwrap())) || n>FRAME_BYTES {return Err(Error::Protocol("invalid frame header".into()));}
    let mut b=vec![0;n];stream.read_exact(&mut b).await.map_err(|e|Error::Protocol(e.to_string()))?;
    if u16::from_be_bytes(h[6..8].try_into().unwrap())==0x8002 {
        use std::io::Read;
        if b.len()<4{return Err(Error::Protocol("compressed frame length".into()));}
        let expected=u32::from_be_bytes(b[..4].try_into().unwrap()) as usize;if expected>FRAME_BYTES{return Err(Error::Overloaded);}
        let decoder=zstd::stream::read::Decoder::new(&b[4..])?;let mut plain=Vec::new();decoder.take(expected as u64+1).read_to_end(&mut plain)?;if plain.len()!=expected{return Err(Error::Protocol("decompressed frame size".into()));}b=plain;
    }
    let message:Message=decode(&b)?;if message.kind()!=u16::from_be_bytes(h[4..6].try_into().unwrap()) {return Err(Error::Protocol("frame type mismatch".into()));}Ok(message)
}
pub fn log_key(origin:NodeId,seq:u64)->Vec<u8> {format!("07/replication/log/{}/{seq:020}",hex(&origin)).into_bytes()}
pub fn stage_key(origin:NodeId,seq:u64)->Vec<u8> {format!("07/replication/staged/{}/{seq:020}",hex(&origin)).into_bytes()}
pub fn progress_key(origin:NodeId)->Vec<u8> {format!("07/replication/applied/{}",hex(&origin)).into_bytes()}
pub fn receipt_key(origin:NodeId,seq:u64)->Vec<u8> {format!("07/replication/receipt/{}/{seq:020}",hex(&origin)).into_bytes()}
pub fn sequence_key(origin:NodeId)->Vec<u8> {format!("07/replication/sequence/{}",hex(&origin)).into_bytes()}
/// Node-local outbound manifest cursor: encoded `(payload_digest, sequence)`.
pub fn schema_manifest_key()->Vec<u8> {b"07/replication/schema/manifest".to_vec()}
/// Highest accepted schema sequence per signer.
pub fn schema_accepted_key(signer:NodeId)->Vec<u8> {format!("07/replication/schema/accepted/{}",hex(&signer)).into_bytes()}
pub fn local_records(backend:&dyn Backend,payload:Vec<u8>,schema:&[u8],millis:u64,logical:u32,signing:&[u8;32])->Result<Vec<Record>> {
    let(db,node)=backend.identity();
    let seq=backend.get(&sequence_key(node))?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0).checked_add(1).ok_or_else(||Error::Protocol("origin sequence exhausted".into()))?;
    let mut tx=Transaction{database:db,origin:node,sequence:seq,id:rand::random(),millis,logical,schema:Sha256::digest(schema).into(),payload_digest:[0;32],payload,signature:Vec::new()};tx.sign(signing)?;
    Ok(vec![Record::put(crate::retention::time_key(node,seq),encode(&crate::retention::now_millis())?),Record::put(log_key(node,seq),encode(&tx)?),Record::put(sequence_key(node),encode(&seq)?),Record::put(progress_key(node),encode(&seq)?),Record::put(receipt_key(node,seq),tx.digest()?.to_vec())])
}
pub fn schema_digest(bytes:&[u8])->[u8;32] {Sha256::digest(bytes).into()}
pub fn observed_key(origin:NodeId)->Vec<u8>{format!("07/replication/observed/{}",hex(&origin)).into_bytes()}
pub fn retained_key(origin:NodeId)->Vec<u8>{format!("07/replication/retained/{}",hex(&origin)).into_bytes()}
pub fn public_key(seed:&[u8;32])->[u8;32]{SigningKey::from_bytes(seed).verifying_key().to_bytes()}

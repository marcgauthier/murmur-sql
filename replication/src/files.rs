//! Immutable authenticated file objects. Metadata belongs to the provided Spool backend.
use crate::*;
use aes_gcm::{Aes256Gcm,KeyInit,aead::{Aead,Payload}};
use sha2::{Digest,Sha256};
use std::{path::{Path,PathBuf},io::{Read,Write},fs::{self,File,OpenOptions},time::UNIX_EPOCH};
const CHUNK:usize=64<<10;
#[derive(Clone)]
pub struct Config {pub directory:PathBuf,pub key:zeroize::Zeroizing<[u8;32]>,pub previous_key:Option<zeroize::Zeroizing<[u8;32]>>,pub generation:u64,pub fetch_bind:SocketAddr,pub max_file_bytes:u64,pub max_staging_bytes:u64,pub concurrent_fetches:usize,pub fetch_interval:Duration}
impl std::fmt::Debug for Config {fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result {f.debug_struct("Files").field("directory",&self.directory).field("generation",&self.generation).finish_non_exhaustive()}}
impl Config {pub fn new(directory:PathBuf,key:[u8;32],fetch_bind:SocketAddr)->Self{Self{directory,key:zeroize::Zeroizing::new(key),previous_key:None,generation:0,fetch_bind,max_file_bytes:4<<30,max_staging_bytes:1<<30,concurrent_fetches:2,fetch_interval:Duration::from_secs(30)}}pub fn validate(&self)->Result<()>{if self.max_file_bytes==0 || self.max_staging_bytes<CHUNK as u64 || self.concurrent_fetches==0 || self.fetch_interval.is_zero(){return Err(Error::Invalid("file limits".into()));}Ok(())}}
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq,Eq)]
pub struct Metadata {pub name:String,pub digest:[u8;32],pub bytes:u64,pub generation:u64,pub millis:u64,pub node:NodeId,pub deleted:bool}
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq,Eq)]
pub enum Availability {Local,Remote(Vec<NodeId>),Unavailable}
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq,Eq)]
pub struct KeyRotationStatus {pub current_generation:u64,pub old_generation_objects:u64,pub old_generation_metadata_missing_locally:u64,pub staged_transfers:u64,pub previous_key_unused_locally:bool}
pub struct Objects {config:Config,backend:Arc<dyn Backend>,error:std::sync::Mutex<Option<String>>,gate:std::sync::Mutex<()>,staged_paths:Arc<std::sync::Mutex<BTreeSet<PathBuf>>>,staging_bytes:Arc<std::sync::Mutex<u64>>}
impl Objects {
    pub fn open(config:Config,backend:Arc<dyn Backend>)->Result<Self>{config.validate()?;for key in backend.keys(b"08/files/meta/")?{if let Some(bytes)=backend.get(&key)?{let meta:Metadata=decode(&bytes)?;if !meta.deleted&&meta.generation<config.generation{if config.previous_key.is_none(){return Err(Error::Invalid("previous file key is required while old-generation metadata exists".into()));}if meta.generation.checked_add(1)!=Some(config.generation){return Err(Error::Invalid("file keys must rotate one generation at a time".into()));}}}}fs::create_dir_all(&config.directory)?;Ok(Self{config,backend,error:std::sync::Mutex::new(None),gate:std::sync::Mutex::new(()),staged_paths:Arc::new(std::sync::Mutex::new(BTreeSet::new())),staging_bytes:Arc::new(std::sync::Mutex::new(0))})}
    pub(crate) fn protect_staging(self: &Arc<Self>, path: PathBuf) -> StagedTransfer {
        let _gate = self.gate.lock().unwrap();
        let paths = vec![path.clone(), path.with_extension("identity")];
        self.staged_paths.lock().unwrap().extend(paths.iter().cloned());
        StagedTransfer { objects: self.clone(), paths }
    }
    pub fn last_fetch_error(&self)->Option<String>{self.error.lock().unwrap().clone()}
    pub(crate) fn fetch_error(&self,error:&Error){*self.error.lock().unwrap()=Some(error.to_string());}
    pub fn upload(&self,name:&str,input:impl Read)->Result<Metadata>{let _gate=self.gate.lock().unwrap();self.upload_inner(name,input)}
    fn upload_inner(&self,name:&str,mut input:impl Read)->Result<Metadata>{
        let mut upload=Upload::new(self,name)?;
        let mut buffer=zeroize::Zeroizing::new(vec![0;CHUNK]);
        loop {let n=input.read(&mut buffer)?;if n==0 {break;}upload.append(&buffer[..n])?;}
        upload.finish_inner(self)
    }
    /// Begin a bounded encrypted upload. No metadata is published until finish.
    pub fn begin_upload(self:&Arc<Self>,name:&str)->Result<Upload>{let _gate=self.gate.lock().unwrap();Upload::new(self,name)}
    /// Publish a completed upload atomically with respect to local mutations.
    pub fn finish_upload(&self,upload:Upload)->Result<Metadata>{if !Arc::ptr_eq(&self.staging_bytes,&upload.used){return Err(Error::Invalid("upload belongs to another object store".into()));}let _gate=self.gate.lock().unwrap();upload.finish_inner(self)}
    /// Reserve shared disk staging capacity for an upload or peer fetch.
    pub(crate) fn reserve_staging(&self,bytes:u64)->Result<StagingReservation>{
        let mut used=self.staging_bytes.lock().unwrap();
        if bytes>self.config.max_staging_bytes.saturating_sub(*used){return Err(Error::Overloaded);}
        *used+=bytes;Ok(StagingReservation{used:self.staging_bytes.clone(),bytes})
    }
    /// Collect a bounded metadata snapshot without materializing all backend keys.
    pub fn list_bounded(&self,max_bytes:usize)->Result<Vec<Metadata>>{
        let mut out=Vec::new();let mut used=0usize;let mut after=None;
        loop {let keys=self.backend.keys_page(b"08/files/meta/",after.as_deref(),256)?;if keys.is_empty(){break;}
            for key in &keys {if let Some(bytes)=self.backend.get(key)? {let meta:Metadata=decode(&bytes)?;if !meta.deleted {used=used.saturating_add(meta.name.len()+256);if used>max_bytes{return Err(Error::Overloaded);}out.push(meta);}}}
            after=keys.last().cloned();
        }
        out.sort_by(|a,b|a.name.cmp(&b.name));Ok(out)
    }
    /// Pin and authenticate one immutable object before streaming it to a client.
    pub fn download(self:&Arc<Self>,name:&str)->Result<Download>{
        let _gate=self.gate.lock().unwrap();
        let meta=self.metadata(name)?.filter(|m|!m.deleted).ok_or_else(||Error::Unavailable("file metadata missing".into()))?;
        let path=self.path(meta.digest,meta.generation);
        let mut file=File::open(&path).map_err(|_|Error::Unavailable("file payload unavailable; retry after background fetching".into()))?;
        self.verify_input(&mut file,&meta,std::io::sink())?;
        use std::io::{Seek,SeekFrom};file.seek(SeekFrom::Start(0))?;
        let mut header=[0;40];file.read_exact(&mut header)?;
        let key=if meta.generation==self.config.generation{&*self.config.key}else{self.config.previous_key.as_deref().ok_or_else(||Error::Auth("object key unavailable".into()))?};
        let derived=object_key(key,header[16..32].try_into().unwrap())?;
        // A unique hard link pins the container across replacement/GC on all platforms.
        let pin=self.config.directory.join(format!(".download-{}",hex(&rand::random::<NodeId>())));
        fs::hard_link(&path,&pin)?;self.staged_paths.lock().unwrap().insert(pin.clone());
        Ok(Download{meta,file,cipher:Aes256Gcm::new_from_slice(&*derived).unwrap(),header,index:0,offset:0,pending:zeroize::Zeroizing::new(Vec::new()),pending_pos:0,pin,active:self.staged_paths.clone()})
    }
    pub fn metadata(&self,name:&str)->Result<Option<Metadata>>{check_name(name)?;self.backend.get(&meta_key(name))?.map(|b|decode(&b)).transpose()}
    pub fn list(&self)->Result<Vec<Metadata>>{let mut out=Vec::new();for key in self.backend.keys(b"08/files/meta/")?{if let Some(b)=self.backend.get(&key)?{let meta:Metadata=decode(&b)?;if !meta.deleted{out.push(meta);}}}out.sort_by(|a,b|a.name.cmp(&b.name));Ok(out)}
    pub fn available(&self,name:&str)->Result<bool>{Ok(self.metadata(name)?.is_some_and(|m|!m.deleted && self.path(m.digest,m.generation).is_file()))}
    pub fn availability(&self,name:&str)->Result<Availability>{let Some(meta)=self.metadata(name)? else{return Ok(Availability::Unavailable)};if meta.deleted{return Ok(Availability::Unavailable);}if self.path(meta.digest,meta.generation).is_file(){return Ok(Availability::Local);}let prefix=availability_prefix(meta.digest,meta.generation);let cutoff=now().saturating_sub(Duration::from_secs(7*86400).as_millis() as u64);let mut peers=Vec::new();for key in self.backend.keys(&prefix)?{if let Some(bytes)=self.backend.get(&key)?{let seen:u64=decode(&bytes)?;if seen>=cutoff{if let Some(node)=key.rsplit(|b|*b==b'/').next().and_then(|part|decode_hex_node(part)){peers.push(node);}}}}peers.sort();peers.dedup();Ok(if peers.is_empty(){Availability::Unavailable}else{Availability::Remote(peers)})}
    pub(crate) fn record_peer_availability(&self,digest:[u8;32],generation:u64,node:NodeId,available:bool)->Result<()>{let mut key=availability_prefix(digest,generation);key.extend(hex(&node).as_bytes());let mutation=if available{crate::Record::put(key,encode(&now())?)}else{crate::Record::delete(key)};self.backend.commit(vec![mutation])}
    pub fn delete(&self,name:&str)->Result<()>{let _gate=self.gate.lock().unwrap();if let Some(mut meta)=self.metadata(name)?{meta.deleted=true;meta.millis=now();meta.node=self.backend.identity().1;self.backend.publish(vec![Record::put(meta_key(name),encode(&meta)?)])?;}Ok(())}
    pub fn read(&self,name:&str,mut output:impl Write)->Result<Metadata>{let meta=self.metadata(name)?.filter(|m|!m.deleted).ok_or_else(||Error::Unavailable("file metadata missing".into()))?;self.verify(&self.path(meta.digest,meta.generation),&meta,&mut output)?;Ok(meta)}
    /// Verify and stream plaintext in bounded chunks, omitting an already
    /// exported prefix when resuming an external transfer.
    pub fn stream_plain_chunks(&self,name:&str,skip:u64,chunk_size:usize,mut emit:impl FnMut(u64,&[u8])->Result<()>)->Result<(Metadata,u64)>{if chunk_size==0{return Err(Error::Invalid("zero file chunk size".into()));}let meta=self.metadata(name)?.filter(|m|!m.deleted).ok_or_else(||Error::Unavailable("file metadata missing".into()))?;let mut sink=ChunkWriter{skip,position:skip,total:0,chunk_size,buffer:Vec::with_capacity(chunk_size),emit:&mut emit,error:None};let verified=self.verify(&self.path(meta.digest,meta.generation),&meta,&mut sink);if let Some(error)=sink.error.take(){return Err(error);}verified?;if !sink.buffer.is_empty(){let start=sink.position;(sink.emit)(start,&sink.buffer)?;sink.position+=sink.buffer.len() as u64;}Ok((meta,sink.total))}
    pub fn verify(&self,path:&Path,meta:&Metadata,output:impl Write)->Result<()>{let mut input=File::open(path).map_err(|_|Error::Unavailable("file payload unavailable".into()))?;self.verify_input(&mut input,meta,output)}
    fn verify_input(&self,mut input:impl Read,meta:&Metadata,mut output:impl Write)->Result<()>{let mut header=[0;40];input.read_exact(&mut header)?;if &header[..8]!=b"SPFO0001" {return Err(Error::Auth("object header".into()));}let generation=u64::from_be_bytes(header[8..16].try_into().unwrap());let key=if generation==self.config.generation {&*self.config.key}else if generation.checked_add(1)==Some(self.config.generation){self.config.previous_key.as_deref().ok_or_else(||Error::Auth("object key unavailable".into()))?}else{return Err(Error::Auth("object key generation".into()));};let derived=object_key(key,header[16..32].try_into().unwrap())?;let cipher=Aes256Gcm::new_from_slice(&*derived).unwrap();let prefix=header[32..40].try_into().unwrap();let mut index=0u32;let mut hash=Sha256::new();let mut total=0u64;loop{let mut n=[0;4];input.read_exact(&mut n)?;let mut size=u32::from_be_bytes(n) as usize;let final_chunk=size==u32::MAX as usize;if final_chunk{input.read_exact(&mut n)?;size=u32::from_be_bytes(n) as usize;}if size>CHUNK+16 || size<16{return Err(Error::Auth("object chunk length".into()));}let mut encrypted=vec![0;size];input.read_exact(&mut encrypted)?;let mut aad=header.to_vec();aad.extend(index.to_be_bytes());if final_chunk{aad.extend(b"final");}let nonce=nonce(prefix,index);let plain=cipher.decrypt((&nonce).into(),Payload{msg:&encrypted,aad:&aad}).map_err(|_|Error::Auth("object authentication".into()))?;if final_chunk{let(length,digest):(u64,[u8;32])=decode(&plain)?;if length!=total || length!=meta.bytes || digest!=meta.digest || <[u8;32]>::from(hash.finalize())!=digest || input.read(&mut [0;1])?!=0 {return Err(Error::Auth("object length or digest".into()));}return Ok(());}total=total.checked_add(plain.len() as u64).ok_or(Error::Overloaded)?;if total>meta.bytes || total>self.config.max_file_bytes{return Err(Error::Overloaded);}hash.update(&plain);output.write_all(&plain)?;index=index.checked_add(1).ok_or(Error::Overloaded)?;}}
    pub fn path(&self,digest:[u8;32],generation:u64)->PathBuf{self.config.directory.join(format!("{}.g{generation}.spfo",hex(&digest)))}
    /// Publish only a fully authenticated staged container.
    pub fn publish_fetched(&self,staged:&Path,meta:&Metadata)->Result<()>{let _gate=self.gate.lock().unwrap();self.verify(staged,meta,std::io::sink())?;let path=self.path(meta.digest,meta.generation);if !path.exists(){fs::rename(staged,path)?;sync_directory(&self.config.directory)?;}Ok(())}
    /// Re-encrypt up to `limit` old-generation objects with the current key.
    /// Metadata generation is the durable progress marker, so a restart resumes
    /// by scanning for the next stale entry.
    pub fn rotate_keys(&self,limit:usize)->Result<usize>{let mut rotated=0;for listed in self.list()?{if rotated>=limit{break;}if listed.generation==self.config.generation||!self.path(listed.digest,listed.generation).is_file(){continue;}let _guard=self.gate.lock().unwrap();let Some(current)=self.metadata(&listed.name)? else{continue;};if current.deleted||current.digest!=listed.digest||current.generation==self.config.generation{continue;}let raw=self.config.directory.join(format!(".rotate-{}",hex(&rand::random::<NodeId>())));let result=(||->Result<Metadata>{let mut plain=private_file(&raw)?;self.verify(&self.path(current.digest,current.generation),&current,&mut plain)?;plain.sync_all()?;drop(plain);self.upload_inner(&current.name,File::open(&raw)?)})();let _=fs::remove_file(&raw);result?;rotated+=1;}Ok(rotated)}
    /// Report whether this store still contains any object or staged transfer
    /// that may need an older file key. This is a local-store check.
    pub fn key_rotation_status(&self)->Result<KeyRotationStatus>{let mut old_generation_metadata_missing_locally=0;for meta in self.list()?{if meta.generation!=self.config.generation&&!self.path(meta.digest,meta.generation).is_file(){old_generation_metadata_missing_locally+=1;}}let mut old_generation_objects=0;let mut staged_transfers=0;for entry in fs::read_dir(&self.config.directory)?{let entry=entry?;let name=entry.file_name().to_string_lossy().into_owned();if name=="staging"&&entry.file_type()?.is_dir(){for staged in fs::read_dir(entry.path())?{if staged?.file_type()?.is_file(){staged_transfers+=1;}}}else if let Some((_,generation))=parse_object_name(&name){if generation!=self.config.generation{old_generation_objects+=1;}}}let previous_key_unused_locally=old_generation_objects==0&&old_generation_metadata_missing_locally==0&&staged_transfers==0;Ok(KeyRotationStatus{current_generation:self.config.generation,old_generation_objects,old_generation_metadata_missing_locally,staged_transfers,previous_key_unused_locally})}
    /// Collect unreferenced encrypted objects and abandoned staging files after
    /// a grace period. Metadata, including deletion tombstones, remains in Spool.
    pub fn collect(&self,grace:Duration)->Result<usize>{let _guard=self.gate.lock().unwrap();let cutoff=now().saturating_sub(grace.as_millis().min(u64::MAX as u128) as u64);let mut live=BTreeSet::new();for key in self.backend.keys(b"08/files/meta/")?{if let Some(bytes)=self.backend.get(&key)?{let meta:Metadata=decode(&bytes)?;if !meta.deleted{live.insert((meta.digest,meta.generation));}}}let mut removed=0;for entry in fs::read_dir(&self.config.directory)?{let entry=entry?;let name=entry.file_name().to_string_lossy().into_owned();let path=entry.path();if name=="staging"&&entry.file_type()?.is_dir(){for staged in fs::read_dir(&path)?{let staged=staged?;if self.staged_paths.lock().unwrap().contains(&staged.path()){continue;}let modified=staged.metadata()?.modified().ok().and_then(|t|t.duration_since(UNIX_EPOCH).ok()).map(|d|d.as_millis().min(u64::MAX as u128) as u64).unwrap_or_else(now);if modified<=cutoff&&staged.file_type()?.is_file(){fs::remove_file(staged.path())?;removed+=1;}}continue;}let modified=entry.metadata()?.modified().ok().and_then(|t|t.duration_since(UNIX_EPOCH).ok()).map(|d|d.as_millis().min(u64::MAX as u128) as u64).unwrap_or_else(now);if modified>cutoff{continue;}if self.staged_paths.lock().unwrap().contains(&path){continue;}if name.starts_with(".upload-")||name.starts_with(".rotate-")||name.starts_with(".download-"){if entry.file_type()?.is_file(){fs::remove_file(path)?;removed+=1;}continue;}if let Some((digest,generation))=parse_object_name(&name){if !live.contains(&(digest,generation)){fs::remove_file(path)?;removed+=1;}}}if removed>0{sync_directory(&self.config.directory)?;}Ok(removed)}
}
struct ChunkWriter<'a,F>{skip:u64,position:u64,total:u64,chunk_size:usize,buffer:Vec<u8>,emit:&'a mut F,error:Option<Error>}
impl<F:FnMut(u64,&[u8])->Result<()>> Write for ChunkWriter<'_,F>{fn write(&mut self,mut bytes:&[u8])->std::io::Result<usize>{let original=bytes.len();self.total=self.total.saturating_add(original as u64);let discard=(self.skip.min(bytes.len() as u64)) as usize;self.skip-=discard as u64;bytes=&bytes[discard..];while !bytes.is_empty(){let take=(self.chunk_size-self.buffer.len()).min(bytes.len());self.buffer.extend_from_slice(&bytes[..take]);bytes=&bytes[take..];if self.buffer.len()==self.chunk_size{if let Err(error)=(self.emit)(self.position,&self.buffer){self.error=Some(error);return Err(std::io::Error::other("file chunk consumer failed"));}self.position+=self.buffer.len() as u64;self.buffer.clear();}}Ok(original)}fn flush(&mut self)->std::io::Result<()>{Ok(())}}
fn parse_object_name(name:&str)->Option<([u8;32],u64)>{let (stem,generation)=name.strip_suffix(".spfo")?.rsplit_once(".g")?;let generation=generation.parse().ok()?;if stem.len()!=64{return None;}let mut digest=[0;32];for(i,pair)in stem.as_bytes().chunks_exact(2).enumerate(){digest[i]=(hexval(pair[0])?<<4)|hexval(pair[1])?;}Some((digest,generation))}
fn hexval(b:u8)->Option<u8>{match b{b'0'..=b'9'=>Some(b-b'0'),b'a'..=b'f'=>Some(b-b'a'+10),_=>None}}
fn availability_prefix(digest:[u8;32],generation:u64)->Vec<u8>{format!("08/files/availability/{}/{generation}/",hex(&digest)).into_bytes()}
fn decode_hex_node(bytes:&[u8])->Option<NodeId>{if bytes.len()!=32{return None;}let mut node=[0;16];for(i,pair)in bytes.chunks_exact(2).enumerate(){node[i]=(hexval(pair[0])?<<4)|hexval(pair[1])?;}Some(node)}
fn nonce(prefix:[u8;8],index:u32)->[u8;12]{let mut n=[0;12];n[..8].copy_from_slice(&prefix);n[8..].copy_from_slice(&index.to_be_bytes());n}
fn meta_key(name:&str)->Vec<u8>{format!("08/files/meta/{}",hex(&Sha256::digest(name.as_bytes()))).into_bytes()}
fn check_name(name:&str)->Result<()>{if name.is_empty() || name.len()>4096 || name.contains('\0'){return Err(Error::Invalid("file name".into()));}Ok(())}
fn now()->u64{std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64}
pub(crate) fn private_file(path:&Path)->Result<File>{let mut opts=OpenOptions::new();opts.write(true).create_new(true);#[cfg(unix)]{use std::os::unix::fs::OpenOptionsExt;opts.mode(0o600);}Ok(opts.open(path)?)}

fn object_key(master:&[u8;32],salt:&[u8;16])->Result<zeroize::Zeroizing<[u8;32]>> {let hk=hkdf::Hkdf::<Sha256>::new(Some(salt),master);let mut key=zeroize::Zeroizing::new([0;32]);hk.expand(b"murmur/sqlite-file-object/v1",&mut *key).map_err(|_|Error::Auth("object key derivation".into()))?;Ok(key)}

// Transfer ownership is transient; Spool remains authoritative for metadata.
pub(crate) struct StagedTransfer { objects: Arc<Objects>, paths: Vec<PathBuf> }
impl Drop for StagedTransfer {
    fn drop(&mut self) {
        let mut active = self.objects.staged_paths.lock().unwrap();
        for path in &self.paths { active.remove(path); }
    }
}

#[cfg(test)]
mod staging_tests {
    use super::*;
    use crate::test_support::Empty;
    #[test]
    fn collection_preserves_active_transfer_and_identity_until_guard_is_released() {
        let dir =
            std::env::temp_dir().join(format!("murmur-staging-{}", hex(&rand::random::<NodeId>())));
        let objects = Arc::new(
            Objects::open(
                Config::new(dir.clone(), [3; 32], "127.0.0.1:0".parse().unwrap()),
                Arc::new(Empty),
            )
            .unwrap(),
        );
        std::fs::create_dir_all(dir.join("staging")).unwrap();
        let path = dir.join("staging/file.part");
        let active = objects.protect_staging(path.clone());
        std::fs::write(&path, b"partial").unwrap();
        std::fs::write(path.with_extension("identity"), [1; 32]).unwrap();
        assert_eq!(objects.collect(Duration::ZERO).unwrap(), 0);
        assert!(path.exists());
        assert!(path.with_extension("identity").exists());
        assert!(
            !objects
                .key_rotation_status()
                .unwrap()
                .previous_key_unused_locally
        );
        drop(active);
        assert_eq!(objects.collect(Duration::ZERO).unwrap(), 2);
        assert!(
            objects
                .key_rotation_status()
                .unwrap()
                .previous_key_unused_locally
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}

// Keep directory durability behavior consistent with Spool on each platform.
pub(crate) fn sync_directory(path:&Path)->Result<()>{#[cfg(windows)]let _=path;#[cfg(not(windows))]File::open(path)?.sync_all()?;Ok(())}

/// A reservation shared by local uploads and background peer fetches.
pub(crate) struct StagingReservation { used: Arc<std::sync::Mutex<u64>>, bytes:u64 }
impl Drop for StagingReservation {fn drop(&mut self){let mut used=self.used.lock().unwrap();*used=used.saturating_sub(self.bytes);}}

/// Incremental encrypted object writer. Dropping aborts without changing metadata.
pub struct Upload {
    name:String,path:PathBuf,file:Option<File>,cipher:Aes256Gcm,header:Vec<u8>,prefix:[u8;8],
    hash:Sha256,total:u64,index:u32,generation:u64,max:u64,
    pending:zeroize::Zeroizing<Vec<u8>>,reservation:StagingReservation,
    used:Arc<std::sync::Mutex<u64>>,staging_max:u64,
    active:Arc<std::sync::Mutex<BTreeSet<PathBuf>>>,
}
impl Upload {
    fn new(objects:&Objects,name:&str)->Result<Self>{
        check_name(name)?;
        let reservation=objects.reserve_staging(128)?;
        let path=objects.config.directory.join(format!(".upload-{}",hex(&rand::random::<NodeId>())));
        let mut file=private_file(&path)?;
        let salt:[u8;16]=rand::random();let derived=object_key(&objects.config.key,&salt)?;
        let prefix:[u8;8]=rand::random();let mut header=b"SPFO0001".to_vec();
        header.extend(objects.config.generation.to_be_bytes());header.extend(salt);header.extend(prefix);
        if let Err(e)=file.write_all(&header){drop(file);let _=fs::remove_file(&path);return Err(e.into());}
        objects.staged_paths.lock().unwrap().insert(path.clone());
        Ok(Self{name:name.into(),path,file:Some(file),cipher:Aes256Gcm::new_from_slice(&*derived).unwrap(),header,prefix,hash:Sha256::new(),total:0,index:0,generation:objects.config.generation,max:objects.config.max_file_bytes,pending:zeroize::Zeroizing::new(Vec::with_capacity(CHUNK)),reservation,used:objects.staging_bytes.clone(),staging_max:objects.config.max_staging_bytes,active:objects.staged_paths.clone()})
    }
    pub fn append(&mut self,mut bytes:&[u8])->Result<()>{
        let total=self.total.checked_add(bytes.len() as u64).ok_or(Error::Overloaded)?;
        if total>self.max{return Err(Error::Overloaded);}
        let reserve=bytes.len() as u64+(total.div_ceil(CHUNK as u64)-self.total.div_ceil(CHUNK as u64))*20;
        {let mut used=self.used.lock().unwrap();if reserve>self.staging_max.saturating_sub(*used){return Err(Error::Overloaded);}*used+=reserve;}
        self.reservation.bytes+=reserve;
        self.hash.update(bytes);self.total=total;
        while !bytes.is_empty(){let n=(CHUNK-self.pending.len()).min(bytes.len());self.pending.extend_from_slice(&bytes[..n]);bytes=&bytes[n..];if self.pending.len()==CHUNK{self.flush_chunk()?;}}
        Ok(())
    }
    fn flush_chunk(&mut self)->Result<()>{
        if self.pending.is_empty(){return Ok(());}
        let mut aad=self.header.clone();aad.extend(self.index.to_be_bytes());
        let encrypted=self.cipher.encrypt((&nonce(self.prefix,self.index)).into(),Payload{msg:&self.pending,aad:&aad}).map_err(|_|Error::Auth("object encryption".into()))?;
        let file=self.file.as_mut().unwrap();file.write_all(&(encrypted.len() as u32).to_be_bytes())?;file.write_all(&encrypted)?;
        self.index=self.index.checked_add(1).ok_or(Error::Overloaded)?;
        use zeroize::Zeroize;self.pending.zeroize();self.pending.clear();Ok(())
    }
    fn finish_inner(mut self,objects:&Objects)->Result<Metadata>{
        self.flush_chunk()?;
        let digest:[u8;32]=self.hash.clone().finalize().into();let terminal=encode(&(self.total,digest))?;
        let mut aad=self.header.clone();aad.extend(self.index.to_be_bytes());aad.extend(b"final");
        let encrypted=self.cipher.encrypt((&nonce(self.prefix,self.index)).into(),Payload{msg:&terminal,aad:&aad}).map_err(|_|Error::Auth("object trailer encryption".into()))?;
        let mut file=self.file.take().unwrap();file.write_all(&u32::MAX.to_be_bytes())?;file.write_all(&(encrypted.len() as u32).to_be_bytes())?;file.write_all(&encrypted)?;file.sync_all()?;drop(file);
        let path=objects.path(digest,self.generation);
        if path.exists(){fs::remove_file(&self.path)?;}else{fs::rename(&self.path,&path)?;sync_directory(&objects.config.directory)?;}
        let meta=Metadata{name:self.name.clone(),digest,bytes:self.total,generation:self.generation,millis:now(),node:objects.backend.identity().1,deleted:false};
        objects.backend.publish(vec![Record::put(meta_key(&meta.name),encode(&meta)?)])?;Ok(meta)
    }
}
impl Drop for Upload {fn drop(&mut self){drop(self.file.take());let _=fs::remove_file(&self.path);self.active.lock().unwrap().remove(&self.path);}}

/// A verified, pinned immutable object, consumed in bounded plaintext chunks.
pub struct Download {pub meta:Metadata,file:File,cipher:Aes256Gcm,header:[u8;40],index:u32,offset:u64,pending:zeroize::Zeroizing<Vec<u8>>,pending_pos:usize,pin:PathBuf,active:Arc<std::sync::Mutex<BTreeSet<PathBuf>>>}
impl Download {
    pub fn next(&mut self,limit:usize)->Result<(u64,Vec<u8>,bool)>{
        if limit==0||limit>1<<20{return Err(Error::Invalid("file chunk limit".into()));}
        let start=self.offset;let mut out=Vec::with_capacity(limit);
        while out.len()<limit&&self.offset<self.meta.bytes {
            if self.pending.is_empty(){let mut n=[0;4];self.file.read_exact(&mut n)?;let n=u32::from_be_bytes(n) as usize;
                if !(16..=CHUNK+16).contains(&n){return Err(Error::Auth("object chunk length".into()));}
                let mut encrypted=vec![0;n];self.file.read_exact(&mut encrypted)?;
                let mut aad=self.header.to_vec();aad.extend(self.index.to_be_bytes());let prefix=self.header[32..40].try_into().unwrap();
                use zeroize::Zeroize;self.pending.zeroize();*self.pending=self.cipher.decrypt((&nonce(prefix,self.index)).into(),Payload{msg:&encrypted,aad:&aad}).map_err(|_|Error::Auth("object authentication".into()))?;self.index+=1;
            }
            let n=(limit-out.len()).min(self.pending.len()-self.pending_pos);out.extend_from_slice(&self.pending[self.pending_pos..self.pending_pos+n]);self.pending_pos+=n;self.offset+=n as u64;
            if self.pending_pos==self.pending.len(){use zeroize::Zeroize;self.pending.zeroize();self.pending_pos=0;}
        }
        Ok((start,out,self.offset==self.meta.bytes))
    }
}
impl Drop for Download {fn drop(&mut self){let _=fs::remove_file(&self.pin);self.active.lock().unwrap().remove(&self.pin);}}

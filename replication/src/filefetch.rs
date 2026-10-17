//! Independent QUIC endpoint; metadata is replicated by the ordinary mesh.
use crate::{*,files::{Objects,Metadata},transport};
use sha2::{Digest,Sha256};
use tokio::io::{AsyncReadExt,AsyncWriteExt,AsyncSeekExt};
use std::sync::Arc;
#[derive(Serialize,Deserialize)]
enum Frame {Request{database:NodeId,digest:[u8;32],generation:u64,offset:u64},Header{bytes:u64,identity:[u8;32]},Chunk(Vec<u8>),Done,Missing}
async fn write(send:&mut quinn::SendStream,frame:&Frame)->Result<()> {let data=encode(frame)?;if data.len()>128<<10{return Err(Error::Overloaded);}let mut header=[0;12];header[..2].copy_from_slice(b"FF");header[2..4].copy_from_slice(&1u16.to_be_bytes());header[8..].copy_from_slice(&(data.len() as u32).to_be_bytes());send.write_all(&header).await.map_err(|e|Error::Protocol(e.to_string()))?;send.write_all(&data).await.map_err(|e|Error::Protocol(e.to_string()))?;Ok(())}
async fn read(recv:&mut quinn::RecvStream)->Result<Frame>{let mut header=[0;12];recv.read_exact(&mut header).await.map_err(|e|Error::Protocol(e.to_string()))?;let n=u32::from_be_bytes(header[8..].try_into().unwrap()) as usize;if &header[..2]!=b"FF" || u16::from_be_bytes(header[2..4].try_into().unwrap())!=1 || n>128<<10{return Err(Error::Protocol("file frame".into()));}let mut b=vec![0;n];recv.read_exact(&mut b).await.map_err(|e|Error::Protocol(e.to_string()))?;decode(&b)}
#[derive(Clone)]
pub(crate) struct Service {pub endpoint:quinn::Endpoint,pub objects:Arc<Objects>,config:Config,database:NodeId,active:Arc<tokio::sync::Mutex<BTreeSet<[u8;32]>>>,permits:Arc<tokio::sync::Semaphore>,staging_permits:Arc<tokio::sync::Semaphore>}
impl Service {
    pub fn start(config:&Config,backend:Arc<dyn Backend>,objects:Arc<Objects>)->Result<Self>{let mut transport_config=config.clone();transport_config.bind=config.files.as_ref().unwrap().fetch_bind;let endpoint=transport::endpoint_with_alpn(&transport_config,backend.identity().1,b"murmur-sqlite/files/1")?;let file_config=config.files.as_ref().unwrap();let staging_units=file_config.max_staging_bytes.div_ceil(1<<20).min(u32::MAX as u64) as usize;let service=Self{endpoint,objects,config:transport_config,database:backend.identity().0,active:Arc::new(tokio::sync::Mutex::new(BTreeSet::new())),permits:Arc::new(tokio::sync::Semaphore::new(file_config.concurrent_fetches)),staging_permits:Arc::new(tokio::sync::Semaphore::new(staging_units))};let this=service.clone();tokio::spawn(async move {while let Some(incoming)=this.endpoint.accept().await {if !transport::network_allowed(&this.config,incoming.remote_address()){incoming.refuse();continue;}let permit=match this.permits.clone().try_acquire_owned(){Ok(p)=>p,Err(_)=>{incoming.refuse();continue;}};let handler=this.clone();tokio::spawn(async move {let _permit=permit;let result=tokio::time::timeout(Duration::from_secs(300),async {let connection=incoming.await.map_err(|e|Error::Protocol(e.to_string()))?;handler.serve(&connection).await}).await;if let Ok(Err(e))=result{handler.objects.fetch_error(&e);}});}});Ok(service)}
    async fn serve(&self,connection:&quinn::Connection)->Result<()>{transport::peer_id(connection,&self.config)?;let(mut send,mut recv)=connection.accept_bi().await.map_err(|e|Error::Protocol(e.to_string()))?;let Frame::Request{database,digest,generation,offset}=read(&mut recv).await? else{return Err(Error::Protocol("file request".into()));};if database!=self.database{return Err(Error::Auth("file database identity".into()));}let path=self.objects.path(digest,generation);let mut file=match tokio::fs::File::open(path).await{Ok(f)=>f,Err(_)=>{write(&mut send,&Frame::Missing).await?;return Ok(());}};let bytes=file.metadata().await?.len();if offset>bytes{return Err(Error::Protocol("file resume offset".into()));}let mut header=[0;40];file.read_exact(&mut header).await?;let identity=Sha256::digest(header).into();write(&mut send,&Frame::Header{bytes,identity}).await?;file.seek(std::io::SeekFrom::Start(offset)).await?;let mut chunk=vec![0;64<<10];loop{if !transport::network_allowed(&self.config,connection.remote_address()){return Err(Error::Auth("file peer migrated to denied network".into()));}let n=file.read(&mut chunk).await?;if n==0{break;}write(&mut send,&Frame::Chunk(chunk[..n].to_vec())).await?;}write(&mut send,&Frame::Done).await?;send.finish().map_err(|e|Error::Protocol(e.to_string()))?;let _=send.stopped().await;Ok(())}
    pub async fn fetch_missing(&self,sources:Vec<(NodeId,SocketAddr)>)->Result<()>{let metadata=self.objects.list()?;for meta in metadata {if self.objects.available(&meta.name)? || self.active.lock().await.contains(&meta.digest){continue;}let permit=match self.permits.clone().try_acquire_owned(){Ok(p)=>p,Err(_)=>break};let container=meta.bytes.checked_add(meta.bytes.div_ceil(64<<10)*20+256).ok_or(Error::Overloaded)?;let maximum=self.config.files.as_ref().unwrap().max_staging_bytes;if container>maximum{drop(permit);self.objects.fetch_error(&Error::Overloaded);continue;}let units=container.max(1).div_ceil(1<<20).min(u32::MAX as u64) as u32;let staged=match self.staging_permits.clone().try_acquire_many_owned(units){Ok(p)=>p,Err(_)=>{drop(permit);continue;}};self.active.lock().await.insert(meta.digest);let this=self.clone();let sources=sources.clone();tokio::spawn(async move {let _permit=permit;let _staged=staged;let _disk=match this.objects.reserve_staging(container){Ok(r)=>r,Err(e)=>{this.objects.fetch_error(&e);this.active.lock().await.remove(&meta.digest);return;}};for(source,address)in sources {let result=tokio::time::timeout(Duration::from_secs(300),this.fetch(source,address,&meta)).await;match result {Ok(Ok(()))=>break,Ok(Err(e))=>this.objects.fetch_error(&e),Err(_)=>this.objects.fetch_error(&Error::Unavailable("file transfer timeout".into()))}}this.active.lock().await.remove(&meta.digest);});}Ok(())}
    async fn fetch(&self,source:NodeId,address:SocketAddr,meta:&Metadata)->Result<()>{if !transport::network_allowed(&self.config,address){return Err(Error::Auth("file source network".into()));}let maximum=self.config.files.as_ref().unwrap().max_staging_bytes;let container_limit=meta.bytes.checked_add(meta.bytes.div_ceil(64<<10)*20+256).ok_or(Error::Overloaded)?;if container_limit>maximum{return Err(Error::Overloaded);}let staging=self.config.files.as_ref().unwrap().directory.join("staging");tokio::fs::create_dir_all(&staging).await?;let path=staging.join(format!("{}-{}.g{}.part",hex(&source),hex(&meta.digest),meta.generation));let _staging=self.objects.protect_staging(path.clone());let offset=resume_offset(&path).await?;let connection=self.endpoint.connect(address,"murmur.invalid").map_err(|e|Error::Protocol(e.to_string()))?.await.map_err(|e|Error::Protocol(e.to_string()))?;if transport::peer_id(&connection,&self.config)?!=source{return Err(Error::Auth("file source identity mismatch".into()));}let(mut send,mut recv)=connection.open_bi().await.map_err(|e|Error::Protocol(e.to_string()))?;write(&mut send,&Frame::Request{database:self.database,digest:meta.digest,generation:meta.generation,offset}).await?;let (bytes,identity)=match read(&mut recv).await?{Frame::Header{bytes,identity}=>(bytes,identity),Frame::Missing=>{self.objects.record_peer_availability(meta.digest,meta.generation,source,false)?;return Err(Error::Unavailable("file source has no object".into()));},_=>return Err(Error::Protocol("file response header".into()))};if bytes>container_limit || bytes>maximum || offset>bytes{return Err(Error::Overloaded);}let identity_path=path.with_extension("identity");if offset>0 {let old=tokio::fs::read(&identity_path).await?;if old!=identity {tokio::fs::remove_file(&path).await?;return Err(Error::Unavailable("source container changed; restart transfer".into()));}}else{tokio::fs::write(&identity_path,identity).await?;}
        let mut options=tokio::fs::OpenOptions::new();options.create(true).append(true);#[cfg(unix)]{options.mode(0o600);}let mut file=options.open(&path).await?;let mut total=offset;loop{match read(&mut recv).await? {Frame::Chunk(data)=>{if data.len()>64<<10{return Err(Error::Protocol("file chunk bounds".into()));}total+=data.len() as u64;if total>bytes{return Err(Error::Protocol("file container length".into()));}file.write_all(&data).await?;},Frame::Done=>break,_=>return Err(Error::Protocol("file transfer frame".into()))}}file.sync_all().await?;drop(file);connection.close(0u32.into(),b"file transfer complete");if total!=bytes{return Err(Error::Unavailable("incomplete file container".into()));}publish_staged(&self.objects,&path,&identity_path,meta).await?;self.objects.record_peer_availability(meta.digest,meta.generation,source,true)?;let _=tokio::fs::remove_file(identity_path).await;Ok(())}
}

async fn publish_staged(
    objects: &Objects,
    path: &std::path::Path,
    identity: &std::path::Path,
    meta: &Metadata,
) -> Result<()> {
    if let Err(error) = objects.publish_fetched(path, meta) {
        // Authentication failure must not leave a complete poisoned container
        // that every subsequent request attempts to resume at its end.
        let _ = tokio::fs::remove_file(path).await;
        let _ = tokio::fs::remove_file(identity).await;
        return Err(error);
    }
    Ok(())
}

// A crash can publish a partial container without its identity sidecar. Such
// bytes cannot be safely resumed; discard them and request a fresh container.
async fn resume_offset(path: &std::path::Path) -> Result<u64> {
    let offset = tokio::fs::metadata(path)
        .await
        .map(|m| m.len())
        .unwrap_or(0);
    if offset > 0
        && tokio::fs::metadata(path.with_extension("identity"))
            .await
            .map(|m| m.len())
            .unwrap_or(0)
            != 32
    {
        tokio::fs::remove_file(path).await?;
        return Ok(0);
    }
    Ok(offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn failed_authentication_discards_poisoned_container_and_resume_identity() {
        let dir = std::env::temp_dir().join(format!(
            "murmur-poisoned-{}",
            hex(&rand::random::<NodeId>())
        ));
        let objects = Objects::open(
            files::Config::new(dir.clone(), [3; 32], "127.0.0.1:0".parse().unwrap()),
            Arc::new(crate::test_support::Empty),
        )
        .unwrap();
        let path = dir.join("file.part");
        let identity = path.with_extension("identity");
        std::fs::write(&path, b"bad container").unwrap();
        std::fs::write(&identity, [1; 32]).unwrap();
        let meta = Metadata {
            name: "report".into(),
            digest: [7; 32],
            bytes: 100,
            generation: 0,
            millis: 0,
            node: [1; 16],
            deleted: false,
        };
        assert!(
            publish_staged(&objects, &path, &identity, &meta)
                .await
                .is_err()
        );
        assert!(!path.exists());
        assert!(!identity.exists());
        assert_eq!(resume_offset(&path).await.unwrap(), 0);
        assert!(!objects.path(meta.digest, meta.generation).exists());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[tokio::test]
    async fn partial_transfer_requires_a_complete_identity_sidecar() {
        let dir =
            std::env::temp_dir().join(format!("murmur-fetch-{}", hex(&rand::random::<NodeId>())));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("file.part");
        for sidecar in [None, Some(vec![1; 31]), Some(vec![1; 32])] {
            std::fs::write(&path, vec![7; 100]).unwrap();
            if let Some(bytes) = &sidecar {
                std::fs::write(path.with_extension("identity"), bytes).unwrap();
            }
            let expected = if sidecar.as_ref().is_some_and(|x| x.len() == 32) {
                100
            } else {
                0
            };
            assert_eq!(resume_offset(&path).await.unwrap(), expected);
            assert_eq!(path.exists(), expected != 0);
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}

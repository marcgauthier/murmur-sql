//! Ephemeral port helpers.
//!
//! SQL listeners take an up-front free port (best effort: the address can be
//! stolen between `bind` and reuse). Replication binds `127.0.0.1:0`
//! directly and needs no coordination (the Go port-claim registry is dropped
//! by design); the bound address is discovered via `MURMUR SHOW REPLICATION`.

use std::net::SocketAddr;

/// Bind `127.0.0.1:0`, report the address, release it.
pub fn free_addr() -> std::io::Result<SocketAddr> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?)
}

/// Just the port half of [`free_addr`].
pub fn free_port() -> std::io::Result<u16> {
    Ok(free_addr()?.port())
}

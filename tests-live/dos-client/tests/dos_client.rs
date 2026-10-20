//! Honest Rust SQL clients remain usable during PostgreSQL and QUIC connection pressure.
use murmur_live_harness::{assert_rows, Cluster, ClusterOptions};
use std::{
    io::Cursor,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::io::AsyncWriteExt;
fn seconds(name: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env::var(name)
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n| *n > 0)
            .unwrap_or(default),
    )
}
fn endpoint(dir: &std::path::Path) -> quinn::Endpoint {
    let ca = std::fs::read(dir.join("ca.pem")).unwrap();
    let leaf = std::fs::read(dir.join("cert.pem")).unwrap();
    let private = std::fs::read(dir.join("key.pem")).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut Cursor::new(ca)) {
        roots.add(cert.unwrap()).unwrap();
    }
    let chain = rustls_pemfile::certs(&mut Cursor::new(leaf))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key = rustls_pemfile::private_key(&mut Cursor::new(private))
        .unwrap()
        .unwrap();
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_root_certificates(roots)
    .with_client_auth_cert(chain, key)
    .unwrap();
    tls.alpn_protocols = vec![b"murmur-sqlite/1".to_vec()];
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap(),
    )));
    endpoint
}
async fn peer_count(cluster: &Cluster, node: usize) -> usize {
    cluster
        .node(node)
        .simple("MURMUR SHOW PEERS")
        .await
        .unwrap()
        .iter()
        .filter(|r| r[2] == "true")
        .count()
}
#[tokio::test(flavor = "multi_thread")]
async fn sql_and_quic_half_open_pressure_does_not_starve_honest_clients() {
    let cluster = Cluster::new(ClusterOptions {
        name: "dos-client".into(),
        num_nodes: 2,
        ..Default::default()
    })
    .await
    .unwrap();
    for i in 0..2 {
        cluster
            .node(i)
            .simple("CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY,body TEXT NOT NULL)")
            .await
            .unwrap();
        cluster
            .node(i)
            .simple("MURMUR SET TABLE records REPLICATED true")
            .await
            .unwrap();
    }
    cluster
        .node(0)
        .simple("INSERT INTO records(id,body) VALUES(0,'baseline')")
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    let mut expected = vec![vec!["baseline".into()]];
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT body FROM records ORDER BY id",
        &expected,
    )
    .await;
    let base = peer_count(&cluster, 0).await;
    assert_eq!(base, 1);
    let count = std::env::var("MURMUR_DOS_CLIENT_QUIC_HALFOPEN")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(24);
    let attacker = endpoint(&cluster.node_info(0).dir);
    let mut half_opens = Vec::new();
    for _ in 0..count {
        let connect = attacker
            .connect(cluster.node_info(0).repl_addr, "localhost")
            .unwrap();
        if let Ok(Ok(conn)) = tokio::time::timeout(Duration::from_secs(2), connect).await {
            if let Ok(Ok((send, recv))) =
                tokio::time::timeout(Duration::from_secs(2), conn.open_bi()).await
            {
                half_opens.push((conn, send, recv));
            }
        }
    }
    assert!(
        half_opens.len() >= count.div_ceil(2),
        "attack failed to hold enough authenticated QUIC half-opens: {} of {count}",
        half_opens.len()
    );
    let mut sql_half = Vec::new();
    for _ in 0..count {
        // Hold one pre-startup connection and one partial PostgreSQL SSLRequest.
        sql_half.push(
            tokio::net::TcpStream::connect(cluster.node_info(0).sql_addr)
                .await
                .unwrap(),
        );
        let mut partial = tokio::net::TcpStream::connect(cluster.node_info(0).sql_addr)
            .await
            .unwrap();
        partial.write_all(&[0, 0, 0]).await.unwrap();
        sql_half.push(partial);
    }
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    udp.connect(cluster.node_info(0).repl_addr).await.unwrap();
    let deadline = Instant::now() + seconds("MURMUR_DOS_CLIENT_ATTACK_SECONDS", 60);
    let mut id = 1i64;
    while Instant::now() < deadline {
        // The server expires missing-Hello connections after five seconds.
        // Replace expired connections so long runs keep applying pressure.
        half_opens.retain(|(connection, _, _)| connection.close_reason().is_none());
        while half_opens.len() < count {
            let connecting = attacker
                .connect(cluster.node_info(0).repl_addr, "localhost")
                .unwrap();
            let connection = tokio::time::timeout(Duration::from_secs(2), connecting)
                .await
                .expect("half-open top-up timed out")
                .expect("half-open top-up handshake failed");
            let (send, recv) = tokio::time::timeout(Duration::from_secs(2), connection.open_bi())
                .await
                .expect("half-open stream timed out")
                .unwrap();
            half_opens.push((connection, send, recv));
        }
        for _ in 0..10 {
            udp.send(&[0xa5; 1200]).await.unwrap();
        }
        for node in 0..2 {
            let body = format!("heartbeat-{id:04}-node-{node}");
            let start = Instant::now();
            let durable_write = async {
                cluster
                    .node(node)
                    .execute("INSERT INTO records(id,body) VALUES($1,$2)", &[&id, &body])
                    .await?;
                cluster.node(node).simple("MURMUR FLUSH").await
            };
            tokio::time::timeout(Duration::from_secs(10), durable_write)
                .await
                .expect("honest durable SQL write starved")
                .unwrap();
            assert!(start.elapsed() < Duration::from_secs(10));
            expected.push(vec![body]);
            id += 1;
        }
        assert!(
            peer_count(&cluster, 0).await <= base + 2,
            "half-opens became replication peers"
        );
        let alive = half_opens
            .iter()
            .filter(|(c, _, _)| c.close_reason().is_none())
            .count();
        assert!(
            alive >= count.div_ceil(2),
            "half-open attack pressure vanished early"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(expected.len() >= 3, "no honest write made under pressure");
    for (node, pattern) in [(0, "%node-1"), (1, "%node-0")] {
        let rows = cluster
            .node(node)
            .query(
                "SELECT count(*) FROM records WHERE body LIKE $1",
                &[&pattern],
            )
            .await
            .unwrap();
        assert!(
            rows[0].get::<_, i64>(0) > 0,
            "remote apply starved for the entire attack window"
        );
    }
    drop(sql_half);
    for (conn, _, _) in half_opens {
        conn.close(0u32.into(), b"done");
    }
    attacker.close(0u32.into(), b"done");
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT body FROM records ORDER BY id",
        &expected,
    )
    .await;
    let recovery = Instant::now() + Duration::from_secs(30);
    while peer_count(&cluster, 0).await != base {
        assert!(
            Instant::now() < recovery,
            "attacker left replication sessions attached"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cluster
        .node(0)
        .execute(
            "INSERT INTO records(id,body) VALUES($1,'post-attack')",
            &[&id],
        )
        .await
        .unwrap();
    cluster.node(0).simple("MURMUR FLUSH").await.unwrap();
    expected.push(vec!["post-attack".into()]);
    assert_rows(
        &cluster,
        &[0, 1],
        "SELECT body FROM records ORDER BY id",
        &expected,
    )
    .await;
}

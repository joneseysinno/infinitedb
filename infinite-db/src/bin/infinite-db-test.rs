//! Minimal TCP client for manual server smoke tests.
//!
//! ```text
//! cargo run --bin infinite-db-test --features sync -- <host:port> ping
//! cargo run --bin infinite-db-test --features sync -- 127.0.0.1:9000 query
//! ```

use std::env;

use infinite_db::infinitedb_core::address::{DimensionVector, SpaceId};
use infinite_db::infinitedb_core::snapshot::SnapshotId;
use infinite_db::{client_roundtrip, Request};
use tokio::runtime::Runtime;

fn main() {
    let mut args = env::args().skip(1);
    let addr: std::net::SocketAddr = args
        .next()
        .expect("usage: infinite-db-test <host:port> <ping|query>")
        .parse()
        .expect("invalid address");
    let cmd = args.next().unwrap_or_else(|| "ping".to_string());

    let request = match cmd.as_str() {
        "ping" => Request::Ping,
        "query" => Request::Query {
            space: SpaceId(1),
            snapshot: SnapshotId(0),
            key_range: None,
            as_of: None,
            include_tombstones: false,
        },
        "write" => Request::Write {
            address: infinite_db::infinitedb_core::address::Address::new(
                SpaceId(1),
                DimensionVector::new(vec![0, 0]),
            ),
            revision: infinite_db::infinitedb_core::address::RevisionId::legacy(1),
            data: vec![1, 2, 3],
        },
        other => panic!("unknown command {other}"),
    };

    let runtime = Runtime::new().expect("tokio runtime");
    let response = runtime
        .block_on(client_roundtrip(addr, request))
        .expect("request failed");
    println!("{response:?}");
}

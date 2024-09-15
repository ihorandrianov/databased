mod bytecode_serializer;
mod errors;
mod filesystem;
mod in_memory;
mod kvstore;
mod log;
mod lru_cache;
mod operation;
mod parser;
mod persistent;
mod tcp_adapter;
mod wal_io;
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    let root = PathBuf::from("data");
    let cache_size = 100u32;
    let mut kvstore = kvstore::KvStore::new(root, cache_size).await.unwrap();
    kvstore.regenerate().await.unwrap();
    kvstore.run().await;
}

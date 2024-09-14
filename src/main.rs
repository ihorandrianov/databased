mod bytecode_serializer;
mod errors;
mod filesystem;
mod in_memory;
mod kvstore;
mod log;
mod operation;
mod parser;
mod persistent;
mod wal_io;
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    let root = PathBuf::from("data");
    let mut kvstore = kvstore::KvStore::new(root).await.unwrap();
    kvstore.regenerate().await.unwrap();
    kvstore.run().await;
}

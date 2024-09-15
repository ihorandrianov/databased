use std::path::PathBuf;

use crate::bytecode_serializer::BytecodeSerializer;
use crate::errors::{BytecodeSerializerError, KVStoreError};
use crate::filesystem::FileSystem;
use crate::in_memory::InMemoryLayer;
use crate::log::WAL;
use crate::lru_cache::LruCacheLayer;
use crate::operation::Op;
use crate::parser::Parser;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::task;

pub struct KvStore {
    store: InMemoryLayer,
    wal: Arc<Mutex<WAL>>,
    file_system: FileSystem,
    parser: Parser,
    send_to_wal: tokio::sync::mpsc::Sender<Op>,
    cache: LruCacheLayer,
}

impl KvStore {
    pub async fn new(root: PathBuf, cache_size: u32) -> Result<Self, KVStoreError> {
        let store = InMemoryLayer::new();

        let file_system = FileSystem::new(root)
            .await
            .map_err(|e| KVStoreError::FileSystemError(e))?;

        file_system
            .init()
            .await
            .map_err(|e| KVStoreError::FileSystemError(e))?;

        let wal_dir = file_system.get_wal_ref().await.clone();

        let (tx, rx) = tokio::sync::mpsc::channel::<Op>(100);
        let wal_filesize_limit = 5 * 1024 * 1024;
        let wal = WAL::new(rx, wal_dir, wal_filesize_limit)
            .await
            .map_err(|e| KVStoreError::WALError(e))?;

        let wal = Arc::new(Mutex::new(wal));

        let parser = Parser::new();

        let cache = LruCacheLayer::new(cache_size);

        Ok(Self {
            store,
            wal,
            file_system,
            parser,
            send_to_wal: tx,
            cache,
        })
    }

    pub async fn run(&mut self) {
        let wal = Arc::clone(&self.wal);
        task::spawn(async move {
            let mut wal = wal.lock().await;
            wal.run().await;
        });

        let std_in = tokio::io::stdin();
        let mut reader = tokio::io::BufReader::new(std_in);
        let mut std_out = tokio::io::stdout();
        loop {
            let op = self.parser.parse(&mut reader).await;
            match op {
                Ok(op) => {
                    for op in op {
                        self.send_to_wal.send(op.clone()).await.unwrap();
                        let result = self.store.eval(op);
                        match result {
                            Some(str) => {
                                if let Err(e) = std_out
                                    .write_all(format!("Result: {}\n", str).as_bytes())
                                    .await
                                {
                                    eprintln!("Error writing to stdout: {:?}", e);
                                };
                                std_out.flush().await.unwrap();
                            }
                            None => {
                                if let Err(e) = std_out.write_all("Result: None\n".as_bytes()).await
                                {
                                    eprintln!("Error writing to stdout: {:?}\n", e)
                                };
                                std_out.flush().await.unwrap();
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error parsing input: {:?}", e);
                }
            }
        }
    }

    pub async fn regenerate(&mut self) -> Result<(), BytecodeSerializerError> {
        let recovered_file = self.wal.lock().await.recover().await;
        let chunks = BytecodeSerializer::recover_from_bytes(&recovered_file).map_err(|_| {
            BytecodeSerializerError::DeserializationError("Error deserializing".to_string())
        })?;
        for chunk in chunks {
            self.store.eval(chunk);
        }
        Ok(())
    }
}

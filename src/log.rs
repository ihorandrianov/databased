use crate::operation::Op;
use crate::wal_io::WALio;
use std::path::PathBuf;
use tokio::fs;
use tokio::sync::mpsc::Receiver;

pub struct WAL {
    rc: Receiver<Op>,
    io_controller: WALio,
    wal_path: PathBuf,
}

impl WAL {
    pub async fn new(rc: Receiver<Op>, wal_path: PathBuf) -> Self {
        let file_name = wal_path.clone().join("wal_01");
        let file_handle = fs::File::options()
            .create(true)
            .write(true)
            .read(true)
            .open(file_name)
            .await
            .unwrap();
        let io_controller = WALio::new(file_handle, 100);
        Self {
            rc,
            io_controller,
            wal_path,
        }
    }

    pub async fn run(&mut self) -> () {
        while let Some(op) = self.rc.recv().await {
            let serialized = op.into_bytes();
            if let Err(e) = self.io_controller.write(serialized).await {
                eprintln!("Error writing to WAL: {:?}", e);
            }
        }
    }
}

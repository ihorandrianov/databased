use crate::operation::Op;
use crate::wal_io::WALio;
use tokio::sync::mpsc::Receiver;

pub struct WAL {
    rc: Receiver<Op>,
    io_controller: WALio,
}

impl WAL {
    pub async fn new(rc: Receiver<Op>, io_controller: WALio) -> Self {
        Self { rc, io_controller }
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        while let Some(op) = self.rc.recv().await {
            let serialized = op.into_bytes();
            self.io_controller.write(serialized).await?;
        }
        Ok(())
    }
}

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct WALio {
    file_handle: File,
    batch: Vec<u8>,
}

impl WALio {
    pub async fn new(file_handle: File, batch_capacity: usize) -> Self {
        Self {
            file_handle,
            batch: Vec::with_capacity(batch_capacity),
        }
    }

    pub async fn write(&mut self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        if (self.batch.len() + data.len()) > self.batch.capacity() {
            self.flush().await?;
        }
        self.batch.extend(data);
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.file_handle.write_all(&self.batch).await?;
        self.batch.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_wal_io() {
        let file_handle = File::options()
            .create(true)
            .write(true)
            .read(true)
            .open("wal_io")
            .await
            .unwrap();
        let mut wal_io = WALio::new(file_handle, 100).await;
        let data = vec![1, 2, 3, 4, 5];
        wal_io.write(data).await.unwrap();
        wal_io.flush().await.unwrap();

        let mut file = File::options().read(true).open("wal_io").await.unwrap();
        let mut contents = vec![];
        file.read_to_end(&mut contents).await.unwrap();

        assert_eq!(contents, vec![1, 2, 3, 4, 5]);
    }
}

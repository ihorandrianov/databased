use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::errors::WALError;

pub struct WALio {
    file_handle: File,
    batch: Vec<u8>,
}

impl WALio {
    pub fn new(file_handle: File, batch_capacity: usize) -> Self {
        Self {
            file_handle,
            batch: Vec::with_capacity(batch_capacity),
        }
    }

    pub async fn write(&mut self, data: Vec<u8>) -> Result<(), WALError> {
        if (self.batch.len() + data.len()) > self.batch.capacity() {
            self.flush().await?;
        }
        self.batch.extend(data);
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), WALError> {
        self.file_handle.write_all(&self.batch).await?;
        self.batch.clear();
        Ok(())
    }

    pub async fn recover(&mut self) -> Result<Vec<u8>, WALError> {
        let mut buffer = Vec::new();
        self.file_handle.read_to_end(&mut buffer).await?;
        Ok(buffer)
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
        let mut wal_io = WALio::new(file_handle, 100);
        let data = {
            let mut data = Vec::new();
            for i in 0..225 {
                data.push(i);
            }
            data
        };
        wal_io.write(data).await.unwrap();
        wal_io.flush().await.unwrap();

        let mut file = File::options().read(true).open("wal_io").await.unwrap();
        let mut contents = vec![];
        file.read_to_end(&mut contents).await.unwrap();

        assert_eq!(
            contents,
            vec![
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43,
                44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
                65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85,
                86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104,
                105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120,
                121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136,
                137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152,
                153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168,
                169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184,
                185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200,
                201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216,
                217, 218, 219, 220, 221, 222, 223, 224
            ]
        );
    }
}

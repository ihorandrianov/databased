use std::path::{Path, PathBuf};

use tokio::fs;

use crate::errors::FileSystemError;

pub struct FileSystem {
    root: PathBuf,
    wal: PathBuf,
    snapshot: PathBuf,
    temp: PathBuf,
    persistent: PathBuf,
}

impl FileSystem {
    pub async fn new(root: PathBuf) -> Result<Self, FileSystemError> {
        let wal = root.join("wal");
        let snapshot = root.join("snapshot");
        let temp = root.join("temp");
        let persistent = root.join("persistent");
        Ok(Self {
            root,
            wal,
            snapshot,
            temp,
            persistent,
        })
    }

    async fn init(&self) -> Result<(), FileSystemError> {
        let dirs = vec![&self.wal, &self.snapshot, &self.temp, &self.persistent];
        for dir in dirs {
            self.create_dir(dir).await?;
        }
        Ok(())
    }

    async fn create_dir(&self, dir: &Path) -> Result<(), FileSystemError> {
        fs::create_dir_all(dir)
            .await
            .map_err(|e| FileSystemError::CreateDir(format!("{:?}, error: {}", dir, e)))?;
        Ok(())
    }

    async fn get_root_ref(&self) -> &PathBuf {
        &self.root
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_filesystem() {
        let root = PathBuf::new();
        let root = root.join("test_filesystem");
        let fs = FileSystem::new(root).await.unwrap();
        fs.init().await.unwrap();
        assert!(fs.get_root_ref().await.is_dir())
    }
}

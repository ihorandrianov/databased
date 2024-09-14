use crate::errors::WALError;
use crate::operation::Op;
use crate::wal_io::WALio;

use std::path::{Path, PathBuf};

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::fs::{self, File};
use tokio::sync::mpsc::Receiver;

type DeletedFilesCount = usize;

pub struct WAL {
    rc: Receiver<Op>,
    io_controller: WALio,
    wal_file_manager: WALFileManager,
}

impl WAL {
    pub async fn new(
        rc: Receiver<Op>,
        wal_path: PathBuf,
        wal_filesize_limit: u64,
    ) -> Result<Self, WALError> {
        let file_manager = WALFileManager::new(wal_path, wal_filesize_limit).await?;
        let file_handle = file_manager.get_file_handler().await?;
        let io_controller = WALio::new(file_handle, 3);
        Ok(Self {
            rc,
            io_controller,
            wal_file_manager: file_manager,
        })
    }

    pub async fn run(&mut self) -> () {
        while let Some(op) = self.rc.recv().await {
            match op {
                Op::GET {
                    key: _,
                    timestamp: __,
                } => {
                    continue;
                }
                _ => {
                    let serialized = op.into_bytes();
                    if let Err(e) = self.io_controller.write(serialized).await {
                        eprintln!("Error writing to WAL: {:?}", e);
                    }
                }
            }
            let new_file = self.wal_file_manager.size_rotate().await;
            match new_file {
                Ok(opt) => {
                    if let Some(file) = opt {
                        self.io_controller.set_new_file_handle(file)
                    }
                }
                Err(e) => eprintln!("Error rotating WAL file: {:?}", e),
            }
        }
    }

    pub async fn recover(&mut self) -> Vec<u8> {
        if let Ok(buffer) = self.io_controller.recover().await {
            buffer
        } else {
            Vec::new()
        }
    }
}

// file names are in the format wal_{timestamp} where timestamp is the time the file was created
pub struct WALFileManager {
    wal_path: PathBuf,
    wal_dir_files: Vec<PathBuf>,
    file_size: u64,
    latest_file: PathBuf,
}

impl WALFileManager {
    pub async fn new(wal_path: PathBuf, file_size: u64) -> Result<Self, WALError> {
        let mut dir_contents = fs::read_dir(&wal_path).await?;
        let mut wal_dir_files = Vec::new();
        while let Some(entry) = dir_contents.next_entry().await? {
            wal_dir_files.push(entry.path());
        }

        let latest_file = {
            if wal_dir_files.is_empty() {
                let timestamp = chrono::Utc::now().timestamp();
                let file_name = wal_path.join(format!("wal_{}", timestamp));
                let _file = fs::File::create(&file_name).await?;
                wal_dir_files.push(file_name.clone());
                file_name
            } else {
                wal_dir_files.sort();
                wal_dir_files
                    .last()
                    .expect("At least 1 file should be present")
                    .clone()
            }
        };

        Ok(Self {
            wal_path,
            wal_dir_files,
            latest_file,
            file_size,
        })
    }

    pub fn get_latest_file(&self) -> &Path {
        &self.latest_file
    }

    pub async fn rotate(&mut self) -> Result<(), WALError> {
        let timestamp = chrono::Utc::now().timestamp();
        let file_name = self.wal_path.join(format!("wal_{}", timestamp));
        let _file = fs::File::create(&file_name).await?;
        self.wal_dir_files.push(file_name.clone());
        Ok(())
    }

    pub async fn timed_cleanup(
        &mut self,
        to_point_in_time: i64,
    ) -> Result<DeletedFilesCount, WALError> {
        let files_to_delete: Vec<&PathBuf> = self
            .wal_dir_files
            .iter()
            .filter(|file_path| {
                file_path
                    .file_name()
                    .and_then(|f| f.to_str())
                    .and_then(|file_name| file_name.split_once('_'))
                    .and_then(|(_, ts_str)| ts_str.parse::<i64>().ok())
                    .map_or(false, |ts| ts < to_point_in_time)
            })
            .collect();

        if files_to_delete.len() == 0 {
            return Ok(0);
        }
        let mut delete_futures = FuturesUnordered::new();
        let mut removed_files_counter: DeletedFilesCount = 0;
        let mut success_deleted_files = vec![];

        for file in files_to_delete {
            let filepath_clone = file.to_owned();
            delete_futures.push(async move {
                fs::remove_file(&filepath_clone)
                    .await
                    .map(|_| filepath_clone)
            })
        }

        while let Some(result) = delete_futures.next().await {
            match result {
                Ok(file) => {
                    removed_files_counter += 1;
                    success_deleted_files.push(file);
                }
                Err(e) => println!("Err deleting file: {}", e),
            }
        }

        self.wal_dir_files
            .retain(|file_path| !success_deleted_files.contains(file_path));

        if self.wal_dir_files.len() == 0 {
            self.rotate().await?;
        }

        Ok(removed_files_counter)
    }

    pub async fn size_rotate(&mut self) -> Result<Option<File>, WALError> {
        let file_size = fs::metadata(&self.latest_file).await?.len();
        if file_size >= self.file_size {
            self.rotate().await?;
            return Ok(Some(fs::File::open(&self.latest_file).await?));
        }

        Ok(None)
    }

    async fn get_file_handler(&self) -> Result<File, WALError> {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&self.latest_file)
            .await?;

        Ok(file)
    }
}

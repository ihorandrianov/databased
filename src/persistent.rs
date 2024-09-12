use std::path::PathBuf;

use crate::errors::PersistentLayerError;

pub struct PersistentLayer<'a> {
    dir: &'a PathBuf,
}

impl<'a> PersistentLayer<'a> {
    pub fn new(dir: &'a PathBuf) -> Self {
        Self { dir }
    }

    pub fn get_dir_contents(&self) -> Result<Vec<PathBuf>, PersistentLayerError> {
        let contents = std::fs::read_dir(self.dir)?;
        let mut paths = Vec::new();
        for entry in contents {
            let entry = entry?;
            paths.push(entry.path());
        }

        Ok(paths)
    }
}

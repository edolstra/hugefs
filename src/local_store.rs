use crate::hash::Hash;
use crate::store::Store;
use std::io::Write;
use std::path::PathBuf;
use log::debug;
use futures::{future::FutureExt, compat::Future01CompatExt};

pub struct LocalStore {
    root: PathBuf,
}

impl LocalStore {
    pub fn new(root: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn path_for_hash(&self, file_hash: &Hash) -> PathBuf {
        let mut path = self.root.clone();
        path.push(file_hash.to_hex());
        path
    }
}

impl Store for LocalStore {
    fn add(&self, data: &[u8]) -> std::io::Result<Hash> {
        let hash = Hash::hash(data)?;

        let path = self.path_for_hash(&hash);

        if !path.exists() {
            // FIXME: make atomic
            debug!("writing {:?}", path);
            let mut file = std::fs::File::create(&path)?;
            file.write_all(data)?
        }

        Ok(hash)
    }

    fn get<'a>(&'a self, file_hash: &Hash, offset: u64, size: u32) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<Vec<u8>>> + Send + 'a>> {
        let file_hash = file_hash.clone();
        async move {
            let path = self.path_for_hash(&file_hash);
            let file = tokio::fs::File::open(path).compat().await?;
            let (file, _) = file.seek(std::io::SeekFrom::Start(offset)).compat().await?;
            let (_, mut buf, n) = tokio::io::read(file, vec![0; size as usize]).compat().await?;
            assert!(n <= size as usize);
            buf.resize(n, 0);
            Ok(buf)
        }.boxed()
    }
}

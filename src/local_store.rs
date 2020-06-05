use crate::{
    error::Error,
    hash::Hash,
    store::{Config, Future, Result, Store},
    types::MutableFileId,
};
use log::debug;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct LocalStore {
    root: PathBuf,
    config: Config,
}

impl LocalStore {
    pub fn new(root: PathBuf) -> std::io::Result<Self> {
        let root = root.canonicalize()?;

        std::fs::create_dir_all(root.join("mutable"))?;
        std::fs::create_dir_all(root.join("ca"))?;

        let mut config_file: PathBuf = root.clone();
        config_file.push("store-config.json");

        let mut config_json = String::new();
        File::open(config_file)?.read_to_string(&mut config_json)?;

        let config = serde_json::from_str(&config_json).unwrap(); // FIXME

        Ok(Self { root, config })
    }

    fn make_mutable_file_path(&self, id: &MutableFileId) -> PathBuf {
        self.root.clone().join("mutable").join(id)
    }

    fn make_new_id(&self) -> MutableFileId {
        format!(
            "{}.{}",
            process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }
}

fn path_for_hash(root: impl Into<PathBuf>, file_hash: &Hash) -> PathBuf {
    root.into().join("ca").join(file_hash.to_hex())
}

async fn read_n<R: tokio::io::AsyncReadExt + std::marker::Unpin>(
    from: &mut R,
    mut buf: &mut [u8],
) -> std::io::Result<usize> {
    let mut n = 0;
    while !buf.is_empty() {
        let n2 = from.read(buf).await?;
        if n2 == 0 {
            break;
        }
        n += n2;
        buf = &mut buf[n2..];
    }
    Ok(n)
}

impl Store for LocalStore {
    fn get_config(&self) -> Result<Config> {
        Ok(self.config.clone())
    }

    fn get_url(&self) -> String {
        self.root.to_str().unwrap().into()
    }

    fn add<'a>(&'a self, file_hash: &Hash, data: &'a [u8]) -> Future<'a, ()> {
        let file_hash = file_hash.clone();
        let path = path_for_hash(&self.root, &file_hash);
        Box::pin(async move {
            if !path.exists() {
                // FIXME: make atomic
                debug!("Writing {}.", path.display());
                let mut file = tokio::fs::File::create(path).await?;
                file.write_all(data).await?;
            }
            Ok(())
        })
    }

    fn has<'a>(&'a self, file_hash: &Hash) -> Future<'a, bool> {
        let file_hash = file_hash.clone();
        Box::pin(async move {
            let path = path_for_hash(&self.root, &file_hash);
            Ok(path.exists())
        })
    }

    fn get<'a>(
        &'a self,
        file_hash: &Hash,
        offset: u64,
        size: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send + 'a>> {
        let file_hash = file_hash.clone();
        Box::pin(async move {
            let path = path_for_hash(&self.root, &file_hash);
            let mut file = tokio::fs::File::open(path).await.map_err(|err| {
                if err.kind() == std::io::ErrorKind::NotFound {
                    Error::NoSuchHash(file_hash.clone())
                } else {
                    Error::StorageError(Box::new(err))
                }
            })?;
            file.seek(std::io::SeekFrom::Start(offset)).await?;
            let mut buf = vec![0u8; size as usize];
            let n = read_n(&mut file, &mut buf).await?;
            assert!(n <= size as usize);
            buf.resize(n, 0);
            Ok(buf)
        })
    }

    fn create_file<'a>(&'a self) -> Option<Future<'a, Box<dyn crate::store::MutableFile>>> {
        Some(Box::pin(async move {
            let id = self.make_new_id();
            let path = self.make_mutable_file_path(&id);
            let file = tokio::fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(path.clone())
                .await?;
            let handle: Box<dyn crate::store::MutableFile> = Box::new(MutableFile {
                path,
                root: self.root.clone(),
                file: futures::lock::Mutex::new(Some(file)),
                len: AtomicU64::new(0),
                keep: false,
            });
            Ok(handle)
        }))
    }

    fn open_file<'a>(
        &'a self,
        id: &crate::types::MutableFileId,
    ) -> Option<Future<'a, Box<dyn crate::store::MutableFile>>> {
        let path = self.make_mutable_file_path(&id);
        if !path.exists() {
            return None;
        }

        Some(Box::pin(async move {
            let file = tokio::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path.clone())
                .await?;
            let handle: Box<dyn crate::store::MutableFile> = Box::new(MutableFile {
                path,
                root: self.root.clone(),
                file: futures::lock::Mutex::new(Some(file)),
                len: AtomicU64::new(0),
                keep: true,
            });
            Ok(handle)
        }))
    }
}

struct MutableFile {
    path: PathBuf,
    root: PathBuf,
    file: futures::lock::Mutex<Option<tokio::fs::File>>,
    len: AtomicU64,
    keep: bool,
}

impl Drop for MutableFile {
    fn drop(&mut self) {
        if !self.keep {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl crate::store::MutableFile for MutableFile {
    fn get_id(&self) -> MutableFileId {
        self.path.file_name().unwrap().to_str().unwrap().into()
    }

    fn write<'a>(&'a self, offset: u64, data: &'a [u8]) -> Future<'a, ()> {
        Box::pin(async move {
            let mut file_lock = self.file.lock().await;
            if let Some(mut file) = file_lock.take() {
                file.seek(std::io::SeekFrom::Start(offset)).await?;
                file.write_all(data).await?;
                *file_lock = Some(file);
                self.len
                    .fetch_max(offset + data.len() as u64, Ordering::Relaxed);
                Ok(())
            } else {
                panic!("write handle invalidated by previous write error") // FIXME: return error
            }
        })
    }

    fn read<'a>(&'a self, offset: u64, size: u32) -> Future<'a, Vec<u8>> {
        Box::pin(async move {
            let mut file_lock = self.file.lock().await;
            if let Some(mut file) = file_lock.take() {
                file.seek(std::io::SeekFrom::Start(offset)).await?;
                let mut buf = vec![0u8; size as usize];
                let n = read_n(&mut file, &mut buf).await?; // FIXME
                *file_lock = Some(file);
                assert!(n <= size as usize);
                buf.resize(n, 0);
                Ok(buf)
            } else {
                panic!("write handle invalidated by previous write error") // FIXME: return error
            }
        })
    }

    fn finish<'a>(&'a self) -> Future<'a, (u64, Hash)> {
        Box::pin(async move {
            let mut file_lock = self.file.lock().await;
            if let Some(mut file) = file_lock.take() {
                file.seek(std::io::SeekFrom::Start(0)).await?;
                // FIXME: make this async and in bounded memory
                let mut buf = vec![];
                file.read_to_end(&mut buf).await?;
                let (len, hash) = Hash::hash(&buf[..])?;
                let final_path = path_for_hash(&self.root, &hash);
                if final_path.exists() {
                    tokio::fs::remove_file(self.path.clone()).await?;
                } else {
                    tokio::fs::rename(self.path.clone(), final_path).await?;
                }
                Ok((len, hash))
            } else {
                panic!("write handle invalidated by previous write error") // FIXME: return error
            }
        })
    }

    fn len(&self) -> u64 {
        self.len.load(Ordering::Relaxed)
    }

    fn keep(&mut self) {
        self.keep = true;
    }

    fn set_file_length<'a>(&'a self, length: u64) -> Future<'a, ()> {
        Box::pin(async move {
            let mut file_lock = self.file.lock().await;
            if let Some(mut file) = file_lock.take() {
                file.set_len(length).await?;
            } else {
                panic!("write handle invalidated by previous write error") // FIXME: return error
            }
            Ok(())
        })
    }
}

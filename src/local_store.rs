use crate::error::Error;
use crate::hash::Hash;
use crate::store::{Config, Future, Result, Store};
use log::debug;
use std::fs::File;
use std::io::{Read, Write};
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

        let mut config_file: PathBuf = root.clone();
        config_file.push("store-config.json");

        let mut config_json = String::new();
        File::open(config_file)?.read_to_string(&mut config_json)?;

        let config = serde_json::from_str(&config_json).unwrap(); // FIXME

        Ok(Self { root, config })
    }

    fn make_temp_path(&self) -> PathBuf {
        let mut path = self.root.clone();
        path.push(format!(
            "temp.{}.{}",
            process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        path
    }
}

fn path_for_hash(root: impl Into<PathBuf>, file_hash: &Hash) -> PathBuf {
    let mut path: PathBuf = root.into();
    path.push(file_hash.to_hex());
    path
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

    fn add(&self, data: &[u8]) -> Result<Hash> {
        let (_, hash) = Hash::hash(data)?;

        let path = path_for_hash(&self.root, &hash);

        if !path.exists() {
            // FIXME: make atomic
            debug!("writing {:?}", path);
            let mut file = std::fs::File::create(&path)?;
            file.write_all(data)?
        }

        Ok(hash)
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
            let temp_path = self.make_temp_path();
            let file = tokio::fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(temp_path.clone())
                .await?;
            let handle: Box<dyn crate::store::MutableFile> = Box::new(MutableFile {
                temp_path,
                file: futures::lock::Mutex::new(Some(file)),
                len: AtomicU64::new(0),
            });
            Ok(handle)
        }))
    }
}

struct MutableFile {
    temp_path: PathBuf,
    file: futures::lock::Mutex<Option<tokio::fs::File>>,
    len: AtomicU64,
}

impl Drop for MutableFile {
    fn drop(&mut self) {
        // FIXME: only do this when necessary
        let _ = std::fs::remove_file(&self.temp_path);
    }
}

impl crate::store::MutableFile for MutableFile {
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
                let final_path = path_for_hash(self.temp_path.clone().parent().unwrap(), &hash);
                if final_path.exists() {
                    tokio::fs::remove_file(self.temp_path.clone()).await?;
                } else {
                    tokio::fs::rename(self.temp_path.clone(), final_path).await?;
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
}

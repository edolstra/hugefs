use crate::{error::Error, hash::Hash, types::MutableFileId};
use serde::Deserialize;
use std::convert::TryFrom;

pub type Result<T> = std::result::Result<T, Error>;

pub type Future<'a, Res> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Res>> + Send + 'a>>;

pub trait Store: Send + Sync {
    fn add<'a>(&'a self, file_hash: &Hash, data: &'a [u8]) -> Future<'a, ()>;

    fn has<'a>(&'a self, file_hash: &Hash) -> Future<'a, bool>;

    fn get<'a>(&'a self, file_hash: &Hash, offset: u64, size: usize) -> Future<'a, Vec<u8>>;

    fn create_file<'a>(&'a self) -> Option<Future<'a, Box<dyn MutableFile>>>;

    fn open_file<'a>(&'a self, id: &MutableFileId) -> Option<Future<'a, Box<dyn MutableFile>>>;

    fn get_config(&self) -> Result<Config> {
        Ok(Config::default())
    }

    fn get_url(&self) -> String;
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    pub key_fingerprint: Option<crate::encrypted_store::KeyFingerprint>,
}

pub trait MutableFile: Send + Sync {
    fn get_id(&self) -> MutableFileId;

    fn write<'a>(&'a self, offset: u64, data: &'a [u8]) -> Future<'a, ()>;

    fn read<'a>(&'a self, offset: u64, size: u32) -> Future<'a, Vec<u8>>;

    fn finish<'a>(&'a self) -> Future<'a, (u64, Hash)>;

    fn len(&self) -> u64;

    fn keep(&mut self);

    fn set_file_length<'a>(&'a self, length: u64) -> Future<'a, ()>;
}

pub async fn copy_file(
    file_hash: &Hash,
    size: u64,
    src_store: &dyn Store,
    dst_store: &dyn Store,
) -> Result<()> {
    // FIXME: copy in smaller chunks, or stream directly from src_store to dst_store.

    let data = src_store
        .get(file_hash, 0, usize::try_from(size).unwrap())
        .await?;

    dst_store.add(file_hash, &data).await?;

    Ok(())
}

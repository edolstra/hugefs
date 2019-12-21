use crate::error::Error;
use crate::hash::Hash;
use serde::Deserialize;

pub type Result<T> = std::result::Result<T, Error>;

pub type Future<'a, Res> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Res>> + Send + 'a>>;

pub trait Store: Send + Sync {
    fn add(&self, data: &[u8]) -> Result<Hash>;

    fn get<'a>(&'a self, file_hash: &Hash, offset: u64, size: u32) -> Future<'a, Vec<u8>>;

    fn create_file<'a>(&'a self) -> Option<Future<'a, Box<dyn MutableFile>>>;

    fn get_config(&self) -> Result<Config> {
        Ok(Config::default())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    pub key_fingerprint: Option<crate::encrypted_store::KeyFingerprint>,
}

pub trait MutableFile: Send + Sync {
    fn write<'a>(&'a self, offset: u64, data: &'a [u8]) -> Future<'a, ()>;

    fn read<'a>(&'a self, offset: u64, size: u32) -> Future<'a, Vec<u8>>;

    fn finish<'a>(&'a self) -> Future<'a, (u64, Hash)>;

    fn len(&self) -> u64;
}

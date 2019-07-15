use crate::hash::Hash;

pub trait Store : Send + Sync {
    fn add(&self, data: &[u8]) -> std::io::Result<Hash>;

    fn get<'a>(&'a self, file_hash: &Hash, offset: u64, size: u32) ->
        std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<Vec<u8>>> + Send + 'a>>;
}

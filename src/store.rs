use crate::hash::Hash;

pub trait Store {
    fn add(&mut self, data: &[u8]) -> std::io::Result<Hash>;

    fn get(&mut self, file_hash: &Hash, offset: u64, size: u32) -> std::io::Result<Vec<u8>>;
}

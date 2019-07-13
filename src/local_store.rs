use crate::hash::Hash;
use crate::store::Store;
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use log::debug;

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
    fn add(&mut self, data: &[u8]) -> std::io::Result<Hash> {
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

    fn get(&mut self, file_hash: &Hash, offset: u64, size: u32) -> std::io::Result<Vec<u8>> {
        let path = self.path_for_hash(&file_hash);
        let file = std::fs::File::open(&path)?;
        let mut buf = vec![0; size as usize];
        let n = nix::sys::uio::pread(file.as_raw_fd(), buf.as_mut_slice(), offset as i64)?;
        buf.resize(n, 0);
        Ok(buf)
    }
}

use crate::hash::Hash;
use crate::store::{Future, MutableFile, Result, Store};
use aes_ctr::stream_cipher::generic_array::GenericArray;
use aes_ctr::stream_cipher::{NewStreamCipher, SyncStreamCipher, SyncStreamCipherSeek};
use aes_ctr::Aes256Ctr;
use log::debug;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct Key(pub GenericArray<u8, <Aes256Ctr as NewStreamCipher>::KeySize>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeyFingerprint(pub Hash);

impl Key {
    pub fn from_file(key_file: &Path) -> std::result::Result<Self, std::io::Error> {
        let mut key = vec![];
        File::open(key_file)?.read_to_end(&mut key)?;
        Ok(Key(GenericArray::clone_from_slice(&key)))
    }

    pub fn fingerprint(&self) -> KeyFingerprint {
        KeyFingerprint(Hash::hash(&self.0[..]).unwrap().1)
    }
}

impl<'de> serde::Deserialize<'de> for KeyFingerprint {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self(Hash::from_hex(&String::deserialize(deserializer)?)))
    }
}

pub struct EncryptedStore {
    inner: Arc<dyn Store>,
    key: Key,
}

impl EncryptedStore {
    pub fn new(inner: Arc<dyn Store>, key: Key) -> Self {
        Self { inner, key }
    }
}

impl Store for EncryptedStore {
    fn add(&self, data: &[u8]) -> Result<Hash> {
        unimplemented!()
    }

    fn get<'a>(&'a self, file_hash: &Hash, offset: u64, size: u32) -> Future<'a, Vec<u8>> {
        let file_hash = file_hash.clone();

        Box::pin(async move {
            /* We use the file hash as the IV/nonce. This is safe
             * because by definition this nonce will only be used to
             * encrypt *this* file. */
            let iv = GenericArray::from_slice(&file_hash.0[0..16]);

            let mut cipher = Aes256Ctr::new(&self.key.0, &iv);

            let encrypted_file_hash = {
                let mut h = file_hash.clone();
                cipher.apply_keystream(&mut h.0);
                h
            };

            debug!(
                "mapped hash {} -> {}",
                file_hash.to_hex(),
                encrypted_file_hash.to_hex()
            );

            let mut data = self.inner.get(&encrypted_file_hash, offset, size).await?;

            /* Note: we shift the counter to prevent reusing the nonce
             * used to encrypt the hash above. */
            assert_eq!(file_hash.0.len(), 64);
            cipher.seek(offset + file_hash.0.len() as u64);
            cipher.apply_keystream(&mut data);

            Ok(data)
        })
    }

    fn create_file<'a>(&'a self) -> Option<Future<'a, Box<dyn MutableFile>>> {
        None
    }
}

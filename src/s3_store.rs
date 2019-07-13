use crate::hash::Hash;
use crate::store::Store;
use rusoto_core::Region;
use rusoto_s3::{S3, S3Client, GetObjectRequest};
use std::io::Read;
use log::debug;

pub struct S3Store {
    s3_client: S3Client,
    bucket_name: String,
}

impl S3Store {

    pub fn open(bucket_name: &str) -> Self {
        let s3_client = S3Client::new(Region::EuWest1);

        Self {
            s3_client,
            bucket_name: bucket_name.into(),
        }
    }

    fn key_for_hash(&self, file_hash: &Hash) -> String {
        format!("plain/{}", file_hash.to_hex())
    }
}

impl Store for S3Store {
    fn add(&mut self, _data: &[u8]) -> std::io::Result<Hash> {
        unimplemented!()
    }

    fn get(&mut self, file_hash: &Hash, offset: u64, size: u32) -> std::io::Result<Vec<u8>> {
        assert!(size > 0);
        let key = self.key_for_hash(file_hash);
        debug!("GET s3://{}/{}", self.bucket_name, key);
        match self.s3_client.get_object(GetObjectRequest {
            bucket: self.bucket_name.clone(),
            key,
            range: Some(format!("bytes={}-{}", offset, offset + (size as u64) - 1)),
            ..Default::default()
        }).sync() {
            Ok(res) => {
                let mut buf = Vec::new();
                res.body.unwrap().into_blocking_read().read_to_end(&mut buf)?;
                assert!(buf.len() <= size as usize);
                Ok(buf)
            },
            Err(err) => panic!(err)
        }
    }
}


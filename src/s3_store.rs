use crate::error::Error;
use crate::hash::Hash;
use crate::store::{Future, Result, Store};
use log::debug;
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, S3Client, S3};
use tokio::io::AsyncReadExt;

pub struct S3Store {
    s3_client: S3Client,
    bucket_name: String,
}

impl S3Store {
    pub fn new(bucket_name: &str) -> Self {
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
    fn get_url(&self) -> String {
        format!("s3://{}", self.bucket_name)
    }

    fn add<'a>(&'a self, _file_hash: &Hash, _data: &'a [u8]) -> Future<'a, ()> {
        unimplemented!()
    }

    fn has<'a>(&'a self, _file_hash: &Hash) -> Future<'a, bool> {
        unimplemented!()
    }

    fn get<'a>(
        &'a self,
        file_hash: &Hash,
        offset: u64,
        size: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send + 'a>> {
        assert!(size > 0);
        let file_hash = file_hash.clone();
        let key = self.key_for_hash(&file_hash);
        debug!("GET s3://{}/{}", self.bucket_name, key);
        Box::pin(async move {
            match self
                .s3_client
                .get_object(GetObjectRequest {
                    bucket: self.bucket_name.clone(),
                    key,
                    range: Some(format!("bytes={}-{}", offset, offset + (size as u64) - 1)),
                    ..Default::default()
                })
                .await
            {
                Ok(res) => {
                    let mut r = res.body.unwrap().into_async_read();
                    let mut buf = Vec::with_capacity(size as usize);
                    r.read_to_end(&mut buf).await?;
                    assert!(buf.len() <= size as usize);
                    Ok(buf)
                }
                Err(rusoto_core::RusotoError::Service(rusoto_s3::GetObjectError::NoSuchKey(
                    ..,
                ))) => Err(Error::NoSuchHash(file_hash.clone())),
                Err(err) => Err(Error::StorageError(Box::new(err))),
            }
        })
    }

    fn create_file<'a>(&'a self) -> Option<Future<'a, Box<dyn crate::store::MutableFile>>> {
        unimplemented!()
    }

    fn open_file<'a>(
        &'a self,
        _id: &crate::types::MutableFileId,
    ) -> Option<Future<'a, Box<dyn crate::store::MutableFile>>> {
        None
    }
}

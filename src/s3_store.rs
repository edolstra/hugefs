use crate::hash::Hash;
use crate::store::Store;
use futures::{compat::Future01CompatExt, future::FutureExt};
use log::debug;
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, S3Client, S3};

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
    fn add(&self, _data: &[u8]) -> std::io::Result<Hash> {
        unimplemented!()
    }

    fn get<'a>(
        &'a self,
        file_hash: &Hash,
        offset: u64,
        size: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<Vec<u8>>> + Send + 'a>>
    {
        assert!(size > 0);
        let key = self.key_for_hash(file_hash);
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
                .compat()
                .await
            {
                Ok(res) => {
                    let r = res.body.unwrap().into_async_read();
                    let (_, buf) = tokio::io::read_to_end(r, Vec::with_capacity(size as usize))
                        .compat()
                        .await?;
                    assert!(buf.len() <= size as usize);
                    Ok(buf)
                }
                Err(err) => panic!(err), // FIXME
            }
        })
    }
}

use fuse::FileAttr;
use futures::{FutureExt, TryFutureExt};
use libc::c_int;
use std::time::Duration;

pub const FOPEN_KEEP_CACHE: u32 = 1 << 1;

pub struct FuseError(c_int);

impl From<c_int> for FuseError {
    fn from(err: c_int) -> Self {
        Self(err)
    }
}

pub fn wrap_read(
    executor: &tokio::runtime::TaskExecutor,
    reply: fuse::ReplyData,
    fut: impl std::future::Future<Output = Result<Vec<u8>, FuseError>> + Send + 'static,
) {
    executor.spawn(
        async {
            match fut.await {
                Ok(data) => reply.data(&data),
                Err(err) => reply.error(err.0),
            }
        }
            .unit_error()
            .boxed()
            .compat(),
    );
}

pub fn wrap_write(
    executor: &tokio::runtime::TaskExecutor,
    reply: fuse::ReplyWrite,
    fut: impl std::future::Future<Output = Result<u32, FuseError>> + Send + 'static,
) {
    executor.spawn(
        async {
            match fut.await {
                Ok(n) => reply.written(n),
                Err(err) => reply.error(err.0),
            }
        }
            .unit_error()
            .boxed()
            .compat(),
    );
}

pub fn wrap_release(
    executor: &tokio::runtime::TaskExecutor,
    reply: fuse::ReplyEmpty,
    fut: impl std::future::Future<Output = Result<(), FuseError>> + Send + 'static,
) {
    executor.spawn(
        async {
            match fut.await {
                Ok(()) => reply.ok(),
                Err(err) => reply.error(err.0),
            }
        }
            .unit_error()
            .boxed()
            .compat(),
    );
}

pub struct CreateOk {
    pub ttl: Duration,
    pub attr: FileAttr,
    pub generation: u64,
    pub fh: u64,
    pub flags: u32,
}

pub fn wrap_create(
    executor: &tokio::runtime::TaskExecutor,
    reply: fuse::ReplyCreate,
    fut: impl std::future::Future<Output = Result<CreateOk, FuseError>> + Send + 'static,
) {
    executor.spawn(
        async {
            match fut.await {
                Ok(data) => {
                    reply.created(&data.ttl, &data.attr, data.generation, data.fh, data.flags)
                }
                Err(err) => reply.error(err.0),
            }
        }
            .unit_error()
            .boxed()
            .compat(),
    );
}

use fuse::FileAttr;
use libc::c_int;
use std::time::Duration;

pub const FOPEN_KEEP_CACHE: u32 = 1 << 1;

pub struct FuseError(c_int);

type Result<T> = std::result::Result<T, FuseError>;

impl From<c_int> for FuseError {
    fn from(err: c_int) -> Self {
        Self(err)
    }
}

pub fn wrap_attr(
    executor: &tokio::runtime::Handle,
    reply: fuse::ReplyAttr,
    fut: impl std::future::Future<Output = Result<(Duration, FileAttr)>> + Send + 'static,
) {
    executor.spawn(
        async {
            match fut.await {
                Ok(attr) => reply.attr(&attr.0, &attr.1),
                Err(err) => reply.error(err.0),
            }
        }
    );
}

pub struct EntryOk {
    pub ttl: Duration,
    pub attr: FileAttr,
    pub generation: u64,
}

pub fn wrap_entry(
    executor: &tokio::runtime::Handle,
    reply: fuse::ReplyEntry,
    fut: impl std::future::Future<Output = Result<EntryOk>> + Send + 'static,
) {
    executor.spawn(
        async {
            match fut.await {
                Ok(entry) => reply.entry(&entry.ttl, &entry.attr, entry.generation),
                Err(err) => reply.error(err.0),
            }
        }
    );
}

pub fn wrap_read(
    executor: &tokio::runtime::Handle,
    reply: fuse::ReplyData,
    fut: impl std::future::Future<Output = Result<Vec<u8>>> + Send + 'static,
) {
    executor.spawn(
        async {
            match fut.await {
                Ok(data) => reply.data(&data),
                Err(err) => reply.error(err.0),
            }
        }
    );
}

pub fn wrap_write(
    executor: &tokio::runtime::Handle,
    reply: fuse::ReplyWrite,
    fut: impl std::future::Future<Output = Result<u32>> + Send + 'static,
) {
    executor.spawn(
        async {
            match fut.await {
                Ok(n) => reply.written(n),
                Err(err) => reply.error(err.0),
            }
        }
    );
}

pub fn wrap_empty(
    executor: &tokio::runtime::Handle,
    reply: fuse::ReplyEmpty,
    fut: impl std::future::Future<Output = Result<()>> + Send + 'static,
) {
    executor.spawn(
        async {
            match fut.await {
                Ok(()) => reply.ok(),
                Err(err) => reply.error(err.0),
            }
        }
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
    executor: &tokio::runtime::Handle,
    reply: fuse::ReplyCreate,
    fut: impl std::future::Future<Output = Result<CreateOk>> + Send + 'static,
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
    );
}

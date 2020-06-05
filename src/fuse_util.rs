use crate::error::{Error, Result};
use fuse::FileAttr;
use libc::c_int;
use log::debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub const FOPEN_KEEP_CACHE: u32 = 1 << 1;

pub struct FuseError(c_int);

/*
type Result<T> = std::result::Result<T, FuseError>;

impl From<c_int> for FuseError {
    fn from(err: c_int) -> Self {
        Self(err)
    }
}
*/

impl From<&Error> for FuseError {
    fn from(err: &Error) -> Self {
        FuseError(match err {
            Error::NoSuchInode(_) => libc::ENXIO,
            Error::NoSuchEntry => libc::ENOENT,
            Error::EntryExists => libc::EEXIST,
            Error::NotDirectory(_) => libc::ENOTDIR,
            Error::IsDirectory(_) => libc::EISDIR,
            Error::NotEmpty(_) => libc::ENOTEMPTY,
            Error::NotMutableFile(_) => libc::EPERM,
            Error::NotSymlink(_) => libc::EINVAL,
            Error::BadFileHandle(_) => libc::ENXIO, // denotes a kernel bug
            Error::NoSuchHash(_) => libc::ENOMEDIUM,
            Error::NoSuchMutableFile(_) => libc::ENOMEDIUM,
            Error::StorageError(_) => libc::EIO,
            Error::NoWritableStore => libc::EROFS,
            Error::ControlError(_) => libc::ENOTCONN,
            _ => libc::EIO,
        })
    }
}

fn maybe_log(err: &Error) -> c_int {
    debug!("Error: {}", err);
    let c_err = FuseError::from(err).0;
    c_err
}

pub fn wrap_attr(
    executor: &tokio::runtime::Handle,
    reply: fuse::ReplyAttr,
    fut: impl std::future::Future<Output = Result<(Duration, FileAttr)>> + Send + 'static,
) {
    executor.spawn(async {
        match fut.await {
            Ok(attr) => reply.attr(&attr.0, &attr.1),
            Err(err) => reply.error(maybe_log(&err)),
        }
    });
}

pub struct EntryOk {
    pub ttl: Duration,
    pub attr: FileAttr,
}

static GENERATION_COUNT: AtomicU64 = AtomicU64::new(0);

pub fn wrap_entry(
    executor: &tokio::runtime::Handle,
    reply: fuse::ReplyEntry,
    fut: impl std::future::Future<Output = Result<EntryOk>> + Send + 'static,
) {
    executor.spawn(async {
        match fut.await {
            Ok(entry) => reply.entry(
                &entry.ttl,
                &entry.attr,
                GENERATION_COUNT.fetch_add(1, Ordering::Relaxed),
            ),
            Err(err) => reply.error(maybe_log(&err)),
        }
    });
}

pub fn wrap_open(
    executor: &tokio::runtime::Handle,
    reply: fuse::ReplyOpen,
    fut: impl std::future::Future<Output = Result<(u64, u32)>> + Send + 'static,
) {
    executor.spawn(async {
        match fut.await {
            Ok((fh, flags)) => reply.opened(fh, flags),
            Err(err) => reply.error(maybe_log(&err)),
        }
    });
}

pub fn wrap_read(
    executor: &tokio::runtime::Handle,
    reply: fuse::ReplyData,
    fut: impl std::future::Future<Output = Result<Vec<u8>>> + Send + 'static,
) {
    executor.spawn(async {
        match fut.await {
            Ok(data) => reply.data(&data),
            Err(err) => reply.error(maybe_log(&err)),
        }
    });
}

pub fn wrap_write(
    executor: &tokio::runtime::Handle,
    reply: fuse::ReplyWrite,
    fut: impl std::future::Future<Output = Result<u32>> + Send + 'static,
) {
    executor.spawn(async {
        match fut.await {
            Ok(n) => reply.written(n),
            Err(err) => reply.error(maybe_log(&err)),
        }
    });
}

pub fn wrap_empty(
    executor: &tokio::runtime::Handle,
    reply: fuse::ReplyEmpty,
    fut: impl std::future::Future<Output = Result<()>> + Send + 'static,
) {
    executor.spawn(async {
        match fut.await {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(maybe_log(&err)),
        }
    });
}

pub struct CreateOk {
    pub ttl: Duration,
    pub attr: FileAttr,
    pub fh: u64,
    pub flags: u32,
}

pub fn wrap_create(
    executor: &tokio::runtime::Handle,
    reply: fuse::ReplyCreate,
    fut: impl std::future::Future<Output = Result<CreateOk>> + Send + 'static,
) {
    executor.spawn(async {
        match fut.await {
            Ok(data) => reply.created(
                &data.ttl,
                &data.attr,
                GENERATION_COUNT.fetch_add(1, Ordering::Relaxed),
                data.fh,
                data.flags,
            ),
            Err(err) => reply.error(maybe_log(&err)),
        }
    });
}

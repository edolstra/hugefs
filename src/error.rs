use crate::fs::Ino;
use crate::fuse_util::FuseError;

#[derive(Debug, Clone)]
pub enum Error {
    NoSuchInode(Ino),
    NotDirectory(Ino),
    BadFileHandle(u64),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for FuseError {
    fn from(err: Error) -> Self {
        match err {
            Error::NoSuchInode(_) => libc::ENXIO,
            Error::NotDirectory(_) => libc::ENOTDIR,
            Error::BadFileHandle(_) => libc::ENXIO, // denotes a kernel bug
        }
        .into()
    }
}

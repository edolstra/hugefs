use crate::fs::Ino;
use crate::fuse_util::FuseError;

#[derive(Debug)]
pub enum Error {
    NoSuchInode(Ino),
    NoSuchEntry,
    EntryExists,
    NotDirectory(Ino),
    NotImmutableFile(Ino),
    BadFileHandle(u64),
    NoSuchHash(crate::hash::Hash),
    StorageError(Box<dyn std::error::Error>),
    NoSuchKey(crate::encrypted_store::KeyFingerprint),
    BadControlRequest,
    BadControlResponse,
    ControlError(String),
    BadPath(std::path::PathBuf),
    NotHugefs,
    UnknownStore(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for FuseError {
    fn from(err: Error) -> Self {
        match err {
            Error::NoSuchInode(_) => libc::ENXIO,
            Error::NoSuchEntry => libc::ENOENT,
            Error::EntryExists => libc::EEXIST,
            Error::NotDirectory(_) => libc::ENOTDIR,
            Error::BadFileHandle(_) => libc::ENXIO, // denotes a kernel bug
            Error::NoSuchHash(_) => libc::ENOMEDIUM,
            Error::StorageError(_) => libc::EIO,
            _ => libc::EIO,
        }
        .into()
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::StorageError(Box::new(err))
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NoSuchInode(ino) => write!(f, "Inode {} does not exist.", ino),
            Error::NoSuchEntry => write!(f, "Directory entry does not exist."),
            Error::EntryExists => write!(f, "Directory entry already exists."),
            Error::NotDirectory(ino) => write!(f, "Inode {} is not a directory.", ino),
            Error::NotImmutableFile(ino) => write!(f, "Inode {} is not an immutable file.", ino),
            Error::BadFileHandle(n) => write!(f, "Bad file handle {}.", n),
            Error::NoSuchHash(hash) => {
                write!(f, "Cannot find file with content hash {}.", hash.to_hex())
            }
            Error::StorageError(err) => write!(f, "Storage error: {}", err.to_string()),
            Error::NoSuchKey(fp) => {
                write!(f, "Cannot find key with fingerprint {}.", fp.0.to_hex())
            }
            Error::BadControlRequest => write!(f, "Bad control request."),
            Error::BadControlResponse => write!(f, "Bad control response."),
            Error::ControlError(s) => write!(f, "Daemon error: {}", s),
            Error::BadPath(p) => write!(f, "Bad path '{:#?}'.", p),
            Error::NotHugefs => write!(f, "Path does not refer to a hugefs filesystem."),
            Error::UnknownStore(s) => write!(f, "Unknown store '{}'.", s),
        }
    }
}

use crate::types::Ino;

#[derive(Debug)]
pub enum Error {
    NoSuchInode(Ino),
    NoSuchEntry,
    EntryExists,
    NotDirectory(Ino),
    IsDirectory(Ino),
    NotEmpty(Ino),
    NotImmutableFile(Ino),
    NotMutableFile(Ino),
    NotSymlink(Ino),
    BadFileHandle(u64),
    NoSuchHash(crate::hash::Hash),
    NoSuchMutableFile(crate::types::MutableFileId),
    StorageError(Box<dyn std::error::Error>),
    NoWritableStore,
    NoSuchKey(crate::encrypted_store::KeyFingerprint),
    BadControlRequest,
    BadControlResponse,
    ControlError(String),
    ControlMisc(Box<dyn std::error::Error>),
    BadPath(std::path::PathBuf),
    NotHugefs,
    UnknownStore(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::StorageError(Box::new(err))
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Self::StorageError(Box::new(err))
    }
}

impl From<r2d2::Error> for Error {
    fn from(err: r2d2::Error) -> Self {
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
            Error::IsDirectory(ino) => write!(f, "Inode {} is a directory.", ino),
            Error::NotEmpty(ino) => write!(f, "Directory {} is not empty.", ino),
            Error::NotImmutableFile(ino) => write!(f, "Inode {} is not an immutable file.", ino),
            Error::NotMutableFile(ino) => write!(f, "Inode {} is not a mutable file.", ino),
            Error::NotSymlink(ino) => write!(f, "Inode {} is not a symlink.", ino),
            Error::BadFileHandle(n) => write!(f, "Bad file handle {}.", n),
            Error::NoSuchHash(hash) => {
                write!(f, "Cannot find file with content hash {}.", hash.to_hex())
            }
            Error::NoSuchMutableFile(id) => write!(f, "Cannot find mutable file with ID {}.", id),
            Error::StorageError(err) => write!(f, "Storage error: {}", err.to_string()),
            Error::NoWritableStore => write!(
                f,
                "Cannot create file because the filesystem does not have a writable store."
            ),
            Error::NoSuchKey(fp) => {
                write!(f, "Cannot find key with fingerprint {}.", fp.0.to_hex())
            }
            Error::BadControlRequest => write!(f, "Bad control request."),
            Error::BadControlResponse => write!(f, "Bad control response."),
            Error::ControlError(s) => write!(f, "Daemon error: {}", s),
            Error::ControlMisc(s) => write!(f, "Misc. control error: {}", s),
            Error::BadPath(p) => write!(f, "Bad path '{:#?}'.", p),
            Error::NotHugefs => write!(f, "Path does not refer to a hugefs filesystem."),
            Error::UnknownStore(s) => write!(f, "Unknown store '{}'.", s),
        }
    }
}

use crate::{
    error::{Error, Result},
    fs_sqlite::FileTypeInfo,
    fusefs::{FilesystemState, open_file},
    hash::Hash,
    types::{Ino, MutableFileId},
};
use log::debug;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Status { path: PathBuf },
    Mirror { path: PathBuf, store: String },
    Finalize { path: PathBuf },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Error { msg: String },
    Status(StatusResponse),
    Mirror(MirrorResponse),
    Finalize(FinalizeResponse),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    pub ino: Ino,
    pub info: FileType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MirrorResponse {
    pub from: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FinalizeResponse {}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FileType {
    Directory {},
    ImmutableFile {
        length: u64,
        hash: Hash,
        stores: Vec<String>,
    },
    MutableFile { length: u64, id: MutableFileId },
    Symlink {},
}

impl FileType {
    pub fn get_type(&self) -> &'static str {
        match self {
            Self::Directory { .. } => "directory",
            Self::ImmutableFile { .. } => "immutable",
            Self::MutableFile { .. } => "mutable",
            Self::Symlink { .. } => "symlink",
        }
    }
}

pub async fn handle_message(
    rx: tokio::sync::mpsc::UnboundedReceiver<u8>,
    fs: Arc<RwLock<FilesystemState>>,
) -> String {
    let res = match handle_inner(rx, fs).await {
        Ok(res) => res,
        Err(err) => Response::Error {
            msg: err.to_string(),
        },
    };
    let res = serde_json::to_string(&res).unwrap();
    debug!("Control response: {}", res);
    res
}

async fn handle_inner(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<u8>,
    fs: Arc<RwLock<FilesystemState>>,
) -> Result<Response> {
    let mut req = Vec::new();
    loop {
        let c = rx.recv().await.ok_or(Error::BadControlRequest)?;
        if c == '\n' as u8 {
            break;
        }
        req.push(c);
    }

    let req = String::from_utf8(req).map_err(|_| Error::BadControlRequest)?;

    debug!("Control request: {}", req);

    let req: Request = serde_json::from_str(&req).map_err(|_| Error::BadControlRequest)?;

    match req {
        Request::Status { path } => handle_status(&path, fs).await.map(|x| Response::Status(x)),
        Request::Mirror { path, store } => handle_mirror(&path, &store, fs)
            .await
            .map(|x| Response::Mirror(x)),
        Request::Finalize { path } => handle_finalize(&path, fs)
            .await
            .map(|x| Response::Finalize(x)),
    }
}

async fn handle_status(path: &Path, state: Arc<RwLock<FilesystemState>>) -> Result<StatusResponse> {
    let st = {
        let state = state.read().unwrap();
        state.fs.stat(state.fs.lookup_path(path)?)?
    };

    let info = match st.file_type {
        FileTypeInfo::MutableRegular { length, id } => FileType::MutableFile {
            length, id
        },
        FileTypeInfo::ImmutableRegular { length, hash } => FileType::ImmutableFile {
            length,
            hash: hash.clone(),
            stores: {
                let mut stores = vec![];
                let ss = state.read().unwrap().stores.clone();
                for store in ss {
                    if store.has(&hash).await? {
                        stores.push(store.get_url());
                    }
                }
                stores
            },
        },
        FileTypeInfo::Directory { .. } => FileType::Directory {},
        FileTypeInfo::Symlink { .. } => FileType::Symlink {},
    };

    Ok(StatusResponse { ino: st.ino, info })
}

async fn handle_mirror(
    path: &Path,
    store: &str,
    fs: Arc<RwLock<FilesystemState>>,
) -> Result<MirrorResponse> {
    /*
    let (hash, size, stores) = {
        let fs = fs.read().unwrap();
        let inode = fs.superblock.lookup_path(path)?;
        let inode = inode.read().unwrap();
        match &inode.contents {
            Contents::RegularFile(file) => (file.hash.clone(), file.length, fs.stores.clone()),
            _ => return Err(Error::NotImmutableFile(inode.ino)),
        }
    };

    let dst_store = stores
        .iter()
        .find(|st| st.get_url() == store)
        .ok_or_else(|| Error::UnknownStore(store.into()))?;

    if dst_store.has(&hash).await? {
        Ok(MirrorResponse { from: None })
    } else {
        for src_store in &stores {
            if Arc::ptr_eq(src_store, dst_store) {
                continue;
            }
            match crate::store::copy_file(&hash, size, src_store.as_ref(), dst_store.as_ref()).await
            {
                Ok(()) => {
                    return Ok(MirrorResponse {
                        from: Some(src_store.get_url()),
                    });
                }
                Err(Error::NoSuchHash(_)) => {}
                Err(err) => {
                    return Err(err);
                }
            }
        }
        Err(Error::NoSuchHash(hash))
    }
     */
    unimplemented!()
}

async fn handle_finalize(
    path: &Path,
    state: Arc<RwLock<FilesystemState>>,
) -> Result<FinalizeResponse> {
    let st = {
        let state = state.read().unwrap();
        state.fs.stat(state.fs.lookup_path(path)?)?
    };

    if let FileTypeInfo::MutableRegular { id, length } = st.file_type {
        let stores = state.read().unwrap().stores.clone();
        let mutable_file = open_file(stores, &id).await?;
        let (length2, hash) = mutable_file.finish().await?;
        assert_eq!(length, length2);
        state.read().unwrap().fs.finalize(st.ino, &hash)?;
    }

    Ok(FinalizeResponse {})
}

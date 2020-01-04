use crate::{
    error::{Error, Result},
    fs::{Contents, Ino},
    fusefs::FilesystemState,
    hash::Hash,
};
use log::debug;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Status { path: PathBuf },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Error { msg: String },
    Status(StatusResponse),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    pub ino: Ino,
    pub info: FileType,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FileType {
    Directory {},
    ImmutableFile {
        size: u64,
        hash: Hash,
        stores: Vec<String>,
    },
    MutableFile {},
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
    }
}

async fn handle_status(path: &Path, fs: Arc<RwLock<FilesystemState>>) -> Result<StatusResponse> {
    let mut status = {
        let inode = fs.read().unwrap().superblock.lookup_path(path)?;
        let inode = inode.read().unwrap();

        let info = match &inode.contents {
            Contents::Directory(_) => FileType::Directory {},
            Contents::RegularFile(file) => FileType::ImmutableFile {
                size: file.length,
                hash: file.hash.clone(),
                stores: vec![],
            },
            Contents::MutableFile(_) => FileType::MutableFile {},
            Contents::Symlink(_) => FileType::Symlink {},
        };

        StatusResponse {
            ino: inode.ino,
            info,
        }
    };

    if let FileType::ImmutableFile { stores, hash, .. } = &mut status.info {
        let ss = fs.read().unwrap().stores.clone();
        for store in ss {
            if store.has(hash).await? {
                stores.push(store.get_url());
            }
        }
    }

    Ok(status)
}

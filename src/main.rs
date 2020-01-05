#![feature(atomic_min_max)]

mod control;
mod encrypted_store;
mod error;
mod fs;
mod fuse_util;
mod fusefs;
mod hash;
mod local_store;
//mod s3_store;
mod store;

use crate::{
    control::{FileType, Request, Response},
    encrypted_store::{Key, KeyFingerprint},
    error::Error,
    store::Store,
};
use log::debug;
use std::collections::HashMap;
use std::ffi::OsString;
use std::io::{BufReader, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use structopt::StructOpt;
use tokio::runtime::Runtime;

#[derive(Debug, StructOpt)]
#[structopt(name = "hugefs", about = "Hugefs interface")]
enum CLI {
    /// Mount a hugefs filesystem
    #[structopt(name = "mount")]
    Mount {
        /// Filesystem state file
        state_file: PathBuf,

        /// Mount point
        mount_point: PathBuf,

        #[structopt(name = "store", short = "s", long = "store")]
        /// Backing stores
        stores: Vec<String>,

        #[structopt(name = "key", short = "k", long = "key")]
        /// Key files
        key_files: Vec<PathBuf>,
    },

    /// Get the status of a file
    #[structopt(name = "status")]
    Status { path: PathBuf },

    /// List files that have only one backing store
    #[structopt(name = "unmirrored")]
    Unmirrored { path: PathBuf },

    /// List files that have at least two backing stores
    #[structopt(name = "mirrored")]
    Mirrored { path: PathBuf },

    /// Copy a file to a backing store
    #[structopt(name = "mirror")]
    Mirror { path: PathBuf, store: String },
}

fn read_key_file(key_file: &Path) -> Result<(KeyFingerprint, Key), std::io::Error> {
    let key = Key::from_file(key_file)?;
    Ok((key.fingerprint(), key))
}

type Keys = HashMap<KeyFingerprint, Key>;

fn open_store(store_loc: &str, keys: &Keys) -> Result<Arc<dyn Store>, Error> {
    let mut store: Arc<dyn Store> = Arc::new(local_store::LocalStore::new(store_loc.into())?);

    let config = store.get_config()?;

    if let Some(key_fingerprint) = config.key_fingerprint {
        debug!(
            "Opening store '{}' using key with fingerprint {}.",
            store_loc,
            key_fingerprint.0.to_hex()
        );
        let key = keys
            .get(&key_fingerprint)
            .ok_or_else(|| Error::NoSuchKey(key_fingerprint))?;
        store = Arc::new(encrypted_store::EncryptedStore::new(store, key.clone()));
    }

    Ok(store)
}

fn mount(
    state_file: PathBuf,
    mount_point: PathBuf,
    stores: Vec<String>,
    key_files: Vec<PathBuf>,
) -> Result<(), Error> {
    let rt = Runtime::new().unwrap();

    let keys: Result<Keys, _> = key_files.iter().map(|k| read_key_file(k)).collect();
    let keys = keys?;

    let stores: Result<Vec<_>, _> = stores.iter().map(|s| open_store(s, &keys)).collect();
    let stores = stores?;

    let superblock = if state_file.exists() {
        fs::Superblock::open_from_json(&mut std::fs::File::open(&state_file).unwrap()).unwrap()
    } else {
        fs::Superblock::new()
    };

    let fs_state = Arc::new(RwLock::new(fusefs::FilesystemState::new(
        superblock, stores,
    )));

    let fs = fusefs::Filesystem::new(Arc::clone(&fs_state), rt.handle().clone());

    let s: OsString = "default_permissions".into();

    fuse::mount(fs, &mount_point, &vec![s.as_os_str()]).unwrap();

    drop(rt);

    fs_state.read().unwrap().sync(&state_file).unwrap();

    Ok(())
}

fn get_fs_root(path: &Path) -> Result<(PathBuf, PathBuf), Error> {
    let mut path = PathBuf::from(path);
    let mut sub: Vec<OsString> = vec![];

    loop {
        if path.clone().join(fusefs::CONTROL_NAME).exists() {
            let mut sub2 = PathBuf::new();
            for s in sub.iter().rev() {
                sub2 = sub2.join(s);
            }
            debug!("Found root '{}', sub '{}'.", path.display(), sub2.display());
            return Ok((path.into(), sub2));
        }
        if let Some(file_name) = path.file_name() {
            sub.push(file_name.into());
        }
        if !path.pop() {
            return Err(Error::NotHugefs);
        }
    }
}

fn execute_request(root: &Path, req: Request) -> Result<Response, Error> {
    let control_path = root.join(fusefs::CONTROL_NAME);

    let mut control_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(control_path)?;

    let mut req_s = serde_json::to_string(&req).unwrap();
    req_s.push('\n');

    control_file.write_all(req_s.as_bytes())?;

    control_file.seek(std::io::SeekFrom::Start(0))?;

    let res = serde_json::from_reader(BufReader::new(control_file))
        .map_err(|_| Error::BadControlResponse)?;

    debug!("Control response: {:?}", res);

    Ok(res)
}

fn status(path: &Path) -> Result<(), Error> {
    let (root, path) = get_fs_root(path)?;

    let req = Request::Status { path };

    match execute_request(&root, req)? {
        Response::Status(status) => {
            println!(" Type: {}", status.info.get_type());
            match status.info {
                FileType::ImmutableFile {
                    size, hash, stores, ..
                } => {
                    println!(" Size: {}", size);
                    println!(" Hash: {}", hash.to_hex());
                    for store in stores {
                        println!("Store: {}", store);
                    }
                }
                _ => {}
            }
        }
        Response::Error { msg } => return Err(Error::ControlError(msg)),
        _ => panic!("Unexpected daemon response."),
    }

    Ok(())
}

fn traverse(
    root: &Path,
    path: &Path,
    callback: &dyn Fn(&Path, usize) -> Result<(), Error>,
) -> Result<(), Error> {
    let req = Request::Status { path: path.into() };

    match execute_request(&root, req)? {
        Response::Status(status) => match status.info {
            FileType::Directory { .. } => {
                for entry in std::fs::read_dir(root.join(path))? {
                    let entry = entry.unwrap();
                    let file_name = entry.file_name();
                    if file_name == "." || file_name == ".." {
                        continue;
                    }
                    traverse(root, &path.join(file_name), callback)?;
                }
            }
            FileType::ImmutableFile { stores, .. } => {
                callback(&root.join(path), stores.len())?;
            }
            FileType::MutableFile { .. } => {
                callback(&root.join(path), 0)?;
            }
            FileType::Symlink { .. } => {}
        },
        Response::Error { msg } => return Err(Error::ControlError(msg)),
        _ => panic!("Unexpected daemon response."),
    }

    Ok(())
}

enum Mode {
    Unmirrored,
    Mirrored,
}

fn find_files(path: &Path, mode: Mode) -> Result<(), Error> {
    let (root, path) = get_fs_root(path)?;

    traverse(&root, &path, &|path: &Path,
                             store_count: usize|
     -> Result<(), Error> {
        if match &mode {
            Mode::Unmirrored => store_count < 2,
            Mode::Mirrored => store_count >= 2,
        } {
            println!("{}", root.join(path).display());
        }
        Ok(())
    })?;

    Ok(())
}

fn mirror(path: &Path, store: &str) -> Result<(), Error> {
    let (root, path) = get_fs_root(path)?;

    let req = Request::Mirror {
        path: path.into(),
        store: store.into(),
    };

    match execute_request(&root, req)? {
        Response::Mirror(_) => {}
        Response::Error { msg } => return Err(Error::ControlError(msg)),
        _ => panic!("Unexpected daemon response."),
    }

    Ok(())
}

fn main() -> Result<(), Error> {
    let _ = env_logger::try_init();

    match CLI::from_args() {
        CLI::Mount {
            state_file,
            mount_point,
            stores,
            key_files,
        } => {
            mount(state_file, mount_point, stores, key_files)?;
        }

        CLI::Status { path } => {
            status(&path)?;
        }

        CLI::Unmirrored { path } => {
            find_files(&path, Mode::Unmirrored)?;
        }

        CLI::Mirrored { path } => {
            find_files(&path, Mode::Mirrored)?;
        }

        CLI::Mirror { path, store } => {
            mirror(&path, &store)?;
        }
    }

    Ok(())
}

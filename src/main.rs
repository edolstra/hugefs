#![feature(atomic_min_max)]

mod encrypted_store;
mod error;
mod fs;
mod fuse_util;
mod fusefs;
mod hash;
mod local_store;
//mod s3_store;
mod store;

use crate::encrypted_store::{Key, KeyFingerprint};
use crate::error::Error;
use crate::store::Store;
use log::debug;
use std::collections::HashMap;
use std::ffi::OsString;
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
    }

    Ok(())
}

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

use aes_ctr::stream_cipher::generic_array::GenericArray;
use std::ffi::OsString;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, RwLock};
use tokio::runtime::Runtime;

fn main() -> Result<(), std::io::Error> {
    let _ = env_logger::try_init();

    let rt = Runtime::new().unwrap();

    //let store = local_store::LocalStore::new("/data/local-store".into()).unwrap();
    let store = local_store::LocalStore::new("/tmp/encrypted-store".into()).unwrap();
    //let store = s3_store::S3Store::open("hugefs");

    let mut key = vec![];
    File::open("key")?.read_to_end(&mut key)?;

    let store =
        encrypted_store::EncryptedStore::new(Arc::new(store), GenericArray::clone_from_slice(&key));

    let json_state = Path::new("/tmp/fs.json");

    let superblock = if json_state.exists() {
        fs::Superblock::open_from_json(&mut std::fs::File::open(&json_state).unwrap()).unwrap()
    } else {
        fs::Superblock::new()
    };

    let fs_state = Arc::new(RwLock::new(fusefs::FilesystemState::new(
        superblock,
        std::sync::Arc::new(store),
    )));

    let fs = fusefs::Filesystem::new(Arc::clone(&fs_state), rt.handle().clone());

    let s: OsString = "default_permissions".into();

    fuse::mount(fs, &"/home/eelco/mnt/tmp", &vec![s.as_os_str()]).unwrap();

    drop(rt);

    fs_state.read().unwrap().sync(json_state).unwrap();

    Ok(())
}

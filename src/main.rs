#![feature(await_macro, async_await, atomic_min_max)]

mod error;
mod fs;
mod fuse_util;
mod fusefs;
mod hash;
mod local_store;
mod s3_store;
mod store;

use std::ffi::OsString;
use std::path::Path;
use tokio::prelude::*;
use tokio::runtime::Runtime;

fn main() {
    let _ = env_logger::try_init();

    let rt = Runtime::new().unwrap();

    let store = local_store::LocalStore::new("/tmp/local-store".into()).unwrap();
    //let store = s3_store::S3Store::open("hugefs");

    let json_state = Path::new("/tmp/fs.json");

    let superblock = if json_state.exists() {
        fs::Superblock::open_from_json(&mut std::fs::File::open(&json_state).unwrap()).unwrap()
    } else {
        fs::Superblock::new()
    };

    let fs = fusefs::Filesystem::new(superblock, std::sync::Arc::new(store), rt.executor());

    let s: OsString = "default_permissions".into();

    fuse::mount(fs, &"/home/eelco/mnt/tmp", &vec![s.as_os_str()]).unwrap();

    rt.shutdown_on_idle().wait().unwrap();
}

mod fs;
mod fusefs;
mod hash;
mod store;
mod local_store;
mod s3_store;

use std::ffi::OsString;
use std::path::Path;

fn main() {
    let _ = env_logger::try_init();

    //let local_store = local_store::LocalStore::new("/tmp/local-store".into()).unwrap();
    let store = s3_store::S3Store::open("hugefs");

    let json_state = Path::new("/tmp/fs.json");

    let superblock = if json_state.exists() {
        fs::Superblock::open_from_json(&mut std::fs::File::open(&json_state).unwrap()).unwrap()
    } else {
        fs::Superblock::new()
    };

    let fs = fusefs::Filesystem::new(superblock, Box::new(store));

    let s: OsString = "default_permissions".into();

    println!("mounting...");

    fuse::mount(fs, &"/home/eelco/mnt/tmp", &vec![s.as_os_str()]).unwrap();
}

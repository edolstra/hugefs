mod fs;
mod fusefs;
mod hash;
mod local_store;
mod store;

use std::ffi::OsString;
use std::path::Path;

fn main() {
    let local_store = local_store::LocalStore::new("/tmp/local-store".into()).unwrap();

    let json_state = Path::new("/tmp/fs.json");

    let superblock = if json_state.exists() {
        fs::Superblock::open_from_json(&mut std::fs::File::open(&json_state).unwrap()).unwrap()
    } else {
        fs::Superblock::new()
    };

    let fs = fusefs::Filesystem::new(superblock, Box::new(local_store));

    let s: OsString = "default_permissions".into();

    println!("mounting...");

    fuse::mount(fs, &"/home/eelco/mnt/tmp", &vec![s.as_os_str()]).unwrap();
}

use crate::hash::Hash;
use crate::store::Store;
use libc;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::{SystemTime, Duration, UNIX_EPOCH};

pub type Ino = u64;

#[derive(Debug, Serialize, Deserialize)]
pub struct Time(pub i64);

impl From<&Time> for SystemTime {
    fn from(time: &Time) -> Self {
        SystemTime::UNIX_EPOCH + Duration::from_nanos(time.0 as u64)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Superblock {
    inodes: HashMap<Ino, Inode>,
    root_ino: Ino,
    next_ino: Ino,
}

impl Superblock {
    pub fn get_inode<'a>(self: &'a Self, ino: Ino) -> Option<&'a Inode> {
        self.inodes.get(&ino)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Inode {
    pub ino: u64,
    pub perm: libc::mode_t,
    pub uid: libc::uid_t,
    pub gid: libc::gid_t,
    pub crtime: Time,
    pub mtime: Time,
    pub contents: Contents,
    //parents: Vec<Ino>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Contents {
    Directory(Directory),
    RegularFile(RegularFile),
    Symlink(Symlink),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Directory {
    pub entries: BTreeMap<String, Ino>, // FIXME: include type?
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegularFile {
    pub length: u64,
    pub hash: Hash,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Symlink {
    pub target: String,
}

fn cur_time() -> Time {
    Time(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            * 1000000000,
    )
}

impl Superblock {
    pub fn new() -> Self {
        let root_ino = 1;
        let mut inodes = HashMap::new();
        inodes.insert(
            root_ino,
            Inode {
                ino: root_ino,
                perm: 0o700,
                uid: 0,
                gid: 0,
                crtime: cur_time(),
                mtime: cur_time(),
                contents: Contents::Directory(Directory {
                    entries: BTreeMap::new(),
                }),
                //parents: vec![],
            },
        );
        Self {
            inodes,
            root_ino,
            next_ino: root_ino + 1,
        }
    }

    pub fn open_from_json<R: Read>(json_data: &mut R) -> Result<Self, serde_json::error::Error> {
        serde_json::from_reader(json_data)
    }

    pub fn import<S: Store>(&mut self, path: &Path, store: &mut S) -> std::io::Result<()> {
        let file = self.import_file(path, store)?;
        let file_ino = file.ino;
        self.inodes.insert(file.ino, file);

        let root = self.inodes.get_mut(&self.root_ino).unwrap();

        match &mut root.contents {
            Contents::Directory(dir) => {
                dir.entries.insert(
                    path.file_name().unwrap().to_str().unwrap().to_string(),
                    file_ino,
                );
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    fn import_file<S: Store>(&mut self, path: &Path, store: &mut S) -> std::io::Result<Inode> {
        let st = fs::symlink_metadata(path)?;

        let ino = self.next_ino;
        self.next_ino += 1;

        let contents = if st.file_type().is_file() {
            let mut buf = vec![];
            std::fs::File::open(&path)?.read_to_end(&mut buf)?;
            Contents::RegularFile(RegularFile {
                length: st.len(),
                hash: store.add(&buf)?,
            })
        } else if st.file_type().is_dir() {
            let mut entries = BTreeMap::new();
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let file = self.import_file(&entry.path(), store)?;
                let file_ino = file.ino;
                self.inodes.insert(file.ino, file);
                entries.insert(entry.file_name().into_string().unwrap(), file_ino);
            }
            Contents::Directory(Directory { entries })
        } else {
            panic!("unsupported file type");
        };

        Ok(Inode {
            ino,
            perm: st.mode() & 0o7777,
            uid: st.uid(),
            gid: st.gid(),
            crtime: cur_time(),
            mtime: Time(st.mtime() * 1000000000 + st.mtime_nsec()),
            contents,
            //parents:
        })
    }
}

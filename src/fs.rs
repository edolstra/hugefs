use crate::error::{Error, Result};
use crate::hash::Hash;
use crate::store::Store;
use libc;
use serde::{Deserialize, Serialize};
use std::collections::{hash_map::Entry, BTreeMap, HashMap};
use std::fs;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub type Ino = u64;

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct Time(pub i64);

impl From<&Time> for SystemTime {
    fn from(time: &Time) -> Self {
        SystemTime::UNIX_EPOCH + Duration::from_nanos(time.0 as u64)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Superblock {
    inodes: HashMap<Ino, Arc<RwLock<Inode>>>,
    root_ino: Ino,
    next_ino: Ino,
}

impl Superblock {
    pub fn get_inode(&self, ino: Ino) -> Result<Arc<RwLock<Inode>>> {
        self.inodes
            .get(&ino)
            .map(|inode| Arc::clone(inode))
            .ok_or(Error::NoSuchInode(ino))
    }

    fn alloc_inode(&mut self) -> Ino {
        let ino = self.next_ino;
        self.next_ino += 1;
        ino
    }

    pub fn add_inode(&mut self, mut inode: Inode) -> Ino {
        assert_eq!(inode.ino, 0);
        let ino = self.alloc_inode();
        inode.ino = ino;
        match self.inodes.entry(ino) {
            Entry::Vacant(e) => e.insert(Arc::new(RwLock::new(inode))),
            _ => panic!("inode {} already exists", ino),
        };
        ino
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

impl Inode {
    pub fn new(contents: Contents) -> Inode {
        let now = cur_time();
        Inode {
            ino: 0,
            perm: 0,
            uid: 0,
            gid: 0,
            crtime: now,
            mtime: now,
            contents,
        }
    }

    pub fn get_directory(&self) -> Result<&Directory> {
        match &self.contents {
            Contents::Directory(dir) => Ok(dir),
            _ => Err(Error::NotDirectory(self.ino)),
        }
    }

    pub fn get_directory_mut(&mut self) -> Result<&mut Directory> {
        match &mut self.contents {
            Contents::Directory(dir) => Ok(dir),
            _ => Err(Error::NotDirectory(self.ino)),
        }
    }

    pub fn is_file(&self) -> bool {
        match self.contents {
            Contents::RegularFile(_) | Contents::MutableFile(_) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Contents {
    Directory(Directory),
    RegularFile(RegularFile),
    Symlink(Symlink),
    MutableFile(Arc<MutableFile>),
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

pub struct MutableFile {
    pub file: Box<dyn crate::store::MutableFile>,
}

impl std::fmt::Debug for MutableFile {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!()
    }
}

impl serde::Serialize for MutableFile {
    fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom("cannot serialize a mutable file"))
    }
}

impl<'de> serde::Deserialize<'de> for MutableFile {
    fn deserialize<D>(_deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Err(serde::de::Error::custom(
            "cannot deserialize a mutable file",
        ))
    }
}

pub fn cur_time() -> Time {
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
        let mut res = Self {
            inodes: HashMap::new(),
            root_ino,
            next_ino: root_ino,
        };
        res.add_inode(Inode {
            perm: 0o700,
            ..Inode::new(Contents::Directory(Directory {
                entries: BTreeMap::new(),
            }))
        });
        res
    }

    pub fn open_from_json<R: Read>(
        json_data: &mut R,
    ) -> std::result::Result<Self, serde_json::error::Error> {
        serde_json::from_reader(json_data)
    }

    pub fn import<S: Store>(&mut self, path: &Path, store: &mut S) -> std::io::Result<()> {
        let file = self.import_file(path, store)?;
        let file_ino = self.add_inode(file);

        let root = self.inodes.get_mut(&self.root_ino).unwrap();

        match &mut root.write().unwrap().contents {
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
                self.inodes.insert(file.ino, Arc::new(RwLock::new(file)));
                entries.insert(entry.file_name().into_string().unwrap(), file_ino);
            }
            Contents::Directory(Directory { entries })
        } else {
            panic!("unsupported file type");
        };

        Ok(Inode {
            perm: st.mode() & 0o7777,
            uid: st.uid(),
            gid: st.gid(),
            mtime: Time(st.mtime() * 1000000000 + st.mtime_nsec()),
            ..Inode::new(contents)
        })
    }
}

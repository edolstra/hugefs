use crate::error::{Error, Result};
use crate::hash::Hash;
use crate::store::Store;
use libc;
use serde::{Deserialize, Serialize};
use std::collections::{hash_map::Entry, BTreeMap, HashMap};
use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path};
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

impl From<SystemTime> for Time {
    fn from(time: SystemTime) -> Self {
        Time(time.duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64)
    }
}

impl Time {
    pub fn from_nanos(secs: i64, nsecs: i64) -> Self {
        Time(secs * 1000000000 + nsecs)
    }

    pub fn now() -> Self {
        SystemTime::now().into()
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

    pub fn get_root_ino(&self) -> Ino {
        self.root_ino
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

    pub fn nr_inodes(&self) -> u64 {
        self.inodes.len() as u64
    }

    pub fn total_file_size(&self) -> u64 {
        // FIXME: maintain in superblock
        let mut total = 0u64;
        for file in self.inodes.values() {
            let file = file.read().unwrap();
            if let Contents::RegularFile(file) = &file.contents {
                total += file.length;
            }
        }
        total
    }

    pub fn lookup_path(&self, path: &Path) -> crate::store::Result<Arc<RwLock<Inode>>> {
        let mut cur_inode = self.inodes.get(&self.root_ino).unwrap();

        for component in path.components() {
            if let Component::Normal(c) = component {
                let next_ino = cur_inode
                    .read()
                    .unwrap()
                    .get_directory()?
                    .get_entry(c.to_str().ok_or_else(|| Error::BadPath(path.into()))?)?;
                cur_inode = self.inodes.get(&next_ino).unwrap();
            } else {
                return Err(Error::BadPath(path.into()));
            }
        }

        Ok(Arc::clone(cur_inode))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Inode {
    pub ino: Ino,
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
        let now = Time::now();
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

impl Directory {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub fn get_entry(&self, name: &str) -> Result<Ino> {
        self.entries.get(name).map(|x| *x).ok_or(Error::NoSuchEntry)
    }

    pub fn check_no_entry(&self, name: &str) -> Result<()> {
        if self.entries.contains_key(name) {
            Err(Error::EntryExists)
        } else {
            Ok(())
        }
    }
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

impl Symlink {
    pub fn new(target: String) -> Self {
        Self { target }
    }
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

    pub fn write_json<W: Write>(
        &self,
        file: &mut W,
    ) -> std::result::Result<(), serde_json::error::Error> {
        serde_json::ser::to_writer(file, &self)
    }

    pub fn import<S: Store>(&mut self, path: &Path, store: &mut S) -> crate::store::Result<()> {
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

    fn import_file<S: Store>(&mut self, path: &Path, store: &mut S) -> crate::store::Result<Inode> {
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
                let file_ino = self.add_inode(file);
                entries.insert(entry.file_name().into_string().unwrap(), file_ino);
            }
            Contents::Directory(Directory { entries })
        } else if st.file_type().is_symlink() {
            Contents::Symlink(Symlink::new(
                fs::read_link(path)?.into_os_string().into_string().unwrap(),
            ))
        } else {
            panic!("unsupported file type");
        };

        Ok(Inode {
            perm: st.mode() & 0o7777,
            uid: st.uid(),
            gid: st.gid(),
            mtime: Time::from_nanos(st.mtime(), st.mtime_nsec()),
            ..Inode::new(contents)
        })
    }
}

use crate::{
    error::{Error, Result},
    fs_sqlite::{DirEntry, FileType, FileTypeInfo, Filesystem, NewFileInfo, NewFileTypeInfo, Stat},
    fuse_util::*,
    hash::Hash,
    store::MutableFile,
    types::MutableFileId,
};
use fuse::{ReplyEmpty, Request};
use futures::future::FutureExt;
use libc::c_int;
use std::collections::{BTreeMap, HashMap};
use std::convert::{TryFrom, TryInto};
use std::ffi::OsStr;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

type Store = Arc<dyn crate::store::Store>;

pub struct FilesystemState {
    pub fs: Filesystem,
    file_handles: FileHandles,
    pub stores: Vec<Store>,
}

struct FileHandles {
    next_fh: u64,
    handles: HashMap<u64, OpenFile>,
}

enum OpenFile {
    MutableFile {
        mutable_file: Arc<Box<dyn crate::store::MutableFile>>,
    },
    ImmutableFile {
        hash: Hash,
        store: RwLock<Option<Store>>,
    },
    Directory(OpenDirectory),
    Control(OpenControlFile),
}

impl FilesystemState {
    pub fn new(fs: Filesystem, stores: Vec<Store>) -> Self {
        FilesystemState {
            fs,
            file_handles: FileHandles {
                next_fh: 1,
                handles: HashMap::new(),
            },
            stores,
        }
    }
}

impl FileHandles {
    fn create(&mut self, open_file: OpenFile) -> u64 {
        let fh = self.next_fh;
        self.next_fh += 1;
        self.handles.insert(fh, open_file);
        fh
    }

    fn remove(&mut self, fh: u64) -> Result<OpenFile> {
        self.handles.remove(&fh).ok_or(Error::BadFileHandle(fh))
    }

    fn get<'a>(&'a mut self, fh: u64) -> Result<&'a mut OpenFile> {
        self.handles.get_mut(&fh).ok_or(Error::BadFileHandle(fh))
    }

    /*
    fn get_regular<'a>(&'a mut self, fh: u64) -> Result<&'a mut OpenRegularFile> {
        match self.handles.get_mut(&fh) {
            Some(OpenFile::Regular(x)) => Ok(x),
            _ => Err(Error::BadFileHandle(fh)),
        }
    }
    */

    fn get_directory<'a>(&'a mut self, fh: u64) -> Result<&'a mut OpenDirectory> {
        match self.handles.get_mut(&fh) {
            Some(OpenFile::Directory(x)) => Ok(x),
            _ => Err(Error::BadFileHandle(fh)),
        }
    }
}

struct OpenDirectory {
    //inode: Arc<RwLock<Inode>>,
    entries: BTreeMap<String, DirEntry>,
    prev_dir_entry: String,
}

type ControlFuture = std::pin::Pin<Box<dyn futures::Future<Output = String> + Send>>;

struct OpenControlFile {
    tx: tokio::sync::mpsc::UnboundedSender<u8>,
    fut: futures::future::Shared<ControlFuture>,
}

impl FileTypeInfo {
    fn file_type(&self) -> fuse::FileType {
        match self {
            FileTypeInfo::ImmutableRegular { .. } | FileTypeInfo::MutableRegular { .. } => {
                fuse::FileType::RegularFile
            }
            FileTypeInfo::Directory { .. } => fuse::FileType::Directory,
            FileTypeInfo::Symlink { .. } => fuse::FileType::Symlink,
        }
    }
}

impl From<&Stat> for fuse::FileAttr {
    fn from(inode: &Stat) -> Self {
        Self {
            ino: inode.ino,
            size: match inode.file_type {
                FileTypeInfo::ImmutableRegular { length, .. } => length,
                FileTypeInfo::MutableRegular { length, .. } => length,
                FileTypeInfo::Directory { entries } => entries,
                FileTypeInfo::Symlink { length } => length,
            },
            blocks: 0,
            atime: (&inode.mtime).into(),
            mtime: (&inode.mtime).into(),
            ctime: (&inode.mtime).into(),
            crtime: (&inode.crtime).into(),
            kind: inode.file_type.file_type(),
            perm: (inode.perm % 0o7777) as u16,
            nlink: inode.nlink,
            uid: inode.uid,
            gid: inode.gid,
            rdev: 0,
            flags: 0,
            blksize: 1024,
        }
    }
}

pub struct FuseFilesystem {
    state: Arc<RwLock<FilesystemState>>,
    executor: tokio::runtime::Handle,
}

impl FuseFilesystem {
    pub fn new(state: Arc<RwLock<FilesystemState>>, executor: tokio::runtime::Handle) -> Self {
        Self { state, executor }
    }
}

static CONTROL_INO: crate::types::Ino = 0xfffffff0;
pub static CONTROL_NAME: &str = ".hugefsctl1";

fn control_inode_attrs() -> fuse::FileAttr {
    let time = SystemTime::UNIX_EPOCH;
    fuse::FileAttr {
        ino: CONTROL_INO,
        size: 1 << 20, // FIXME
        blocks: 0,
        atime: time,
        mtime: time,
        ctime: time,
        crtime: time,
        kind: fuse::FileType::RegularFile,
        perm: 0o600,
        nlink: 1,
        uid: 0,
        gid: 0,
        rdev: 0,
        flags: 0,
        blksize: 0,
    }
}

impl fuse::Filesystem for FuseFilesystem {
    fn init(&mut self, _req: &Request) -> std::result::Result<(), c_int> {
        Ok(())
    }

    fn destroy(&mut self, _req: &Request) {}

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuse::ReplyEntry) {
        let state = Arc::clone(&self.state);
        let name: String = name.to_str().unwrap().to_string();

        wrap_entry(&self.executor, reply, async move {
            let state = state.read().unwrap();

            if parent == state.fs.get_root_ino() && name == CONTROL_NAME {
                return Ok(crate::fuse_util::EntryOk {
                    ttl: Duration::from_secs(3600),
                    attr: control_inode_attrs(),
                });
            }

            Ok(crate::fuse_util::EntryOk {
                ttl: Duration::from_secs(60),
                attr: fuse::FileAttr::from(&state.fs.lookup(parent, &name)?),
            })
        });
    }

    fn forget(&mut self, _req: &Request, _ino: u64, _nlookup: u64) {}

    fn getattr(&mut self, _req: &Request, ino: u64, reply: fuse::ReplyAttr) {
        let state = self.state.read().unwrap();
        if ino == CONTROL_INO {
            reply.attr(&Duration::from_secs(60), &control_inode_attrs());
        } else {
            reply.attr(
                &Duration::from_secs(60),
                &fuse::FileAttr::from(&state.fs.stat(ino).unwrap()),
            );
        }
    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: fuse::ReplyAttr,
    ) {
        let state = Arc::clone(&self.state);

        wrap_attr(&self.executor, reply, async move {
            let st = state.read().unwrap().fs.set_attributes(
                ino,
                &crate::fs_sqlite::SetAttributes {
                    length: size,
                    perm: mode.map(|m| m & 0o7777),
                    uid: uid,
                    gid: gid,
                    crtime: crtime.map(|t| t.into()),
                    mtime: mtime.map(|t| t.into()),
                },
            )?;

            if let Some(size) = size {
                if let FileTypeInfo::MutableRegular { length, id } = &st.file_type {
                    assert_eq!(*length, size);
                    let stores = state.read().unwrap().stores.clone();
                    let handle = open_file(stores, &id).await?;
                    handle.set_file_length(size).await?;
                }
            }

            Ok((Duration::from_secs(60), fuse::FileAttr::from(&st)))
        });
    }

    fn readlink(&mut self, _req: &Request, ino: u64, reply: fuse::ReplyData) {
        let state = Arc::clone(&self.state);
        wrap_read(&self.executor, reply, async move {
            Ok(state.read().unwrap().fs.readlink(ino)?.as_bytes().to_vec())
        });
    }

    fn mknod(
        &mut self,
        _req: &Request,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _rdev: u32,
        reply: fuse::ReplyEntry,
    ) {
        reply.error(libc::ENOTSUP);
    }

    fn mkdir(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        reply: fuse::ReplyEntry,
    ) {
        let state = Arc::clone(&self.state);
        let name: String = name.to_str().unwrap().to_string();
        let uid = req.uid();
        let gid = req.gid();

        wrap_entry(&self.executor, reply, async move {
            let stat = state.read().unwrap().fs.create_file(
                parent,
                &name,
                true,
                NewFileInfo {
                    file_type: NewFileTypeInfo::Directory,
                    perm: mode & 0o7777,
                    uid,
                    gid,
                },
            )?;

            assert!(stat.nlink > 0);

            Ok(crate::fuse_util::EntryOk {
                ttl: Duration::from_secs(60),
                attr: (&stat).into(),
            })
        });
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let state = Arc::clone(&self.state);
        let name: String = name.to_str().unwrap().to_string();

        wrap_empty(&self.executor, reply, async move {
            // FIXME: check that this is not a directory.
            state.read().unwrap().fs.remove_file(parent, &name)?;
            Ok(())
        });
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let state = Arc::clone(&self.state);
        let name: String = name.to_str().unwrap().to_string();

        wrap_empty(&self.executor, reply, async move {
            // FIXME: check that this is a directory.
            state.read().unwrap().fs.remove_file(parent, &name)?;
            Ok(())
        });
    }

    fn symlink(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        link: &Path,
        reply: fuse::ReplyEntry,
    ) {
        let state = Arc::clone(&self.state);
        let name: String = name.to_str().unwrap().to_string();
        let target: String = link.to_str().unwrap().to_string();
        let uid = req.uid();
        let gid = req.gid();

        wrap_entry(&self.executor, reply, async move {
            let stat = state.read().unwrap().fs.create_file(
                parent,
                &name,
                true,
                NewFileInfo {
                    file_type: NewFileTypeInfo::Symlink { target },
                    perm: 0o777,
                    uid,
                    gid,
                },
            )?;

            Ok(crate::fuse_util::EntryOk {
                ttl: Duration::from_secs(60),
                attr: (&stat).into(),
            })
        });
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent_ino: u64,
        name: &OsStr,
        new_parent_ino: u64,
        new_name: &OsStr,
        reply: ReplyEmpty,
    ) {
        let state = Arc::clone(&self.state);
        let name: String = name.to_str().unwrap().to_string();
        let new_name: String = new_name.to_str().unwrap().to_string();

        wrap_empty(&self.executor, reply, async move {
            state
                .read()
                .unwrap()
                .fs
                .rename(parent_ino, &name, new_parent_ino, &new_name)?;
            Ok(())
        });
    }

    fn link(
        &mut self,
        _req: &Request,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: fuse::ReplyEntry,
    ) {
        let state = Arc::clone(&self.state);
        let newname: String = newname.to_str().unwrap().to_string();

        wrap_entry(&self.executor, reply, async move {
            Ok(crate::fuse_util::EntryOk {
                ttl: Duration::from_secs(60),
                attr: fuse::FileAttr::from(
                    &state.read().unwrap().fs.link(ino, newparent, &newname)?,
                ),
            })
        });
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: u32, reply: fuse::ReplyOpen) {
        let state = Arc::clone(&self.state);

        wrap_open(&self.executor, reply, async move {
            if ino == CONTROL_INO {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u8>();
                let fut: ControlFuture =
                    crate::control::handle_message(rx, Arc::clone(&state)).boxed();
                let fut = fut.shared();
                tokio::task::spawn(fut.clone());
                return Ok((
                    state
                        .write()
                        .unwrap()
                        .file_handles
                        .create(OpenFile::Control(OpenControlFile { tx, fut })),
                    fuse::consts::FOPEN_DIRECT_IO, /* | fuse::consts::FOPEN_NONSEEKABLE */
                ));
            }

            let stat = state.read().unwrap().fs.stat(ino)?;

            match stat.file_type {
                FileTypeInfo::MutableRegular { id, .. } => {
                    let mutable_file = {
                        let stores = state.read().unwrap().stores.clone();
                        Arc::new(open_file(stores, &id).await?)
                    };
                    let fh = state
                        .write()
                        .unwrap()
                        .file_handles
                        .create(OpenFile::MutableFile { mutable_file });
                    Ok((fh, FOPEN_KEEP_CACHE))
                }
                FileTypeInfo::ImmutableRegular { hash, .. } => Ok((
                    state
                        .write()
                        .unwrap()
                        .file_handles
                        .create(OpenFile::ImmutableFile {
                            hash,
                            store: RwLock::new(None),
                        }),
                    FOPEN_KEEP_CACHE,
                )),
                _ => Err(Error::IsDirectory(ino)),
            }
        });
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        reply: fuse::ReplyData,
    ) {
        let state = Arc::clone(&self.state);
        wrap_read(&self.executor, reply, async move {
            enum File {
                MutableFile(Arc<Box<dyn crate::store::MutableFile>>),
                ImmutableFile(Option<Store>, Hash),
                Control(futures::future::Shared<ControlFuture>),
            };

            let file = {
                let state = &mut *state.write().unwrap();
                match state.file_handles.get(fh)? {
                    OpenFile::MutableFile { mutable_file, .. } => {
                        File::MutableFile(mutable_file.clone())
                    }
                    OpenFile::ImmutableFile { hash, store } => {
                        File::ImmutableFile(store.read().unwrap().clone(), hash.clone())
                    }
                    OpenFile::Directory(_) => {
                        return Err(Error::IsDirectory(ino));
                    }
                    OpenFile::Control(control_file) => File::Control(control_file.fut.clone()),
                }
            };

            match file {
                File::MutableFile(mutable_file) => mutable_file.read(offset as u64, size).await,

                File::ImmutableFile(store, hash) => {
                    if let Some(store) = store {
                        let data = store
                            .get(&hash, offset as u64, usize::try_from(size).unwrap())
                            .await?;
                        return Ok(data);
                    } else {
                        // Find a store that has this file.
                        let stores = state.read().unwrap().stores.clone();
                        for store in stores {
                            match store
                                .get(&hash, offset as u64, usize::try_from(size).unwrap())
                                .await
                            {
                                Ok(data) => {
                                    // Update the file handle to use this store from now on.
                                    match state.write().unwrap().file_handles.get(fh)? {
                                        OpenFile::ImmutableFile { store: st, .. } => {
                                            *st.write().unwrap() = Some(store);
                                        }
                                        _ => unreachable!(),
                                    }
                                    return Ok(data);
                                }
                                Err(Error::NoSuchHash(_)) => continue,
                                Err(err) => {
                                    return Err(err);
                                }
                            }
                        }
                        return Err(Error::NoSuchHash(hash));
                    }
                }

                File::Control(fut) => {
                    let res = fut.await;
                    // FIXME: inefficient
                    return Ok(res
                        .as_bytes()
                        .iter()
                        .skip(offset as usize)
                        .take(size as usize)
                        .map(|s| *s)
                        .collect());
                }
            }
        });
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
        reply: fuse::ReplyWrite,
    ) {
        assert!(offset >= 0);
        let state = Arc::clone(&self.state);
        let data = data.to_vec();

        wrap_write(&self.executor, reply, async move {
            let mutable_file = {
                let state = &mut *state.write().unwrap();

                match state.file_handles.get(fh)? {
                    OpenFile::MutableFile { mutable_file } => Arc::clone(mutable_file),

                    OpenFile::ImmutableFile { .. } => return Err(Error::NotMutableFile(ino)),

                    OpenFile::Control(control_file) => {
                        for d in &data {
                            control_file
                                .tx
                                .send(*d)
                                .map_err(|err| Error::ControlMisc(Box::new(err)))?;
                        }
                        return Ok(data.len() as u32);
                    }

                    OpenFile::Directory(_) => return Err(Error::IsDirectory(ino)),
                }
            };

            mutable_file.write(offset as u64, &data).await?;

            state
                .read()
                .unwrap()
                .fs
                .update_length_at_least(ino, offset as u64 + data.len() as u64)?;

            Ok(data.len().try_into().unwrap())
        });
    }

    fn flush(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        reply.ok();
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let state = Arc::clone(&self.state);

        wrap_empty(&self.executor, reply, async move {
            let state = &mut *state.write().unwrap();
            state.file_handles.remove(fh)?;

            /*
            let (length, hash) = mutable_file.file.finish().await.unwrap();

            debug!("finalised file with hash {}, size {}", hash, length);

            inode.write().unwrap().contents =
                Contents::RegularFile(crate::fs::RegularFile { length, hash });
            */

            Ok(())
        });
    }

    fn fsync(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request, ino: u64, _flags: u32, reply: fuse::ReplyOpen) {
        let mut state = self.state.write().unwrap();
        let stat = state.fs.stat(ino).unwrap();
        match stat.file_type {
            FileTypeInfo::Directory { .. } => {
                let entries = state.fs.read_directory(ino).unwrap();
                let fh = state
                    .file_handles
                    .create(OpenFile::Directory(OpenDirectory {
                        prev_dir_entry: String::new(),
                        entries,
                    }));
                reply.opened(fh, 0);
            }
            _ => {
                reply.error(libc::ENOTDIR);
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _offset: i64,
        mut reply: fuse::ReplyDirectory,
    ) {
        let state = &mut *self.state.write().unwrap();
        if let Ok(open_dir) = state.file_handles.get_directory(fh) {
            // FIXME: clone
            for (k, v) in open_dir
                .entries
                .range::<String, _>((Excluded(open_dir.prev_dir_entry.clone()), Unbounded))
            {
                if reply.add(
                    v.ino,
                    0, /* FIXME */
                    match v.file_type {
                        FileType::MutableRegular | FileType::ImmutableRegular => {
                            fuse::FileType::RegularFile
                        }
                        FileType::Directory => fuse::FileType::Directory,
                        FileType::Symlink => fuse::FileType::Symlink,
                    },
                    k,
                ) {
                    break;
                } else {
                    open_dir.prev_dir_entry = k.clone();
                }
            }

            // FIXME: indicate buffer too small
            reply.ok();
        } else {
            reply.error(libc::EBADF);
        }
    }

    fn releasedir(&mut self, _req: &Request, _ino: u64, fh: u64, _flags: u32, reply: ReplyEmpty) {
        let mut state = self.state.write().unwrap();
        if let Ok(_) = state.file_handles.remove(fh) {
            reply.ok();
        } else {
            reply.error(libc::EBADF);
        }
    }

    fn fsyncdir(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: fuse::ReplyStatfs) {
        let state = self.state.read().unwrap();
        let bsize = 1 << 15;
        let cur_bytes = state.fs.total_file_size().unwrap();
        let cur_blocks = cur_bytes / (bsize as u64);
        let free_blocks = 1 << 35;
        let nr_inodes = state.fs.nr_inodes().unwrap();
        let free_inodes = 1 << 24;
        reply.statfs(
            cur_blocks + free_blocks, // blocks
            free_blocks,              // bfree,
            free_blocks,              // bavail,
            nr_inodes + free_inodes,  // files
            free_inodes,              // ffree,
            bsize,                    // bsize
            255,                      // namelen
            bsize,                    // frsize,
        );
    }

    fn setxattr(
        &mut self,
        _req: &Request,
        _ino: u64,
        _name: &OsStr,
        _value: &[u8],
        _flags: u32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(libc::ENOTSUP);
    }

    fn getxattr(
        &mut self,
        _req: &Request,
        _ino: u64,
        _name: &OsStr,
        _size: u32,
        reply: fuse::ReplyXattr,
    ) {
        reply.error(libc::ENOTSUP);
    }

    fn listxattr(&mut self, _req: &Request, _ino: u64, _size: u32, reply: fuse::ReplyXattr) {
        reply.error(libc::ENOTSUP);
    }

    fn removexattr(&mut self, _req: &Request, _ino: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(libc::ENOTSUP);
    }

    fn access(&mut self, _req: &Request, _ino: u64, _mask: u32, reply: ReplyEmpty) {
        // FIXME: should not be called with default_permissions
        reply.ok();
    }

    fn create(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _flags: u32,
        reply: fuse::ReplyCreate,
    ) {
        let state = Arc::clone(&self.state);
        let name: String = name.to_str().unwrap().to_string();
        let uid = req.uid();
        let gid = req.gid();

        // FIXME: check flags

        wrap_create(&self.executor, reply, async move {
            // FIXME: this creates a file even if creation fails.
            let mut mutable_file = {
                let stores = state.read().unwrap().stores.clone();
                create_file(stores).await?
            };

            let mut state = state.write().unwrap();

            let stat = state.fs.create_file(
                parent,
                &name,
                true,
                NewFileInfo {
                    file_type: NewFileTypeInfo::MutableRegular {
                        id: mutable_file.get_id(),
                    },
                    perm: mode & 0o7777,
                    uid,
                    gid,
                },
            )?;

            mutable_file.keep();

            let fh = state.file_handles.create(OpenFile::MutableFile {
                mutable_file: Arc::new(mutable_file),
            });

            Ok(crate::fuse_util::CreateOk {
                ttl: Duration::from_secs(60),
                attr: (&stat).into(),
                fh,
                flags: 0, // FIXME
            })
        });
    }

    fn getlk(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: u32,
        _pid: u32,
        reply: fuse::ReplyLock,
    ) {
        reply.error(libc::ENOTSUP);
    }

    fn setlk(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: u32,
        _pid: u32,
        _sleep: bool,
        reply: ReplyEmpty,
    ) {
        reply.error(libc::ENOTSUP);
    }

    fn bmap(
        &mut self,
        _req: &Request,
        _ino: u64,
        _blocksize: u32,
        _idx: u64,
        reply: fuse::ReplyBmap,
    ) {
        reply.error(libc::ENOTSUP);
    }
}

async fn create_file(stores: Vec<Store>) -> Result<Box<dyn MutableFile>> {
    for store in stores {
        if let Some(fut) = store.create_file() {
            return Ok(fut.await.unwrap());
        }
    }
    Err(Error::NoWritableStore)
}

async fn open_file(
    stores: Vec<Store>,
    mutable_file_id: &MutableFileId,
) -> Result<Box<dyn MutableFile>> {
    for store in stores {
        if let Some(fut) = store.open_file(mutable_file_id) {
            return Ok(fut.await.unwrap());
        }
    }
    Err(Error::NoSuchMutableFile(mutable_file_id.clone()))
}

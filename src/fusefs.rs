use crate::error::{Error, Result};
use crate::fs::{Contents, Inode, Superblock};
use crate::fuse_util::*;
use crate::hash::Hash;
use crate::store::MutableFile;
use fuse::{ReplyEmpty, Request};
use libc::c_int;
use log::{debug, error};
use std::collections::{btree_map::Entry, HashMap};
use std::convert::TryInto;
use std::ffi::OsStr;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::{Duration, SystemTime};

type Store = Arc<dyn crate::store::Store>;

pub struct FilesystemState {
    superblock: Superblock,
    file_handles: HashMap<u64, OpenFile>,
    next_fh: u64,
    stores: Vec<Store>,
}

impl FilesystemState {
    pub fn new(superblock: Superblock, stores: Vec<Store>) -> Self {
        FilesystemState {
            superblock,
            file_handles: HashMap::new(),
            next_fh: 1,
            stores,
        }
    }

    fn new_file_handle(&mut self, open_file: OpenFile) -> u64 {
        let fh = self.next_fh;
        self.next_fh += 1;
        self.file_handles.insert(fh, open_file);
        fh
    }

    fn get_file_handle<'a>(&'a mut self, fh: u64) -> Result<&'a mut OpenFile> {
        self.file_handles
            .get_mut(&fh)
            .ok_or(Error::BadFileHandle(fh))
    }

    pub fn sync(&self, path: &Path) -> std::io::Result<()> {
        let mut temp_path: PathBuf = path.into();
        temp_path.set_extension("json.tmp");
        let mut file = std::fs::File::create(&temp_path)?;
        self.superblock.write_json(&mut file).unwrap();
        std::fs::rename(temp_path, path)?;
        Ok(())
    }
}

struct OpenFile {
    inode: Arc<RwLock<Inode>>,
    prev_dir_entry: Option<String>,
    for_writing: bool,
    store: RwLock<Option<Store>>,
}

impl OpenFile {
    fn new(inode: Arc<RwLock<Inode>>) -> Self {
        OpenFile {
            inode,
            prev_dir_entry: None,
            for_writing: false,
            store: RwLock::new(None),
        }
    }
}

impl Inode {
    fn file_type(&self) -> fuse::FileType {
        match self.contents {
            Contents::Directory(_) => fuse::FileType::Directory,
            Contents::RegularFile(_) | Contents::MutableFile(_) => fuse::FileType::RegularFile,
            Contents::Symlink(_) => fuse::FileType::Symlink,
        }
    }
}

impl From<&Inode> for fuse::FileAttr {
    fn from(inode: &Inode) -> Self {
        Self {
            ino: inode.ino,
            size: match &inode.contents {
                Contents::Directory(dir) => dir.entries.len() as u64,
                Contents::RegularFile(file) => file.length,
                Contents::Symlink(link) => link.target.len() as u64,
                Contents::MutableFile(file) => file.file.len(),
            },
            blocks: 0,
            atime: (&inode.mtime).into(),
            mtime: (&inode.mtime).into(),
            ctime: (&inode.mtime).into(),
            crtime: (&inode.crtime).into(),
            kind: inode.file_type(),
            perm: (inode.perm % 0o7777) as u16,
            nlink: 1,
            uid: inode.uid,
            gid: inode.gid,
            rdev: 0,
            flags: 0,
        }
    }
}

pub struct Filesystem {
    state: Arc<RwLock<FilesystemState>>,
    executor: tokio::runtime::Handle,
}

impl Filesystem {
    pub fn new(state: Arc<RwLock<FilesystemState>>, executor: tokio::runtime::Handle) -> Self {
        Filesystem { state, executor }
    }
}

static GENERATION_COUNT: AtomicU64 = AtomicU64::new(0);

impl fuse::Filesystem for Filesystem {
    fn init(&mut self, _req: &Request) -> std::result::Result<(), c_int> {
        Ok(())
    }

    fn destroy(&mut self, _req: &Request) {}

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuse::ReplyEntry) {
        let state = self.state.read().unwrap();
        let inode = state.superblock.get_inode(parent).unwrap();
        let inode = inode.read().unwrap();
        if let Contents::Directory(dir) = &inode.contents {
            if let Some(entry) = dir.entries.get(name.to_str().unwrap()) {
                let child = state.superblock.get_inode(*entry).unwrap();
                reply.entry(
                    &Duration::from_secs(60),
                    &(&*child.read().unwrap()).into(),
                    0,
                );
            } else {
                reply.error(libc::ENOENT);
            }
        } else {
            reply.error(libc::ENOTDIR);
        }
    }

    fn forget(&mut self, _req: &Request, _ino: u64, _nlookup: u64) {}

    fn getattr(&mut self, _req: &Request, ino: u64, reply: fuse::ReplyAttr) {
        let state = self.state.read().unwrap();
        let inode = state.superblock.get_inode(ino).unwrap();
        reply.attr(&Duration::from_secs(60), &(&*inode.read().unwrap()).into());
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
        _fh: Option<u64>,
        crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: fuse::ReplyAttr,
    ) {
        let state = Arc::clone(&self.state);

        wrap_attr(&self.executor, reply, async move {
            let state = &mut *state.write().unwrap();
            let inode = state.superblock.get_inode(ino)?;
            let mut inode = inode.write().unwrap();

            if let Some(_) = size {
                // FIXME: support truncating mutable files.
                return Err(libc::ENOTSUP.into());
            }

            if let Some(mode) = mode {
                inode.perm = mode & 0o7777;
            }

            if let Some(uid) = uid {
                inode.uid = uid;
            }

            if let Some(gid) = gid {
                inode.gid = gid;
            }

            if let Some(mtime) = mtime {
                inode.mtime = mtime.into();
            }

            if let Some(crtime) = crtime {
                inode.crtime = crtime.into();
            }

            Ok((Duration::from_secs(60), (&*inode).into()))
        });
    }

    fn readlink(&mut self, _req: &Request, ino: u64, reply: fuse::ReplyData) {
        let state = Arc::clone(&self.state);
        wrap_read(&self.executor, reply, async move {
            let state = &mut *state.write().unwrap();
            let inode = state.superblock.get_inode(ino)?;
            let inode = inode.read().unwrap();
            match &inode.contents {
                Contents::Symlink(link) => Ok(link.target.as_bytes().to_vec()),
                _ => Err(libc::EINVAL.into()),
            }
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
            let state = &mut *state.write().unwrap();
            let parent = state.superblock.get_inode(parent)?;
            let mut parent = parent.write().unwrap();
            let dir = parent.get_directory_mut()?;

            dir.check_no_entry(&name)?;

            let inode = Inode {
                perm: mode & 0o7777,
                uid,
                gid,
                ..Inode::new(Contents::Directory(crate::fs::Directory::new()))
            };

            let mut attr: fuse::FileAttr = (&inode).into();
            let ino = state.superblock.add_inode(inode);
            dir.entries.insert(name, ino);
            attr.ino = ino;

            Ok(crate::fuse_util::EntryOk {
                ttl: Duration::from_secs(60),
                attr,
                generation: GENERATION_COUNT.fetch_add(1, Ordering::Relaxed),
            })
        });
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let state = Arc::clone(&self.state);
        let name: String = name.to_str().unwrap().to_string();

        wrap_empty(&self.executor, reply, async move {
            let state = &mut *state.write().unwrap();
            let parent = state.superblock.get_inode(parent)?;
            let mut parent = parent.write().unwrap();
            let dir = parent.get_directory_mut()?;

            match dir.entries.entry(name) {
                Entry::Vacant(_) => Err(libc::ENOENT.into()),
                Entry::Occupied(e) => {
                    let child_ino = *e.get();
                    let child = state.superblock.get_inode(child_ino)?;
                    let child = child.read().unwrap();

                    if let Contents::Directory(_) = &child.contents {
                        Err(libc::EISDIR.into())
                    } else {
                        e.remove_entry();
                        Ok(())
                    }
                }
            }
        });
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let state = Arc::clone(&self.state);
        let name: String = name.to_str().unwrap().to_string();

        wrap_empty(&self.executor, reply, async move {
            let state = &mut *state.write().unwrap();
            let parent = state.superblock.get_inode(parent)?;
            let mut parent = parent.write().unwrap();
            let dir = parent.get_directory_mut()?;

            match dir.entries.entry(name) {
                Entry::Vacant(_) => Err(libc::ENOENT.into()),
                Entry::Occupied(e) => {
                    let child_ino = *e.get();
                    let child = state.superblock.get_inode(child_ino)?;
                    let child = child.read().unwrap();

                    if let Contents::Directory(dir) = &child.contents {
                        if dir.entries.is_empty() {
                            e.remove_entry();
                            Ok(())
                        } else {
                            Err(libc::ENOTEMPTY.into())
                        }
                    } else {
                        Err(libc::ENOTDIR.into())
                    }
                }
            }
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
            let state = &mut *state.write().unwrap();
            let parent = state.superblock.get_inode(parent)?;
            let mut parent = parent.write().unwrap();
            let dir = parent.get_directory_mut()?;

            dir.check_no_entry(&name)?;

            let inode = Inode {
                perm: 0o777,
                uid,
                gid,
                ..Inode::new(Contents::Symlink(crate::fs::Symlink::new(target)))
            };

            let mut attr: fuse::FileAttr = (&inode).into();
            let ino = state.superblock.add_inode(inode);
            dir.entries.insert(name, ino);
            attr.ino = ino;

            Ok(crate::fuse_util::EntryOk {
                ttl: Duration::from_secs(60),
                attr,
                generation: GENERATION_COUNT.fetch_add(1, Ordering::Relaxed),
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
            let state = &mut *state.write().unwrap();
            let parent = state.superblock.get_inode(parent_ino)?;
            let mut parent = parent.write().unwrap();
            let dir = parent.get_directory_mut()?;

            let ino = dir.get_entry(&name)?;

            // ugly
            if parent_ino == new_parent_ino {
                dir.check_no_entry(&new_name)?;
                dir.entries.remove(&name);
                dir.entries.insert(new_name, ino);
            } else {
                let new_parent = state.superblock.get_inode(new_parent_ino)?;
                let mut new_parent = new_parent.write().unwrap();
                let new_dir = new_parent.get_directory_mut()?;

                new_dir.check_no_entry(&new_name)?;

                dir.entries.remove(&name);
                new_dir.entries.insert(new_name, ino);
            }

            Ok(())
        });
    }

    fn link(
        &mut self,
        _req: &Request,
        _ino: u64,
        _newparent: u64,
        _newname: &OsStr,
        reply: fuse::ReplyEntry,
    ) {
        reply.error(libc::ENOTSUP);
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: u32, reply: fuse::ReplyOpen) {
        let mut state = self.state.write().unwrap();
        let inode = state.superblock.get_inode(ino).unwrap();
        if inode.read().unwrap().is_file() {
            let fh = state.new_file_handle(OpenFile::new(inode));
            reply.opened(fh, FOPEN_KEEP_CACHE);
        } else {
            reply.error(libc::EISDIR);
        }
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
                Regular(Option<Store>, Hash),
                Mutable(Arc<crate::fs::MutableFile>),
            };
            let file = {
                let state = &mut *state.write().unwrap();
                let open_file = state.get_file_handle(fh)?;
                let inode = open_file.inode.read().unwrap();
                assert_eq!(ino, inode.ino);
                match &inode.contents {
                    Contents::RegularFile(reg) => {
                        File::Regular(open_file.store.read().unwrap().clone(), reg.hash.clone())
                    }
                    Contents::MutableFile(file) => File::Mutable(Arc::clone(file)),
                    _ => return Err(libc::EISDIR.into()),
                }
            };
            match file {
                File::Regular(store, hash) => {
                    if let Some(store) = store {
                        let data = store.get(&hash, offset as u64, size).await?;
                        return Ok(data);
                    } else {
                        // Find a store that has this file.
                        let stores = state.read().unwrap().stores.clone();
                        for store in stores {
                            match store.get(&hash, offset as u64, size).await {
                                Ok(data) => {
                                    *state
                                        .write()
                                        .unwrap()
                                        .get_file_handle(fh)?
                                        .store
                                        .write()
                                        .unwrap() = Some(store);
                                    return Ok(data);
                                }
                                Err(Error::NoSuchHash(_)) => continue,
                                Err(err) => {
                                    error!("Error reading file {}: {}", ino, err);
                                    return Err(libc::EIO.into());
                                }
                            }
                        }
                        error!("Cannot find file {} with hash {}", ino, hash.to_hex());
                        return Err(libc::ENOMEDIUM.into());
                    }
                }
                File::Mutable(file) => match file.file.read(offset as u64, size).await {
                    Ok(data) => return Ok(data),
                    Err(err) => {
                        error!("Error reading file {}: {}", ino, err);
                        return Err(libc::EIO.into());
                    }
                },
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
        let state = Arc::clone(&self.state);
        let data = data.to_vec();

        wrap_write(&self.executor, reply, async move {
            let file = {
                let state = &mut *state.write().unwrap();
                let open_file = state.get_file_handle(fh)?;
                let inode = open_file.inode.read().unwrap();
                assert_eq!(ino, inode.ino);
                match &inode.contents {
                    Contents::MutableFile(file) => Arc::clone(file),
                    Contents::RegularFile(_) => return Err(libc::EPERM.into()),
                    _ => return Err(libc::EISDIR.into()),
                }
            };

            file.file.write(offset as u64, &data).await.unwrap();

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
            let (inode, mutable_file) = {
                let state = &mut *state.write().unwrap();
                if let Some(open_file) = state.file_handles.remove(&fh) {
                    if !open_file.for_writing {
                        return Ok(());
                    }
                    let mut inode = open_file.inode.write().unwrap();
                    if let Contents::MutableFile(file) = &mut inode.contents {
                        (Arc::clone(&open_file.inode), Arc::clone(file))
                    } else {
                        return Ok(());
                    }
                } else {
                    return Err(libc::EBADF.into());
                }
            };

            let (length, hash) = mutable_file.file.finish().await.unwrap();

            debug!("finalised file with hash {}, size {}", hash, length);

            inode.write().unwrap().contents =
                Contents::RegularFile(crate::fs::RegularFile { length, hash });

            Ok(())
        });
    }

    fn fsync(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request, ino: u64, _flags: u32, reply: fuse::ReplyOpen) {
        let mut state = self.state.write().unwrap();
        let inode = state.superblock.get_inode(ino).unwrap();
        if inode.read().unwrap().file_type() == fuse::FileType::Directory {
            let mut open_file = OpenFile::new(inode);
            open_file.prev_dir_entry = Some(String::new());
            let fh = state.new_file_handle(open_file);
            reply.opened(fh, 0);
        } else {
            reply.error(libc::ENOTDIR);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        _offset: i64,
        mut reply: fuse::ReplyDirectory,
    ) {
        let state = &mut *self.state.write().unwrap();
        if let Some(open_file) = state.file_handles.get_mut(&fh) {
            let inode = open_file.inode.read().unwrap();
            assert_eq!(ino, inode.ino);
            if let Some(prev_dir_entry) = &mut open_file.prev_dir_entry {
                if let Contents::Directory(dir) = &inode.contents {
                    // FIXME: clone
                    for (k, v) in dir
                        .entries
                        .range::<String, _>((Excluded(prev_dir_entry.clone()), Unbounded))
                    {
                        if reply.add(
                            ino,
                            0, /* FIXME */
                            state
                                .superblock
                                .get_inode(*v)
                                .unwrap()
                                .read()
                                .unwrap()
                                .file_type(),
                            k,
                        ) {
                            break;
                        } else {
                            *prev_dir_entry = k.clone();
                        }
                    }

                    // FIXME: indicate buffer too small
                    reply.ok();
                } else {
                    reply.error(libc::ENOTDIR);
                }
            } else {
                reply.error(libc::ENOTDIR);
            }
        } else {
            reply.error(libc::EBADF);
        }
    }

    fn releasedir(&mut self, _req: &Request, _ino: u64, fh: u64, _flags: u32, reply: ReplyEmpty) {
        let mut state = self.state.write().unwrap();
        if let Some(_) = state.file_handles.remove(&fh) {
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
        let cur_bytes = state.superblock.total_file_size();
        let cur_blocks = cur_bytes / (bsize as u64);
        let free_blocks = 1 << 35;
        let nr_inodes = state.superblock.nr_inodes();
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
            let mutable_file = {
                let stores = state.read().unwrap().stores.clone();
                create_file(stores).await?
            };

            let state = &mut *state.write().unwrap();
            let parent = state.superblock.get_inode(parent)?;
            let mut parent = parent.write().unwrap();
            let dir = parent.get_directory_mut()?;

            dir.check_no_entry(&name)?;

            let inode = Inode {
                perm: mode & 0o7777,
                uid,
                gid,
                ..Inode::new(Contents::MutableFile(Arc::new(crate::fs::MutableFile {
                    file: mutable_file,
                })))
            };

            let mut attr: fuse::FileAttr = (&inode).into();
            let ino = state.superblock.add_inode(inode);
            dir.entries.insert(name, ino);
            attr.ino = ino;

            let mut open_file = OpenFile::new(state.superblock.get_inode(ino)?);
            open_file.for_writing = true;
            let fh = state.new_file_handle(open_file);

            Ok(crate::fuse_util::CreateOk {
                ttl: Duration::from_secs(60),
                attr,
                generation: GENERATION_COUNT.fetch_add(1, Ordering::Relaxed),
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

async fn create_file(stores: Vec<Store>) -> std::result::Result<Box<dyn MutableFile>, FuseError> {
    for store in stores {
        if let Some(fut) = store.create_file() {
            return Ok(fut.await.unwrap());
        }
    }
    Err(libc::EROFS.into())
}

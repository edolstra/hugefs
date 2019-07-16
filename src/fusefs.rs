use crate::fs::{Contents, Inode, Superblock};
use crate::store::Store;
use crate::fuse_util::*;
use fuse::{ReplyEmpty, Request};
use libc::c_int;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::Path;
use std::time::{SystemTime, Duration};
use std::sync::{Arc, RwLock};
use log::error;

pub struct FilesystemState {
    superblock: Superblock,
    file_handles: HashMap<u64, OpenFile>,
    next_fh: u64,
    store: Arc<dyn Store>,
}

impl FilesystemState {
    pub fn new(superblock: Superblock, store: Arc<dyn Store>) -> Self {
        FilesystemState {
            superblock,
            file_handles: HashMap::new(),
            next_fh: 1,
            store,
        }
    }

    fn new_file_handle(&mut self, open_file: OpenFile) -> u64 {
        let fh = self.next_fh;
        self.next_fh += 1;
        self.file_handles.insert(fh, open_file);
        fh
    }
}

struct OpenFile {
    inode: Arc<RwLock<Inode>>,
    prev_dir_entry: Option<String>,
}

impl OpenFile {
    fn new(inode: Arc<RwLock<Inode>>) -> Self {
        OpenFile {
            inode,
            prev_dir_entry: None,
        }
    }
}

impl Inode {
    fn file_type(&self) -> fuse::FileType {
        match self.contents {
            Contents::Directory(_) => fuse::FileType::Directory,
            Contents::RegularFile(_) => fuse::FileType::RegularFile,
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
    executor: tokio::runtime::TaskExecutor,
}

impl Filesystem {

    pub fn new(superblock: Superblock, store: Arc<dyn Store>, executor: tokio::runtime::TaskExecutor) -> Self {
        Filesystem {
            state: Arc::new(RwLock::new(FilesystemState::new(superblock, store))),
            executor,
        }
    }

}

impl fuse::Filesystem for Filesystem {
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        Ok(())
    }

    fn destroy(&mut self, _req: &Request) {
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuse::ReplyEntry) {
        let state = self.state.read().unwrap();
        if let Some(inode) = state.superblock.get_inode(parent) {
            if let Contents::Directory(dir) = &(&*inode.read().unwrap()).contents {
                if let Some(entry) = dir.entries.get(name.to_str().unwrap()) {
                    let child = state.superblock.get_inode(*entry).unwrap();
                    reply.entry(&Duration::from_secs(60), &(&*child.read().unwrap()).into(), 0);
                } else {
                    reply.error(libc::ENOENT);
                }
            } else {
                reply.error(libc::ENOTDIR);
            }
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn forget(&mut self, _req: &Request, _ino: u64, _nlookup: u64) {
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: fuse::ReplyAttr) {
        let state = self.state.read().unwrap();
        if let Some(inode) = state.superblock.get_inode(ino) {
            reply.attr(&Duration::from_secs(60), &(&*inode.read().unwrap()).into());
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn setattr(
        &mut self,
        _req: &Request,
        _ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<SystemTime>,
        _mtime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: fuse::ReplyAttr,
    ) {
        reply.error(libc::EROFS);
    }

    fn readlink(&mut self, _req: &Request, _ino: u64, reply: fuse::ReplyData) {
        reply.error(libc::ENOTSUP);
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
        reply.error(libc::EROFS);
    }

    fn mkdir(
        &mut self,
        _req: &Request,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        reply: fuse::ReplyEntry,
    ) {
        reply.error(libc::EROFS);
    }

    fn unlink(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(libc::EROFS);
    }

    fn rmdir(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(libc::EROFS);
    }

    fn symlink(
        &mut self,
        _req: &Request,
        _parent: u64,
        _name: &OsStr,
        _link: &Path,
        reply: fuse::ReplyEntry,
    ) {
        reply.error(libc::EROFS);
    }

    fn rename(
        &mut self,
        _req: &Request,
        _parent: u64,
        _name: &OsStr,
        _newparent: u64,
        _newname: &OsStr,
        reply: ReplyEmpty,
    ) {
        reply.error(libc::EROFS);
    }

    fn link(
        &mut self,
        _req: &Request,
        _ino: u64,
        _newparent: u64,
        _newname: &OsStr,
        reply: fuse::ReplyEntry,
    ) {
        reply.error(libc::EROFS);
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: u32, reply: fuse::ReplyOpen) {
        let mut state = self.state.write().unwrap();
        if let Some(inode) = state.superblock.get_inode(ino) {
            if inode.read().unwrap().file_type() == fuse::FileType::RegularFile {
                let fh = state.new_file_handle(OpenFile::new(inode));
                reply.opened(fh, FOPEN_KEEP_CACHE);
            } else {
                reply.error(libc::EISDIR);
            }
        } else {
            reply.error(libc::ENOENT);
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
            let hash = {
                let state = &mut *state.write().unwrap();
                if let Some(open_file) = state.file_handles.get_mut(&fh) {
                    let inode = open_file.inode.read().unwrap();
                    assert_eq!(ino, inode.ino);
                    if let Contents::RegularFile(reg) = &inode.contents {
                        reg.hash.clone()
                    } else {
                        return Err(libc::EISDIR);
                    }
                } else {
                    return Err(libc::EBADF);
                }
            };
            let store = Arc::clone(&state.read().unwrap().store);
            match store.get(&hash, offset as u64, size).await {
                Ok(data) => return Ok(data),
                Err(err) => {
                    error!("Error reading file {}: {}", ino, err);
                    return Err(libc::EIO);
                }
            }
        });
    }

    fn write(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _data: &[u8],
        _flags: u32,
        reply: fuse::ReplyWrite,
    ) {
        reply.error(libc::EROFS);
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
        let mut state = self.state.write().unwrap();
        if let Some(_) = state.file_handles.remove(&fh) {
            reply.ok();
        } else {
            reply.error(libc::EBADF);
        }
    }

    fn fsync(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request, ino: u64, _flags: u32, reply: fuse::ReplyOpen) {
        let mut state = self.state.write().unwrap();
        if let Some(inode) = state.superblock.get_inode(ino) {
            if inode.read().unwrap().file_type() == fuse::FileType::Directory {
                let mut open_file = OpenFile::new(inode);
                open_file.prev_dir_entry = Some(String::new());
                let fh = state.new_file_handle(open_file);
                reply.opened(fh, 0);
            } else {
                reply.error(libc::ENOTDIR);
            }
        } else {
            reply.error(libc::ENOENT);
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
                            state.superblock.get_inode(*v).unwrap().read().unwrap().file_type(),
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
        reply.error(libc::ENOTSUP);
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
        _req: &Request,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _flags: u32,
        reply: fuse::ReplyCreate,
    ) {
        reply.error(libc::EROFS);
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

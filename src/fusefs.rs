use crate::fs::{Contents, Ino, Inode, Superblock};
use crate::store::Store;
use fuse::{ReplyEmpty, Request};
use libc::c_int;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::Path;
use time::Timespec;

pub struct Filesystem {
    superblock: Superblock,
    file_handles: HashMap<u64, OpenFile>,
    next_fh: u64,
    store: Box<Store>,
}

impl Filesystem {
    pub fn new(superblock: Superblock, store: Box<Store>) -> Self {
        Filesystem {
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
    ino: Ino,
    prev_dir_entry: Option<String>,
}

impl OpenFile {
    fn new(ino: Ino) -> Self {
        OpenFile {
            ino,
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

const FOPEN_KEEP_CACHE: u32 = 1 << 1;

impl fuse::Filesystem for Filesystem {
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        println!("init");
        Ok(())
    }

    fn destroy(&mut self, _req: &Request) {
        println!("destroy");
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuse::ReplyEntry) {
        println!("lookup {} {:?}", parent, name);

        if let Some(inode) = self.superblock.get_inode(parent) {
            if let Contents::Directory(dir) = &inode.contents {
                if let Some(entry) = dir.entries.get(name.to_str().unwrap()) {
                    let child = self.superblock.get_inode(*entry).unwrap();
                    reply.entry(&Timespec::new(60, 0), &child.into(), 0);
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
        println!("forget");
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: fuse::ReplyAttr) {
        println!("getattr {}", ino);
        if let Some(inode) = self.superblock.get_inode(ino) {
            reply.attr(&Timespec::new(60, 0), &inode.into());
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
        _atime: Option<Timespec>,
        _mtime: Option<Timespec>,
        _fh: Option<u64>,
        _crtime: Option<Timespec>,
        _chgtime: Option<Timespec>,
        _bkuptime: Option<Timespec>,
        _flags: Option<u32>,
        reply: fuse::ReplyAttr,
    ) {
        println!("getattr");
        reply.error(libc::EROFS);
    }

    fn readlink(&mut self, _req: &Request, _ino: u64, reply: fuse::ReplyData) {
        println!("readlink");
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
        println!("mknod");
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
        println!("mkdir");
        reply.error(libc::EROFS);
    }

    fn unlink(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        println!("unlink");
        reply.error(libc::EROFS);
    }

    fn rmdir(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        println!("rmdir");
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
        println!("symlink");
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
        println!("rename");
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
        println!("link");
        reply.error(libc::EROFS);
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: u32, reply: fuse::ReplyOpen) {
        println!("open");

        if let Some(inode) = self.superblock.get_inode(ino) {
            if inode.file_type() == fuse::FileType::RegularFile {
                let fh = self.new_file_handle(OpenFile::new(ino));
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
        println!("read {} {}", offset, size);

        if let Some(open_file) = self.file_handles.get_mut(&fh) {
            assert_eq!(ino, open_file.ino);
            if let Some(inode) = self.superblock.get_inode(ino) {
                if let Contents::RegularFile(reg) = &inode.contents {
                    let data = self.store.get(&reg.hash, offset as u64, size).unwrap();
                    reply.data(&data);
                /*
                reply.data(&buf[0..n]);
                */
                } else {
                    reply.error(libc::EISDIR);
                }
            } else {
                reply.error(libc::ENOENT);
            }
        } else {
            reply.error(libc::EBADF);
        }
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
        println!("write");
        reply.error(libc::EROFS);
    }

    fn flush(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        println!("flush");
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
        println!("release");

        if let Some(_) = self.file_handles.remove(&fh) {
            reply.ok();
        } else {
            reply.error(libc::EBADF);
        }
    }

    fn fsync(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        println!("fsync");
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request, ino: u64, _flags: u32, reply: fuse::ReplyOpen) {
        println!("opendir");

        if let Some(inode) = self.superblock.get_inode(ino) {
            if inode.file_type() == fuse::FileType::Directory {
                let mut open_file = OpenFile::new(ino);
                open_file.prev_dir_entry = Some(String::new());
                let fh = self.new_file_handle(open_file);
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
        println!("readdir");

        if let Some(open_file) = self.file_handles.get_mut(&fh) {
            assert_eq!(ino, open_file.ino);
            if let Some(prev_dir_entry) = &mut open_file.prev_dir_entry {
                if let Some(inode) = self.superblock.get_inode(ino) {
                    if let Contents::Directory(dir) = &inode.contents {
                        // FIXME: clone
                        for (k, v) in dir
                            .entries
                            .range::<String, _>((Excluded(prev_dir_entry.clone()), Unbounded))
                        {
                            if reply.add(
                                ino,
                                0, /* FIXME */
                                self.superblock.get_inode(*v).unwrap().file_type(),
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
                }
            } else {
                reply.error(libc::ENOTDIR);
            }
        } else {
            reply.error(libc::EBADF);
        }
    }

    fn releasedir(&mut self, _req: &Request, _ino: u64, fh: u64, _flags: u32, reply: ReplyEmpty) {
        println!("releasedir");

        if let Some(_) = self.file_handles.remove(&fh) {
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
        println!("fsyncidr");
        reply.ok();
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: fuse::ReplyStatfs) {
        println!("statfs");
        reply.error(libc::ENOTSUP);
    }

    fn setxattr(
        &mut self,
        _req: &Request,
        ino: u64,
        name: &OsStr,
        _value: &[u8],
        _flags: u32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        println!("setxattr {} {:?}", ino, name);
        reply.error(libc::ENOTSUP);
    }

    fn getxattr(
        &mut self,
        _req: &Request,
        ino: u64,
        name: &OsStr,
        _size: u32,
        reply: fuse::ReplyXattr,
    ) {
        println!("getxattr {} {:?}", ino, name);
        reply.error(libc::ENOTSUP);
    }

    fn listxattr(&mut self, _req: &Request, ino: u64, _size: u32, reply: fuse::ReplyXattr) {
        println!("listxattr {}", ino);
        reply.error(libc::ENOTSUP);
    }

    fn removexattr(&mut self, _req: &Request, ino: u64, _name: &OsStr, reply: ReplyEmpty) {
        println!("removexattr {}", ino);
        reply.error(libc::ENOTSUP);
    }

    fn access(&mut self, _req: &Request, _ino: u64, _mask: u32, reply: ReplyEmpty) {
        println!("access");
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
        println!("create");
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
        println!("getlk");
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
        println!("setlk");
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
        println!("bmap");
        reply.error(libc::ENOTSUP);
    }
}

use crate::error::{Error, Result};
use crate::hash::Hash;
use crate::types::{Ino, MutableFileId, Time};
use log::debug;
use rusqlite::{OptionalExtension, ToSql, Transaction, NO_PARAMS};
use std::collections::BTreeMap;
use std::path::{Component, Path};

pub struct Filesystem {
    pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
    root_ino: Ino,
}

impl Filesystem {
    pub fn open(db_path: &Path) -> Result<Self> {
        let manager = r2d2_sqlite::SqliteConnectionManager::file(db_path).with_init(|c| {
            c.pragma_update(None, "foreign_keys", &"1")?;
            c.pragma_update(Some(rusqlite::DatabaseName::Main), "journal_mode", &"wal")?;
            Ok(())
        });
        let pool = r2d2::Pool::new(manager).unwrap();

        let mut conn = pool.get()?;

        conn.execute_batch(include_str!("fs_schema.sql"))?;

        let root_ino = if let Some(root_ino) = conn
            .query_row("select root from Root", NO_PARAMS, |row| {
                row.get::<usize, i64>(0)
            })
            .optional()?
        {
            root_ino as Ino
        } else {
            let txn = conn.transaction()?;
            let ino = create_inode(
                &txn,
                NewFileInfo {
                    file_type: NewFileTypeInfo::Directory,
                    perm: 0o700,
                    uid: 0,
                    gid: 0,
                },
            )?
            .ino;
            txn.execute("insert into Root (root) values (?)", &[ino as i64])?;
            inc_nlink(&txn, ino)?;
            txn.commit()?;
            ino
        };

        debug!("root ino = {}", root_ino);

        Ok(Self { pool, root_ino })
    }

    pub fn get_root_ino(&self) -> Ino {
        self.root_ino
    }

    pub fn lookup_path(&self, path: &Path) -> Result<Ino> {
        let mut cur_ino = self.root_ino;

        for component in path.components() {
            if let Component::Normal(c) = component {
                cur_ino = self
                    .lookup(
                        cur_ino,
                        c.to_str().ok_or_else(|| Error::BadPath(path.into()))?,
                    )?
                    .ino;
            } else {
                return Err(Error::BadPath(path.into()));
            }
        }

        Ok(cur_ino)
    }

    pub fn stat(&self, ino: Ino) -> Result<Stat> {
        stat(&self.pool.get()?.transaction()?, ino)
    }

    pub fn set_attributes(&self, ino: Ino, attrs: &SetAttributes) -> Result<Stat> {
        let mut conn = self.pool.get()?;
        let txn = conn.transaction()?;

        let mut st = stat(&txn, ino)?;

        if let Some(l) = attrs.length {
            if let FileTypeInfo::MutableRegular { length, .. } = &mut st.file_type {
                *length = l;
            } else {
                return Err(Error::NotMutableFile(ino));
            }
        }

        st.perm = attrs.perm.unwrap_or(st.perm);
        st.uid = attrs.uid.unwrap_or(st.uid);
        st.gid = attrs.gid.unwrap_or(st.gid);
        st.crtime = attrs.crtime.unwrap_or(st.crtime);
        st.mtime = attrs.mtime.unwrap_or(st.mtime);

        {
            let mut stmt = txn.prepare_cached("update Inodes set perm = ?, uid = ?, gid = ?, crtime = ?, mtime = ?, length = ? where ino = ?")?;
            let nr_updated = stmt.execute(&[
                &(st.perm as i64),
                &(st.uid as i64),
                &(st.gid as i64),
                &(st.crtime.0 as i64),
                &(st.mtime.0 as i64),
                &(match st.file_type {
                    FileTypeInfo::MutableRegular { length, .. } => length as i64,
                    FileTypeInfo::ImmutableRegular { length, .. } => length as i64,
                    FileTypeInfo::Directory { entries, .. } => entries as i64,
                    FileTypeInfo::Symlink { length } => length as i64,
                }),
                &(ino as i64),
            ])?;
            assert_eq!(nr_updated, 1);
        }

        txn.commit()?;

        Ok(st)
    }

    pub fn update_length_at_least(&self, ino: Ino, length: u64) -> Result<()> {
        let mut conn = self.pool.get()?;
        let txn = conn.transaction()?;

        {
            let mut stmt = txn.prepare_cached(
                "update Inodes set length = max(?, length) where ino = ? and type = 1",
            )?;

            let nr_updated = stmt.execute(&[&(length as i64), &(ino as i64)])?;
            assert_eq!(nr_updated, 1);
        }

        txn.commit()?;

        Ok(())
    }

    pub fn create_file(
        &self,
        parent_ino: Ino,
        name: &str,
        exclusive: bool,
        info: NewFileInfo,
    ) -> Result<Stat> {
        let mut conn = self.pool.get()?;
        let txn = conn.transaction()?;
        let mut stat = create_inode(&txn, info)?;
        link_file(&txn, parent_ino, exclusive, name, &mut stat)?;
        txn.commit()?;
        Ok(stat)
    }

    pub fn remove_file(&self, parent_ino: Ino, name: &str) -> Result<()> {
        let mut conn = self.pool.get()?;
        let txn = conn.transaction()?;
        unlink_file(&txn, parent_ino, name)?;
        txn.commit()?;
        Ok(())
    }

    pub fn rename(&self, from_dir: Ino, from_name: &str, to_dir: Ino, to_name: &str) -> Result<()> {
        let mut conn = self.pool.get()?;
        let txn = conn.transaction()?;
        let mut st = lookup(&txn, from_dir, from_name)?;
        link_file(&txn, to_dir, false, to_name, &mut st)?;
        unlink_file(&txn, from_dir, from_name)?;
        txn.commit()?;
        Ok(())
    }

    pub fn link(&self, ino: Ino, dir: Ino, name: &str) -> Result<Stat> {
        let mut conn = self.pool.get()?;
        let txn = conn.transaction()?;
        let mut st = stat(&txn, ino)?;
        link_file(&txn, dir, false, name, &mut st)?;
        txn.commit()?;
        Ok(st)
    }

    pub fn readlink(&self, ino: Ino) -> Result<String> {
        let conn = self.pool.get()?;

        let mut stmt = conn.prepare_cached("select target from Symlinks where ino = ?")?;

        stmt.query_row(&[&(ino as i64)], |row| row.get(0))
            .optional()?
            .ok_or(Error::NotSymlink(ino))
    }

    pub fn total_file_size(&self) -> Result<u64> {
        Ok(0)
    }

    pub fn nr_inodes(&self) -> Result<u64> {
        Ok(0)
    }
}

pub struct Stat {
    pub ino: Ino,
    pub file_type: FileTypeInfo,
    pub perm: libc::mode_t,
    pub uid: libc::uid_t,
    pub gid: libc::gid_t,
    pub nlink: u32,
    pub crtime: Time,
    pub mtime: Time,
}

pub struct SetAttributes {
    pub length: Option<u64>,
    pub perm: Option<libc::mode_t>,
    pub uid: Option<libc::uid_t>,
    pub gid: Option<libc::gid_t>,
    pub crtime: Option<Time>,
    pub mtime: Option<Time>,
}

pub fn stat(txn: &Transaction, ino: Ino) -> Result<Stat> {
    txn.query_row(
        "select type, perm, uid, gid, nlink, crtime, mtime, length, ptr from Inodes where ino = ?",
        &[ino as i64],
        |row| {
            Ok(Stat {
                ino,
                file_type: match row.get(0)? {
                    1 => FileTypeInfo::MutableRegular {
                        id: {
                            let blob: Vec<u8> = row.get(8)?;
                            String::from_utf8(blob).unwrap()
                        },
                        length: row.get::<usize, i64>(7)? as u64,
                    },
                    2 => FileTypeInfo::ImmutableRegular {
                        hash: {
                            let blob: Vec<u8> = row.get(8)?;
                            Hash::from_bytes(&blob)
                        },
                        length: row.get::<usize, i64>(7)? as u64,
                    },
                    3 => FileTypeInfo::Directory {
                        entries: row.get::<usize, i64>(7)? as u64,
                    },
                    4 => FileTypeInfo::Symlink {
                        length: row.get::<usize, i64>(7)? as u64,
                    },
                    n => panic!("Inode {} has invalid file type {}.", ino, n),
                },
                perm: row.get(1)?,
                uid: row.get(2)?,
                gid: row.get(3)?,
                nlink: row.get(4)?,
                crtime: Time(row.get(5)?),
                mtime: Time(row.get(6)?),
            })
        },
    )
    .optional()?
    .ok_or(Error::NoSuchInode(ino))
}

pub enum FileTypeInfo {
    MutableRegular { id: MutableFileId, length: u64 },
    ImmutableRegular { hash: Hash, length: u64 },
    Directory { entries: u64 },
    Symlink { length: u64 },
}

impl From<&FileTypeInfo> for i64 {
    fn from(file_type: &FileTypeInfo) -> Self {
        match file_type {
            FileTypeInfo::MutableRegular { .. } => 1,
            FileTypeInfo::ImmutableRegular { .. } => 2,
            FileTypeInfo::Directory { .. } => 3,
            FileTypeInfo::Symlink { .. } => 4,
        }
    }
}

pub struct NewFileInfo {
    pub file_type: NewFileTypeInfo,
    pub perm: libc::mode_t,
    pub uid: libc::uid_t,
    pub gid: libc::gid_t,
}

pub enum NewFileTypeInfo {
    MutableRegular { id: MutableFileId },
    ImmutableRegular { hash: Hash, length: u64 },
    Directory,
    Symlink { target: String },
}

impl From<&NewFileTypeInfo> for i64 {
    fn from(file_type: &NewFileTypeInfo) -> Self {
        match file_type {
            NewFileTypeInfo::MutableRegular { .. } => 1,
            NewFileTypeInfo::ImmutableRegular { .. } => 2,
            NewFileTypeInfo::Directory => 3,
            NewFileTypeInfo::Symlink { .. } => 4,
        }
    }
}

fn create_inode(txn: &Transaction, info: NewFileInfo) -> Result<Stat> {
    let mut stmt = txn.prepare_cached(
        "insert into Inodes (type, perm, uid, gid, nlink, crtime, mtime, length, ptr) values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )?;

    let now = Time::now();

    let ptr: Option<Vec<u8>> = match &info.file_type {
        NewFileTypeInfo::MutableRegular { id, .. } => Some(id.clone().into_bytes()),
        NewFileTypeInfo::ImmutableRegular { hash, .. } => Some(hash.0.to_vec()),
        _ => None,
    };

    let ino = stmt.insert(&[
        &(i64::from(&info.file_type)),
        &(info.perm as i64),
        &(info.uid as i64),
        &(info.gid as i64),
        &0,
        &(now.0 as i64),
        &(now.0 as i64),
        &(match &info.file_type {
            NewFileTypeInfo::MutableRegular { .. } => 0i64,
            NewFileTypeInfo::ImmutableRegular { length, .. } => *length as i64,
            NewFileTypeInfo::Directory => 0,
            NewFileTypeInfo::Symlink { target } => target.len() as i64,
        }),
        &ptr as &dyn ToSql,
    ])? as Ino;

    if let NewFileTypeInfo::Symlink { target } = &info.file_type {
        let mut stmt = txn.prepare_cached("insert into Symlinks (ino, target) values (?, ?)")?;
        stmt.insert(&[&(ino as i64), &target as &dyn ToSql])?;
    }

    Ok(Stat {
        ino,
        file_type: match info.file_type {
            NewFileTypeInfo::MutableRegular { id } => {
                FileTypeInfo::MutableRegular { id, length: 0 }
            }
            NewFileTypeInfo::ImmutableRegular { hash, length } => {
                FileTypeInfo::ImmutableRegular { hash, length }
            }
            NewFileTypeInfo::Directory => FileTypeInfo::Directory { entries: 0 },
            NewFileTypeInfo::Symlink { target } => FileTypeInfo::Symlink {
                length: target.len() as u64,
            },
        },
        perm: info.perm,
        uid: info.uid,
        gid: info.gid,
        nlink: 0,
        crtime: now,
        mtime: now,
    })
}

fn get_dir_entry(txn: &Transaction, parent_ino: Ino, entry_name: &str) -> Result<Option<Ino>> {
    let mut stmt = txn.prepare_cached("select ino from DirEntries where dir = ? and name = ?")?;
    Ok(stmt
        .query_row(&[&(parent_ino as i64), &entry_name as &dyn ToSql], |row| {
            Ok(row.get::<usize, i64>(0)? as Ino)
        })
        .optional()?)
}

fn link_file(
    txn: &Transaction,
    parent_ino: Ino,
    exclusive: bool,
    entry_name: &str,
    stat: &mut Stat,
) -> Result<bool> {
    let prev_ino = if let Some(prev_ino) = get_dir_entry(txn, parent_ino, entry_name)? {
        if stat.ino == prev_ino {
            return Ok(false);
        }
        if exclusive {
            return Err(Error::EntryExists);
        }
        Some(prev_ino)
    } else {
        None
    };

    let mut stmt = txn.prepare_cached(
        "insert or replace into DirEntries (dir, name, ino, type) values (?, ?, ?, ?)",
    )?;

    stmt.execute(&[
        &(parent_ino as i64),
        &entry_name as &dyn ToSql,
        &(stat.ino as i64),
        &(i64::from(&stat.file_type)),
    ])?;

    inc_nlink(txn, stat.ino)?;
    stat.nlink += 1;

    if let Some(prev_ino) = prev_ino {
        if dec_nlink(txn, prev_ino)? == 0 {
            delete_inode(txn, prev_ino)?;
        }
    }

    // FIXME: update directory length, directory mtime.

    Ok(true)
}

fn unlink_file(txn: &Transaction, parent_ino: Ino, entry_name: &str) -> Result<()> {
    if let Some(entry_ino) = get_dir_entry(txn, parent_ino, entry_name)? {
        let mut stmt = txn.prepare_cached("delete from DirEntries where dir = ? and name = ?")?;
        let nr_deleted = stmt.execute(&[&(parent_ino as i64), &entry_name as &dyn ToSql])?;
        assert_eq!(nr_deleted, 1);
        if dec_nlink(txn, entry_ino)? == 0 {
            delete_inode(txn, entry_ino)?;
        }
        Ok(())
    } else {
        Err(Error::NoSuchEntry)
    }
}

fn inc_nlink(txn: &Transaction, ino: Ino) -> Result<()> {
    let mut stmt = txn.prepare_cached("update Inodes set nlink = nlink + 1 where ino = ?")?;
    let nr_updated = stmt.execute(&[&(ino as i64)])?;
    assert_eq!(nr_updated, 1);
    Ok(())
}

fn dec_nlink(txn: &Transaction, ino: Ino) -> Result<u32> {
    let mut stmt =
        txn.prepare_cached("update Inodes set nlink = nlink - 1 where ino = ? and nlink > 0")?;
    let nr_updated = stmt.execute(&[&(ino as i64)])?;
    assert_eq!(nr_updated, 1);
    let mut stmt = txn.prepare_cached("select nlink from Inodes where ino = ?")?;
    Ok(stmt
        .query_row(&[&(ino as i64)], |row| Ok(row.get(0)?))
        .optional()?
        .unwrap())
}

fn delete_inode(txn: &Transaction, ino: Ino) -> Result<()> {
    debug!("deleting inode {}", ino);
    // FIXME: check whether directory is empty.
    let mut stmt = txn.prepare_cached("delete from Inodes where ino = ?")?;
    match stmt.execute(&[&(ino as i64)]) {
        Ok(nr_updated) => {
            assert_eq!(nr_updated, 1);
            Ok(())
        }
        Err(rusqlite::Error::SqliteFailure(err, _))
            if err.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            Err(Error::NotEmpty(ino))
        }
        Err(err) => Err(err.into()),
    }
}

pub struct DirEntry {
    pub ino: Ino,
    pub file_type: FileType,
}

pub enum FileType {
    MutableRegular,
    ImmutableRegular,
    Directory,
    Symlink,
}

impl Filesystem {
    pub fn read_directory(&self, dir: Ino) -> Result<BTreeMap<String, DirEntry>> {
        let conn = self.pool.get()?;

        // FIXME: check whether dir is a directory.
        let mut stmt =
            conn.prepare_cached("select name, ino, type from DirEntries where dir = ?")?;

        let mut res = BTreeMap::new();

        for x in stmt.query_map(&[&(dir as i64)], |row| {
            Ok((row.get(0)?, row.get::<_, i64>(1)? as Ino, row.get(2)?))
        })? {
            let (name, ino, file_type) = x?;
            let file_type = match file_type {
                1 => FileType::MutableRegular,
                2 => FileType::ImmutableRegular,
                3 => FileType::Directory,
                4 => FileType::Symlink,
                n => panic!(
                    "Directory entry {}/{} has invalid file type {}.",
                    ino, name, n
                ),
            };
            res.insert(name, DirEntry { ino, file_type });
        }

        Ok(res)
    }

    pub fn lookup(&self, dir: Ino, name: &str) -> Result<Stat> {
        lookup(&self.pool.get()?.transaction()?, dir, name)
    }
}

pub fn lookup(txn: &Transaction, dir: Ino, name: &str) -> Result<Stat> {
    // FIXME: check whether dir is a directory.
    let mut stmt = txn.prepare_cached("select ino from DirEntries where dir = ? and name = ?")?;

    if let Some(ino) = stmt
        .query_row(&[&(dir as i64), &name as &dyn ToSql], |row| {
            Ok(row.get::<_, i64>(0)? as Ino)
        })
        .optional()?
    {
        stat(txn, ino)
    } else {
        Err(Error::NoSuchEntry)
    }
}

impl Filesystem {
    /*
    pub fn import_json<R: Read>(&self, json_data: &mut R) -> Result<()> {
        serde_json::from_reader(json_data)
    }
    */
}

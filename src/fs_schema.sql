create table if not exists Inodes (
    ino    integer primary key autoincrement not null,
    -- 1: mutable regular file
    -- 2: immutable regular file
    -- 3: directory
    -- 4: symlink
    type   integer not null,
    perm   integer not null,
    uid    integer not null,
    gid    integer not null,
    nlink  integer not null,
    crtime integer not null, -- creation time in nanoseconds since the epoch
    mtime  integer not null, -- modification time in nanoseconds since the epoch
    length integer,
    ptr    blob              -- hash (for immutable files) or backing store file name (for mutable files)
);

create table if not exists DirEntries (
    dir    integer not null,
    name   text not null,
    ino    integer not null,
    type   integer not null, -- denormal; duplicates Inodes.type
    primary key (dir, name),
    foreign key (dir) references Inodes(ino) on delete restrict,
    foreign key (ino) references Inodes(ino) on delete restrict
);

create table if not exists Symlinks (
    ino    integer primary key not null,
    target text not null,
    foreign key (ino) references Inodes(ino) on delete cascade
);

create table if not exists Root (
    root primary key not null,
    foreign key (root) references Inodes(ino) on delete restrict
);

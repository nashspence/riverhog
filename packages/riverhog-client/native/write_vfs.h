#ifndef RIVERHOG_WRITE_VFS_H
#define RIVERHOG_WRITE_VFS_H
#include <wchar.h>
#include "write_quota.h"
typedef struct rh_write_vfs rh_write_vfs;
rh_write_vfs *rh_write_vfs_create(const char *, const char *, rh_quota *, rh_read_quota *, const char **);
int rh_write_vfs_bind(rh_write_vfs *, const char *, void *);
int rh_write_vfs_unbind(rh_write_vfs *);
int rh_write_vfs_bound(rh_write_vfs *);
int rh_write_vfs_arm(rh_write_vfs *, rh_quota *, rh_read_quota *);
int rh_write_vfs_close(rh_write_vfs *);
int rh_write_vfs_open_files(rh_write_vfs *);
const char *rh_write_vfs_source_id(rh_write_vfs *);
const wchar_t *rh_write_vfs_library_path(rh_write_vfs *);
#endif

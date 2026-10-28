#ifndef RIVERHOG_WRITE_VFS_H
#define RIVERHOG_WRITE_VFS_H
#include <wchar.h>
#include "write_quota.h"
typedef struct rh_write_vfs rh_write_vfs;
typedef struct rh_sqlite_heap rh_sqlite_heap;
/* The application keeps one SQLite-accounted heap owner for the process.
 * Grant increases are explicit and monotonic. Default heap-backed caches/no supplied pagecache pool are required.
 */
rh_sqlite_heap *rh_sqlite_heap_create(const char *, int64_t, const char **);
int rh_sqlite_heap_report(rh_sqlite_heap *, int64_t *, int64_t *, int64_t *);
int64_t rh_sqlite_heap_limit(rh_sqlite_heap *);
int rh_sqlite_heap_increase(rh_sqlite_heap *, int64_t);
rh_write_vfs *rh_write_vfs_create(const char *, const char *, rh_quota *, rh_read_quota *, rh_callback_quota *, rh_sqlite_heap *, const char **);
int rh_write_vfs_bind(rh_write_vfs *, const char *, void *, const uint32_t *);
int rh_write_vfs_unbind(rh_write_vfs *);
int rh_write_vfs_bound(rh_write_vfs *);
int rh_write_vfs_arm(rh_write_vfs *, rh_quota *, rh_read_quota *, rh_callback_quota *);
int rh_write_vfs_close(rh_write_vfs *);
int rh_write_vfs_open_files(rh_write_vfs *);
const char *rh_write_vfs_source_id(rh_write_vfs *);
const wchar_t *rh_write_vfs_library_path(rh_write_vfs *);
#endif

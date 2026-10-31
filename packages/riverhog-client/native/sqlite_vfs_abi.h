#ifndef RIVERHOG_SQLITE_VFS_ABI_H
#define RIVERHOG_SQLITE_VFS_ABI_H
/* SQLite's public version-one VFS ABI; no engine implementation or linkage.
 * https://sqlite.org/c3ref/io_methods.html
 * https://sqlite.org/c3ref/vfs.html
 */
typedef long long rh_sqlite_int64;
typedef struct rh_sqlite_file rh_sqlite_file;
typedef struct rh_sqlite_io rh_sqlite_io;
typedef struct rh_sqlite_vfs rh_sqlite_vfs;
struct rh_sqlite_file { const rh_sqlite_io *methods; };
struct rh_sqlite_io {
    int version;
    int (*close)(rh_sqlite_file *);
    int (*read)(rh_sqlite_file *, void *, int, rh_sqlite_int64);
    int (*write)(rh_sqlite_file *, const void *, int, rh_sqlite_int64);
    int (*truncate)(rh_sqlite_file *, rh_sqlite_int64);
    int (*sync)(rh_sqlite_file *, int);
    int (*size)(rh_sqlite_file *, rh_sqlite_int64 *);
    int (*lock)(rh_sqlite_file *, int);
    int (*unlock)(rh_sqlite_file *, int);
    int (*reserved)(rh_sqlite_file *, int *);
    int (*control)(rh_sqlite_file *, int, void *);
    int (*sector)(rh_sqlite_file *);
    int (*characteristics)(rh_sqlite_file *);
};
struct rh_sqlite_vfs {
    int version, file_bytes, pathname_bytes;
    rh_sqlite_vfs *next;
    const char *name;
    void *data;
    int (*open)(rh_sqlite_vfs *, const char *, rh_sqlite_file *, int, int *);
    int (*delete_file)(rh_sqlite_vfs *, const char *, int);
    int (*access)(rh_sqlite_vfs *, const char *, int, int *);
    int (*fullpath)(rh_sqlite_vfs *, const char *, int, char *);
    void *(*dlopen_file)(rh_sqlite_vfs *, const char *);
    void (*dlerror_file)(rh_sqlite_vfs *, int, char *);
    void (*(*dlsym_file)(rh_sqlite_vfs *, void *, const char *))(void);
    void (*dlclose_file)(rh_sqlite_vfs *, void *);
    int (*randomness)(rh_sqlite_vfs *, int, char *);
    int (*sleep)(rh_sqlite_vfs *, int);
    int (*time)(rh_sqlite_vfs *, double *);
    int (*error)(rh_sqlite_vfs *, int, char *);
};
#endif

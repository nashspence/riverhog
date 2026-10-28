#include "write_vfs.h"
#include "sqlite_vfs_abi.h"
#include <limits.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

#define RH_OK 0
#define RH_BUSY 5
#define RH_IOERR 10
#define RH_FULL 13
#define RH_MISUSE 21
#define RH_CANTOPEN 14
#define RH_SHORT_READ (RH_IOERR | (2 << 8))
#define RH_MAIN_DB 0x00000100
#define RH_MAIN_JOURNAL 0x00000800
#define RH_FILE_CLASSES 0x00087F00
#define RH_DELETE_ON_CLOSE 0x00000008
#define RH_MEMORY_FILE 0x00000080
/* https://sqlite.org/c3ref/c_fcntl_begin_atomic_write.html */
#define RH_SIZE_HINT 5
#define RH_CHUNK_SIZE 6
#define RH_WIN32_AV_RETRY 9
#define RH_MMAP_SIZE 18
#define RH_SET_HANDLE 23
#define RH_BEGIN_ATOMIC 31
#define RH_COMMIT_ATOMIC 32
#define RH_ROLLBACK_ATOMIC 33
#define RH_NULL_IO 43
#define RH_GET_HANDLE 29

typedef struct {
    const char *(*source_id)(void);
    rh_sqlite_vfs *(*find)(const char *);
    int (*register_vfs)(rh_sqlite_vfs *, int);
    int (*unregister_vfs)(rh_sqlite_vfs *);
    void *module;
} rh_sqlite_api;

struct rh_write_vfs {
    rh_sqlite_vfs vfs;
    rh_sqlite_vfs *base;
    rh_sqlite_api api;
    rh_quota *quota;
    rh_read_quota *reads;
    unsigned long thread;
    size_t offset;
    int open_files, bound;
    size_t path_capacity;
    char *database_name, *journal_name;
    wchar_t *database_wide, *journal_wide, *actual_wide;
    void *database_handle, *journal_handle;
    uint32_t database_volume, database_index_high, database_index_low;
    uint32_t journal_index_high, journal_index_low;
    wchar_t library_path[32768];
};

/* Align the delegate for native pointer, integer and floating members. */
typedef union {
    void *pointer;
    rh_sqlite_int64 integer;
    long double real;
} rh_file_alignment;

typedef struct {
    rh_sqlite_file file;
    rh_write_vfs *owner;
    enum rh_file kind;
} rh_write_file;

static unsigned long current_thread(void) {
#ifdef _WIN32
    return GetCurrentThreadId();
#else
    return 0;
#endif
}

static int owned(rh_write_vfs *vfs) { return current_thread() == vfs->thread; }
static rh_sqlite_file *real_file(rh_write_file *file) {
    return (rh_sqlite_file *)((char *)file + file->owner->offset);
}
static int storage_result(rh_write_vfs *vfs, int result) {
    if (result != RH_OK) rh_failed(vfs->quota);
    return result;
}

static int admitted_name(rh_write_vfs *vfs, const char *name, enum rh_file kind) {
    return vfs->bound && name && strcmp(name,
        kind == RH_DATABASE ? vfs->database_name : vfs->journal_name) == 0;
}
#ifdef _WIN32
static int regular_file(HANDLE handle, BY_HANDLE_FILE_INFORMATION *info) {
    FILE_STANDARD_INFO standard;
    return GetFileInformationByHandle(handle, info) &&
        GetFileInformationByHandleEx(handle, FileStandardInfo, &standard, (DWORD)sizeof(standard)) &&
        !(info->dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT |
          FILE_ATTRIBUTE_SPARSE_FILE | FILE_ATTRIBUTE_COMPRESSED | FILE_ATTRIBUTE_OFFLINE |
          FILE_ATTRIBUTE_RECALL_ON_OPEN | FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS)) &&
        info->nNumberOfLinks == 1 && standard.NumberOfLinks == 1 &&
        !standard.DeletePending && !standard.Directory &&
        standard.EndOfFile.QuadPart >= 0 && standard.AllocationSize.QuadPart >= 0;
}
static int current_path(rh_write_vfs *vfs, HANDLE handle, const wchar_t *expected) {
    DWORD length = GetFinalPathNameByHandleW(handle, vfs->actual_wide,
                                           (DWORD)vfs->path_capacity, 0);
    return length > 4 && length < vfs->path_capacity &&
        wcsncmp(vfs->actual_wide, L"\\\\?\\", 4) == 0 &&
        CompareStringOrdinal(vfs->actual_wide + 4, -1, expected, -1, TRUE) == CSTR_EQUAL;
}
static int journal_absent(rh_write_vfs *vfs) {
    DWORD error;
    if (GetFileAttributesW(vfs->journal_wide) != INVALID_FILE_ATTRIBUTES) return 0;
    error = GetLastError();
    return error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND;
}
static int journal_current(rh_write_vfs *vfs) {
    BY_HANDLE_FILE_INFORMATION info;
    return vfs->journal_handle &&
        regular_file(vfs->journal_handle, &info) &&
        current_path(vfs, vfs->journal_handle, vfs->journal_wide) &&
        info.dwVolumeSerialNumber == vfs->database_volume &&
        info.nFileIndexHigh == vfs->journal_index_high &&
        info.nFileIndexLow == vfs->journal_index_low;
}
static int pin_journal(rh_write_vfs *vfs, uint32_t high, uint32_t low) {
    HANDLE handle = CreateFileW(vfs->journal_wide, FILE_READ_ATTRIBUTES,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
        FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    if (handle == INVALID_HANDLE_VALUE) return 0;
    /* The delete-sharing metadata pin prevents file-ID reuse. Only a successful
     * delegated journal deletion releases this generation during a binding.
     */
    vfs->journal_handle = handle;
    vfs->journal_index_high = high;
    vfs->journal_index_low = low;
    return journal_current(vfs);
}
#endif
static int opened_identity(rh_write_vfs *vfs, rh_sqlite_file *real, enum rh_file kind) {
#ifdef _WIN32
    HANDLE handle = NULL;
    BY_HANDLE_FILE_INFORMATION info;
    if (real->methods->control(real, RH_GET_HANDLE, &handle) != RH_OK ||
        !handle || !regular_file(handle, &info) ||
        !current_path(vfs, handle, kind == RH_DATABASE ? vfs->database_wide : vfs->journal_wide))
        return 0;
    if (info.dwVolumeSerialNumber != vfs->database_volume) return 0;
    if (kind == RH_DATABASE)
        return info.nFileIndexHigh == vfs->database_index_high &&
               info.nFileIndexLow == vfs->database_index_low;
    if (!vfs->journal_handle &&
        !pin_journal(vfs, info.nFileIndexHigh, info.nFileIndexLow)) return 0;
    return journal_current(vfs) &&
        info.nFileIndexHigh == vfs->journal_index_high &&
        info.nFileIndexLow == vfs->journal_index_low;
#else
    (void)vfs; (void)real; (void)kind;
    return 0;
#endif
}

static int file_close(rh_sqlite_file *raw) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    int result;
    if (!owned(file->owner)) return RH_IOERR;
    result = real->methods->close(real);
    /* SQLite does not retry xClose after its returned error. */
    --file->owner->open_files;
    raw->methods = NULL;
    return storage_result(file->owner, result);
}
static int file_read(rh_sqlite_file *raw, void *buffer, int amount, rh_sqlite_int64 offset) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    int result;
    if (!owned(file->owner)) return RH_IOERR;
    if (rh_read_admit(file->owner->reads, file->owner->quota, file->kind,
                      (int64_t)amount, (int64_t)offset) != RH_ALLOWED) return RH_FULL;
    result = real->methods->read(real, buffer, amount, offset);
    if (result != RH_OK && result != RH_SHORT_READ) rh_failed(file->owner->quota);
    return result;
}
static int file_write(rh_sqlite_file *raw, const void *buffer, int amount, rh_sqlite_int64 offset) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    if (!owned(file->owner)) return RH_IOERR;
    if (rh_admit(file->owner->quota, file->kind, (int64_t)amount, (int64_t)offset) != RH_ALLOWED)
        return RH_FULL;
    return storage_result(file->owner, real->methods->write(real, buffer, amount, offset));
}
static int file_truncate(rh_sqlite_file *raw, rh_sqlite_int64 size) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    if (!owned(file->owner)) return RH_IOERR;
    if (rh_resize(file->owner->quota, file->kind, (int64_t)size) != RH_ALLOWED) return RH_FULL;
    return storage_result(file->owner, real->methods->truncate(real, size));
}
static int file_sync(rh_sqlite_file *raw, int flags) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    if (!owned(file->owner)) return RH_IOERR;
    return storage_result(file->owner, real->methods->sync(real, flags));
}
static int file_size(rh_sqlite_file *raw, rh_sqlite_int64 *size) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    if (!owned(file->owner)) return RH_IOERR;
    return real->methods->size(real, size);
}
static int file_lock(rh_sqlite_file *raw, int level) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    if (!owned(file->owner)) return RH_IOERR;
    return real->methods->lock(real, level);
}
static int file_unlock(rh_sqlite_file *raw, int level) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    if (!owned(file->owner)) return RH_IOERR;
    return real->methods->unlock(real, level);
}
static int file_reserved(rh_sqlite_file *raw, int *result) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    if (!owned(file->owner)) return RH_IOERR;
    return real->methods->reserved(real, result);
}
static int file_control(rh_sqlite_file *raw, int operation, void *argument) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    if (!owned(file->owner)) return RH_IOERR;
    if (operation == RH_SIZE_HINT) {
        if (!argument) {
            rh_forbid_control(file->owner->quota, file->kind);
            return RH_MISUSE;
        }
        if (rh_resize(file->owner->quota, file->kind,
                      (int64_t)*(rh_sqlite_int64 *)argument) != RH_ALLOWED) return RH_FULL;
        return storage_result(file->owner, real->methods->control(real, operation, argument));
    }
    /* Keep native truncation exact: nonzero chunks round beyond the admitted end.
     * Version-one methods expose no mmap/WAL methods; reject alternate bypass controls.
     * Win32 retry changes are process-global and belong to the application owner.
     */
    if ((operation == RH_CHUNK_SIZE && (!argument || *(int *)argument != 0)) ||
        (operation == RH_MMAP_SIZE && (!argument || *(rh_sqlite_int64 *)argument > 0)) ||
        (operation == RH_WIN32_AV_RETRY &&
         (!argument || ((int *)argument)[0] >= 0 || ((int *)argument)[1] >= 0)) ||
        operation == RH_SET_HANDLE || operation == RH_BEGIN_ATOMIC ||
        operation == RH_COMMIT_ATOMIC || operation == RH_ROLLBACK_ATOMIC ||
        operation == RH_NULL_IO) {
        rh_forbid_control(file->owner->quota, file->kind);
        return RH_IOERR;
    }
    return real->methods->control(real, operation, argument);
}
static int file_sector(rh_sqlite_file *raw) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    return owned(file->owner) ? real->methods->sector(real) : 0;
}
static int file_characteristics(rh_sqlite_file *raw) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    return owned(file->owner) ? real->methods->characteristics(real) : 0;
}
static const rh_sqlite_io methods = {
    1, file_close, file_read, file_write, file_truncate, file_sync, file_size,
    file_lock, file_unlock, file_reserved, file_control, file_sector, file_characteristics
};

static int vfs_open(
    rh_sqlite_vfs *raw, const char *name, rh_sqlite_file *file, int flags, int *out_flags
) {
    rh_write_vfs *vfs = raw->data;
    rh_write_file *wrapper = (rh_write_file *)file;
    rh_sqlite_file *real;
    int result;
    file->methods = NULL;
    if (!owned(vfs) || vfs->open_files == INT_MAX) return RH_IOERR;
    wrapper->owner = vfs;
    wrapper->kind = (flags & RH_FILE_CLASSES) == RH_MAIN_DB ? RH_DATABASE :
        (flags & RH_FILE_CLASSES) == RH_MAIN_JOURNAL ? RH_JOURNAL : RH_OTHER;
    if (wrapper->kind == RH_OTHER || !admitted_name(vfs, name, wrapper->kind) ||
        (flags & (RH_DELETE_ON_CLOSE | RH_MEMORY_FILE))) {
        (void)rh_admit(vfs->quota, RH_OTHER, 0, 0);
        return RH_CANTOPEN;
    }
#ifdef _WIN32
    if (wrapper->kind == RH_JOURNAL &&
        !(vfs->journal_handle ? journal_current(vfs) : journal_absent(vfs))) {
        (void)rh_admit(vfs->quota, RH_OTHER, 0, 0);
        return RH_CANTOPEN;
    }
#endif
    real = real_file(wrapper);
    real->methods = NULL;
    result = vfs->base->open(vfs->base, name, real, flags, out_flags);
    if (real->methods) {
        ++vfs->open_files;
        file->methods = &methods;
    }
    if (result == RH_OK && (!real->methods || !opened_identity(vfs, real, wrapper->kind))) {
        (void)rh_admit(vfs->quota, RH_OTHER, 0, 0);
        return RH_CANTOPEN;
    }
    if (result != RH_OK) rh_failed(vfs->quota);
    return result;
}
static int vfs_delete(rh_sqlite_vfs *raw, const char *name, int sync_directory) {
    rh_write_vfs *vfs = raw->data;
    if (!owned(vfs)) return RH_IOERR;
    if (!admitted_name(vfs, name, RH_JOURNAL)) {
        (void)rh_admit(vfs->quota, RH_OTHER, 0, 0);
        return RH_IOERR;
    }
#ifdef _WIN32
    if (!(vfs->journal_handle ? journal_current(vfs) : journal_absent(vfs))) {
        (void)rh_admit(vfs->quota, RH_OTHER, 0, 0);
        return RH_IOERR;
    }
    {
        int result = vfs->base->delete_file(vfs->base, name, sync_directory);
        if (result == RH_OK && vfs->journal_handle) {
            if (!CloseHandle(vfs->journal_handle)) return storage_result(vfs, RH_IOERR);
            vfs->journal_handle = NULL;
        }
        return storage_result(vfs, result);
    }
#else
    (void)sync_directory;
    return RH_IOERR;
#endif
}
static int vfs_access(rh_sqlite_vfs *raw, const char *name, int flags, int *result) {
    rh_write_vfs *vfs = raw->data;
    if (!owned(vfs)) return RH_IOERR;
    return vfs->base->access(vfs->base, name, flags, result);
}
static int vfs_fullpath(rh_sqlite_vfs *raw, const char *name, int size, char *out) {
    rh_write_vfs *vfs = raw->data;
    if (!owned(vfs)) return RH_IOERR;
    return vfs->base->fullpath(vfs->base, name, size, out);
}
static void *vfs_dlopen(rh_sqlite_vfs *raw, const char *name) {
    rh_write_vfs *vfs = raw->data;
    return vfs->base->dlopen_file(vfs->base, name);
}
static void vfs_dlerror(rh_sqlite_vfs *raw, int size, char *out) {
    rh_write_vfs *vfs = raw->data;
    vfs->base->dlerror_file(vfs->base, size, out);
}
static void (*vfs_dlsym(rh_sqlite_vfs *raw, void *handle, const char *name))(void) {
    rh_write_vfs *vfs = raw->data;
    return vfs->base->dlsym_file(vfs->base, handle, name);
}
static void vfs_dlclose(rh_sqlite_vfs *raw, void *handle) {
    rh_write_vfs *vfs = raw->data;
    vfs->base->dlclose_file(vfs->base, handle);
}
static int vfs_random(rh_sqlite_vfs *raw, int size, char *out) {
    rh_write_vfs *vfs = raw->data;
    return vfs->base->randomness(vfs->base, size, out);
}
static int vfs_sleep(rh_sqlite_vfs *raw, int microseconds) {
    rh_write_vfs *vfs = raw->data;
    return vfs->base->sleep(vfs->base, microseconds);
}
static int vfs_time(rh_sqlite_vfs *raw, double *out) {
    rh_write_vfs *vfs = raw->data;
    return vfs->base->time(vfs->base, out);
}
static int vfs_error(rh_sqlite_vfs *raw, int size, char *out) {
    rh_write_vfs *vfs = raw->data;
    return vfs->base->error ? vfs->base->error(vfs->base, size, out) : 0;
}

static void release_module(rh_sqlite_api *api) {
#ifdef _WIN32
    if (api->module) FreeLibrary((HMODULE)api->module);
#else
    (void)api;
#endif
}
static int bind_api(rh_write_vfs *vfs, const char **error) {
#ifdef _WIN32
    HMODULE module;
    FARPROC symbol;
    DWORD length;
    if (!GetModuleHandleExW(0, L"sqlite3.dll", &module)) {
        *error = "the already loaded Windows SQLite module is unavailable";
        return 0;
    }
    vfs->api.module = module;
#define RH_BIND(member, exported) \
    symbol = GetProcAddress(module, exported); \
    if (!symbol) { *error = "the loaded SQLite public API is incomplete"; return 0; } \
    _Static_assert(sizeof(vfs->api.member) == sizeof(symbol), "Windows API pointer size"); \
    memcpy(&vfs->api.member, &symbol, sizeof(symbol))
    RH_BIND(source_id, "sqlite3_sourceid");
    RH_BIND(find, "sqlite3_vfs_find");
    RH_BIND(register_vfs, "sqlite3_vfs_register");
    RH_BIND(unregister_vfs, "sqlite3_vfs_unregister");
#undef RH_BIND
    length = GetModuleFileNameW(module, vfs->library_path, 32768);
    if (!length || length >= 32768) {
        *error = "the loaded SQLite module path is unavailable";
        return 0;
    }
    return 1;
#else
    (void)vfs;
    *error = "native Windows SQLite VFS is required";
    return 0;
#endif
}

rh_write_vfs *rh_write_vfs_create(
    const char *name, const char *expected_source, rh_quota *quota,
    rh_read_quota *reads, const char **error
) {
    rh_write_vfs *vfs = calloc(1, sizeof(*vfs));
    if (!vfs) { *error = "native VFS allocation failed"; return NULL; }
    if (!bind_api(vfs, error)) goto failed;
    if (strcmp(vfs->api.source_id(), expected_source)) {
        *error = "the loaded SQLite source identity differs from stdlib";
        goto failed;
    }
    vfs->base = vfs->api.find("win32");
    if (!vfs->base || vfs->base->version < 1 || vfs->base->file_bytes <= 0 ||
        vfs->api.find(name)) {
        *error = "the normal Win32 VFS is unavailable or the wrapper name is registered";
        goto failed;
    }
    if (vfs->base->pathname_bytes <= 8 || vfs->base->pathname_bytes > INT_MAX - 9) {
        *error = "the native VFS pathname capacity is invalid";
        goto failed;
    }
    vfs->path_capacity = (size_t)vfs->base->pathname_bytes + 9;
    if (vfs->path_capacity > SIZE_MAX / (3 * sizeof(wchar_t))) {
        *error = "the native VFS pathname allocation exceeds addressable capacity";
        goto failed;
    }
    vfs->database_name = calloc(2 * vfs->path_capacity, 1);
    vfs->database_wide = calloc(3 * vfs->path_capacity, sizeof(wchar_t));
    if (!vfs->database_name || !vfs->database_wide) {
        *error = "the native VFS pathname allocation failed";
        goto failed;
    }
    vfs->journal_name = vfs->database_name + vfs->path_capacity;
    vfs->journal_wide = vfs->database_wide + vfs->path_capacity;
    vfs->actual_wide = vfs->journal_wide + vfs->path_capacity;
    vfs->offset = (sizeof(rh_write_file) + _Alignof(rh_file_alignment) - 1) /
        _Alignof(rh_file_alignment) * _Alignof(rh_file_alignment);
    if (vfs->offset > (size_t)INT_MAX - (size_t)vfs->base->file_bytes) {
        *error = "the native VFS file allocation exceeds its public ABI";
        goto failed;
    }
    vfs->thread = current_thread();
    vfs->quota = quota;
    vfs->reads = reads;
    vfs->vfs = (rh_sqlite_vfs){
        .version = 1, .file_bytes = (int)(vfs->offset + (size_t)vfs->base->file_bytes),
        .pathname_bytes = vfs->base->pathname_bytes, .name = name, .data = vfs,
        .open = vfs_open, .delete_file = vfs_delete, .access = vfs_access,
        .fullpath = vfs_fullpath, .dlopen_file = vfs_dlopen, .dlerror_file = vfs_dlerror,
        .dlsym_file = vfs_dlsym, .dlclose_file = vfs_dlclose, .randomness = vfs_random,
        .sleep = vfs_sleep, .time = vfs_time, .error = vfs_error
    };
    if (vfs->api.register_vfs(&vfs->vfs, 0) != RH_OK) {
        *error = "native VFS registration failed";
        goto failed;
    }
    return vfs;
failed:
    release_module(&vfs->api);
    free(vfs->database_name);
    free(vfs->database_wide);
    free(vfs);
    return NULL;
}
int rh_write_vfs_bind(
    rh_write_vfs *vfs, const char *path, void *borrowed_handle, const uint32_t *journal_identity
) {
    if (!owned(vfs)) return RH_MISUSE;
    if (vfs->open_files || vfs->bound || vfs->database_handle || vfs->journal_handle) return RH_BUSY;
#ifdef _WIN32
    {
        HANDLE pinned = NULL;
        BY_HANDLE_FILE_INFORMATION info;
        size_t length;
        int rc;
        if (!borrowed_handle || borrowed_handle == INVALID_HANDLE_VALUE ||
            !DuplicateHandle(GetCurrentProcess(), borrowed_handle, GetCurrentProcess(),
                             &pinned, 0, FALSE, DUPLICATE_SAME_ACCESS)) return RH_IOERR;
        /* Retain the pin even if a later check fails, until explicit unbind/close. */
        vfs->database_handle = pinned;
        if (!regular_file(pinned, &info)) return RH_IOERR;
        rc = vfs->base->fullpath(vfs->base, path, vfs->base->pathname_bytes + 1,
                                vfs->database_name);
        if (rc != RH_OK) return rc;
        length = strlen(vfs->database_name);
        if (length + 8 > (size_t)vfs->base->pathname_bytes) return RH_CANTOPEN;
        memcpy(vfs->journal_name, vfs->database_name, length);
        memcpy(vfs->journal_name + length, "-journal", 9);
        if (!MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, vfs->database_name, -1,
                                vfs->database_wide, (int)vfs->path_capacity) ||
            !MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, vfs->journal_name, -1,
                                vfs->journal_wide, (int)vfs->path_capacity) ||
            !current_path(vfs, pinned, vfs->database_wide)) return RH_IOERR;
        vfs->database_volume = info.dwVolumeSerialNumber;
        vfs->database_index_high = info.nFileIndexHigh;
        vfs->database_index_low = info.nFileIndexLow;
        if (journal_identity) {
            if (journal_identity[0] != vfs->database_volume ||
                !pin_journal(vfs, journal_identity[1], journal_identity[2])) return RH_IOERR;
        } else if (!journal_absent(vfs)) {
            return RH_IOERR;
        }
        vfs->bound = 1;
        return RH_OK;
    }
#else
    (void)path; (void)borrowed_handle; (void)journal_identity;
    return RH_IOERR;
#endif
}
int rh_write_vfs_unbind(rh_write_vfs *vfs) {
    if (!owned(vfs)) return RH_MISUSE;
    if (vfs->open_files) return RH_BUSY;
#ifdef _WIN32
    if (vfs->journal_handle) {
        if (!CloseHandle(vfs->journal_handle)) return RH_IOERR;
        vfs->journal_handle = NULL;
    }
    if (vfs->database_handle && !CloseHandle(vfs->database_handle)) return RH_IOERR;
#endif
    vfs->database_handle = NULL;
    vfs->bound = 0;
    return RH_OK;
}
int rh_write_vfs_bound(rh_write_vfs *vfs) { return owned(vfs) ? vfs->bound : -1; }

int rh_write_vfs_arm(rh_write_vfs *vfs, rh_quota *quota, rh_read_quota *reads) {
    if (!owned(vfs)) return RH_MISUSE;
    if (vfs->quota->recovering && vfs->open_files) return RH_BUSY;
    vfs->quota = quota;
    vfs->reads = reads;
    return RH_OK;
}
int rh_write_vfs_close(rh_write_vfs *vfs) {
    int result;
    if (!owned(vfs)) return RH_MISUSE;
    if (vfs->open_files) return RH_BUSY;
    result = rh_write_vfs_unbind(vfs);
    if (result != RH_OK) return result;
    result = vfs->api.unregister_vfs(&vfs->vfs);
    if (result != RH_OK) return result;
    release_module(&vfs->api);
    free(vfs->database_name);
    free(vfs->database_wide);
    free(vfs);
    return RH_OK;
}
int rh_write_vfs_open_files(rh_write_vfs *vfs) {
    return owned(vfs) ? vfs->open_files : -1;
}
const char *rh_write_vfs_source_id(rh_write_vfs *vfs) { return vfs->api.source_id(); }
const wchar_t *rh_write_vfs_library_path(rh_write_vfs *vfs) { return vfs->library_path; }

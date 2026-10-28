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
#define RH_PERSIST_WAL 10
#define RH_POWERSAFE_OVERWRITE 13
#define RH_TEMP_FILENAME 16
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
    rh_sqlite_int64 (*hard_heap_limit)(rh_sqlite_int64);
    int (*status)(int, rh_sqlite_int64 *, rh_sqlite_int64 *, int);
    void *(*allocate)(uint64_t);
    void (*release)(void *);
    const char *(*uri_key)(const char *, int);
    const char *(*uri_parameter)(const char *, const char *);
    const char *(*create_filename)(const char *, const char *, const char *, int, const char **);
    void (*free_filename)(const char *);
    void *module;
} rh_sqlite_api;

struct rh_write_vfs {
    rh_sqlite_vfs vfs;
    rh_sqlite_vfs *base;
    rh_sqlite_api api;
    rh_quota *quota;
    rh_read_quota *reads;
    rh_callback_quota *calls;
    rh_sqlite_heap *heap;
    unsigned long thread;
    size_t offset;
    int open_files, bound, quarantined;
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
    int forward_close;
} rh_write_file;

static unsigned long current_thread(void) {
#ifdef _WIN32
    return GetCurrentThreadId();
#else
    return 0;
#endif
}

static int retry_usable(rh_sqlite_heap *);
static int owned(rh_write_vfs *vfs) { return current_thread() == vfs->thread; }
static int callback(rh_write_vfs *vfs, int can_error) {
    if (!owned(vfs) || vfs->quarantined) return 0;
    if (!retry_usable(vfs->heap)) {
        rh_forbid_retry(vfs->quota);
        if (!can_error) vfs->calls->pending_error = 1;
        return 0;
    }
    return rh_callback_admit(vfs->calls, vfs->quota, can_error) == RH_ALLOWED;
}
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
/* SQLite passes the original URI metadata for both database and journal names.
 * Inspect at most two admitted keys and one terminator; never copy or rewrite
 * that metadata before delegating the original filename.
 */
static int admitted_uri(rh_write_vfs *vfs, const char *name) {
    int index, seen = 0;
    for (index = 0; index < 3; ++index) {
        const char *key = vfs->api.uri_key(name, index);
        const char *value;
        int bit;
        if (!key) return 1;
        if (index == 2) return 0;
        value = vfs->api.uri_parameter(name, key);
        if (!value) return 0;
        if (strcmp(key, "vfs") == 0) {
            bit = 1;
            if (strcmp(value, vfs->vfs.name)) return 0;
        } else if (strcmp(key, "mode") == 0) {
            bit = 2;
            if (strcmp(value, "ro") && strcmp(value, "rw") && strcmp(value, "rwc")) return 0;
        } else return 0;
        if (seen & bit) return 0;
        seen |= bit;
    }
    return 0;
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
    {
        int pending = file->owner->calls->pending_error;
        int accounted = rh_callback_close(file->owner->calls, file->owner->quota,
                                           file->forward_close);
        file->owner->calls->pending_error = 0;
        result = storage_result(file->owner, real->methods->close(real));
        /* SQLite discards its file object after xClose, including on error.
         * A native failure cannot prove that its underlying handle was released.
         * Keep this owner, its pins and grants until application process exit;
         * other already-open files must still use their prepaid close paths.
         */
        if (result != RH_OK || !accounted) file->owner->quarantined = 1;
        /* SQLite does not retry xClose after its returned error. Always delegate
         * the close, including an impossible accounting inconsistency.
         */
        --file->owner->open_files;
        raw->methods = NULL;
        if (!accounted) {
            file->owner->quota->diagnostic_overflow = 1;
            rh_failed(file->owner->quota);
            return RH_IOERR;
        }
        return result == RH_OK && pending ? RH_FULL : result;
    }
}
static int file_read(rh_sqlite_file *raw, void *buffer, int amount, rh_sqlite_int64 offset) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    int result;
    if (!callback(file->owner, 1)) return RH_FULL;
    if (rh_read_admit(file->owner->reads, file->owner->quota, file->kind,
                      (int64_t)amount, (int64_t)offset) != RH_ALLOWED) return RH_FULL;
    result = real->methods->read(real, buffer, amount, offset);
    if (result != RH_OK && result != RH_SHORT_READ) rh_failed(file->owner->quota);
    return result;
}
static int file_write(rh_sqlite_file *raw, const void *buffer, int amount, rh_sqlite_int64 offset) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    if (!callback(file->owner, 1)) return RH_FULL;
    if (rh_admit(file->owner->quota, file->kind, (int64_t)amount, (int64_t)offset) != RH_ALLOWED)
        return RH_FULL;
    return storage_result(file->owner, real->methods->write(real, buffer, amount, offset));
}
static int file_truncate(rh_sqlite_file *raw, rh_sqlite_int64 size) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    if (!callback(file->owner, 1)) return RH_FULL;
    if (rh_resize(file->owner->quota, file->kind, (int64_t)size) != RH_ALLOWED) return RH_FULL;
    return storage_result(file->owner, real->methods->truncate(real, size));
}
static int file_sync(rh_sqlite_file *raw, int flags) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    if (!callback(file->owner, 1)) return RH_FULL;
    return storage_result(file->owner, real->methods->sync(real, flags));
}
static int file_size(rh_sqlite_file *raw, rh_sqlite_int64 *size) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    if (!callback(file->owner, 1)) return RH_FULL;
    return storage_result(file->owner, real->methods->size(real, size));
}
static int file_lock(rh_sqlite_file *raw, int level) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    if (!callback(file->owner, 1)) return RH_FULL;
    {
        int result = real->methods->lock(real, level);
        return result == RH_BUSY ? result : storage_result(file->owner, result);
    }
}
static int file_unlock(rh_sqlite_file *raw, int level) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    if (!callback(file->owner, 1)) return RH_FULL;
    return storage_result(file->owner, real->methods->unlock(real, level));
}
static int file_reserved(rh_sqlite_file *raw, int *result) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    if (!callback(file->owner, 1)) return RH_FULL;
    return storage_result(file->owner, real->methods->reserved(real, result));
}
static int file_control(rh_sqlite_file *raw, int operation, void *argument) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    if (!callback(file->owner, 1)) return RH_FULL;
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
        ((operation == RH_PERSIST_WAL || operation == RH_POWERSAFE_OVERWRITE) &&
         (!argument || *(int *)argument >= 0)) ||
        (operation == RH_WIN32_AV_RETRY &&
         (!argument || ((int *)argument)[0] >= 0 || ((int *)argument)[1] >= 0)) ||
        operation == RH_SET_HANDLE || operation == RH_BEGIN_ATOMIC ||
        operation == RH_COMMIT_ATOMIC || operation == RH_ROLLBACK_ATOMIC ||
        operation == RH_NULL_IO || operation == RH_TEMP_FILENAME) {
        rh_forbid_control(file->owner->quota, file->kind);
        return RH_IOERR;
    }
    {
        int result = real->methods->control(real, operation, argument);
        return result == 12 /* SQLITE_NOTFOUND: unsupported control */ ?
            result : storage_result(file->owner, result);
    }
}
static int file_sector(rh_sqlite_file *raw) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    return callback(file->owner, 0) ? real->methods->sector(real) : 0;
}
static int file_characteristics(rh_sqlite_file *raw) {
    rh_write_file *file = (rh_write_file *)raw;
    rh_sqlite_file *real = real_file(file);
    return callback(file->owner, 0) ? real->methods->characteristics(real) : 0;
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
    int result, close_bank;
    file->methods = NULL;
    if (!owned(vfs) || vfs->quarantined || vfs->open_files == INT_MAX) return RH_IOERR;
    if (!retry_usable(vfs->heap)) {
        rh_forbid_retry(vfs->quota);
        return RH_IOERR;
    }
    wrapper->owner = vfs;
    wrapper->kind = (flags & RH_FILE_CLASSES) == RH_MAIN_DB ? RH_DATABASE :
        (flags & RH_FILE_CLASSES) == RH_MAIN_JOURNAL ? RH_JOURNAL : RH_OTHER;
    if (wrapper->kind == RH_OTHER || !admitted_name(vfs, name, wrapper->kind) ||
        (flags & (RH_DELETE_ON_CLOSE | RH_MEMORY_FILE))) {
        (void)rh_admit(vfs->quota, RH_OTHER, 0, 0);
        return RH_CANTOPEN;
    }
    if (!admitted_uri(vfs, name)) {
        (void)rh_admit(vfs->quota, RH_OTHER, 0, 0);
        return RH_CANTOPEN;
    }
    close_bank = rh_callback_open(vfs->calls, vfs->quota);
    if (close_bank < 0) return RH_FULL;
    wrapper->forward_close = close_bank;
#ifdef _WIN32
    if (wrapper->kind == RH_JOURNAL &&
        !(vfs->journal_handle ? journal_current(vfs) : journal_absent(vfs))) {
        (void)rh_callback_abandon(vfs->calls, close_bank);
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
    } else {
        (void)rh_callback_abandon(vfs->calls, close_bank);
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
    if (!callback(vfs, 1)) return RH_FULL;
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
            if (!CloseHandle(vfs->journal_handle)) {
                vfs->quarantined = 1;
                return storage_result(vfs, RH_IOERR);
            }
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
    if (!callback(vfs, 1)) return RH_FULL;
    return vfs->base->access(vfs->base, name, flags, result);
}
static int vfs_fullpath(rh_sqlite_vfs *raw, const char *name, int size, char *out) {
    rh_write_vfs *vfs = raw->data;
    if (!callback(vfs, 1)) return RH_FULL;
    return vfs->base->fullpath(vfs->base, name, size, out);
}
static void forbid_loading(rh_write_vfs *vfs) {
    if (!owned(vfs)) return;
    rh_forbid_control(vfs->quota, RH_OTHER);
    vfs->calls->pending_error = 1;
}
static void *vfs_dlopen(rh_sqlite_vfs *raw, const char *name) {
    rh_write_vfs *vfs = raw->data;
    (void)name;
    forbid_loading(vfs);
    return NULL;
}
static void vfs_dlerror(rh_sqlite_vfs *raw, int size, char *out) {
    rh_write_vfs *vfs = raw->data;
    forbid_loading(vfs);
    if (size > 0 && out) out[0] = 0;
}
static void (*vfs_dlsym(rh_sqlite_vfs *raw, void *handle, const char *name))(void) {
    rh_write_vfs *vfs = raw->data;
    (void)handle; (void)name;
    forbid_loading(vfs);
    return NULL;
}
static void vfs_dlclose(rh_sqlite_vfs *raw, void *handle) {
    rh_write_vfs *vfs = raw->data;
    (void)handle;
    forbid_loading(vfs);
}
static int vfs_random(rh_sqlite_vfs *raw, int size, char *out) {
    rh_write_vfs *vfs = raw->data;
    return callback(vfs, 0) ? vfs->base->randomness(vfs->base, size, out) : 0;
}
static int vfs_sleep(rh_sqlite_vfs *raw, int microseconds) {
    rh_write_vfs *vfs = raw->data;
    if (!owned(vfs) || vfs->quarantined) return 0;
    if (!retry_usable(vfs->heap)) {
        rh_forbid_retry(vfs->quota);
        vfs->calls->pending_error = 1;
        return 0;
    }
    if (rh_callback_sleep(vfs->calls, vfs->quota, (int64_t)microseconds) != RH_ALLOWED) return 0;
    return vfs->base->sleep(vfs->base, microseconds);
}
static int vfs_time(rh_sqlite_vfs *raw, double *out) {
    rh_write_vfs *vfs = raw->data;
    return callback(vfs, 1) ? vfs->base->time(vfs->base, out) : RH_FULL;
}
static int vfs_error(rh_sqlite_vfs *raw, int size, char *out) {
    rh_write_vfs *vfs = raw->data;
    if (!callback(vfs, 0)) {
        if (size > 0 && out) out[0] = 0;
        return 0;
    }
    return vfs->base->error ? vfs->base->error(vfs->base, size, out) : 0;
}

static void release_module(rh_sqlite_api *api) {
#ifdef _WIN32
    if (api->module) FreeLibrary((HMODULE)api->module);
#else
    (void)api;
#endif
}
static int bind_api(rh_sqlite_api *api, wchar_t *library_path, const char **error) {
#ifdef _WIN32
    HMODULE module;
    FARPROC symbol;
    DWORD length;
    if (!GetModuleHandleExW(0, L"sqlite3.dll", &module)) {
        *error = "the already loaded Windows SQLite module is unavailable";
        return 0;
    }
    api->module = module;
#define RH_BIND(member, exported) \
    symbol = GetProcAddress(module, exported); \
    if (!symbol) { *error = "the loaded SQLite public API is incomplete"; return 0; } \
    _Static_assert(sizeof(api->member) == sizeof(symbol), "Windows API pointer size"); \
    memcpy(&api->member, &symbol, sizeof(symbol))
    RH_BIND(source_id, "sqlite3_sourceid");
    RH_BIND(find, "sqlite3_vfs_find");
    RH_BIND(register_vfs, "sqlite3_vfs_register");
    RH_BIND(unregister_vfs, "sqlite3_vfs_unregister");
    RH_BIND(hard_heap_limit, "sqlite3_hard_heap_limit64");
    RH_BIND(status, "sqlite3_status64");
    RH_BIND(allocate, "sqlite3_malloc64");
    RH_BIND(release, "sqlite3_free");
    RH_BIND(uri_key, "sqlite3_uri_key");
    RH_BIND(uri_parameter, "sqlite3_uri_parameter");
    RH_BIND(create_filename, "sqlite3_create_filename");
    RH_BIND(free_filename, "sqlite3_free_filename");
#undef RH_BIND
    length = GetModuleFileNameW(module, library_path, 32768);
    if (!length || length >= 32768) {
        *error = "the loaded SQLite module path is unavailable";
        return 0;
    }
    return 1;
#else
    (void)api; (void)library_path;
    *error = "native Windows SQLite VFS is required";
    return 0;
#endif
}

struct rh_sqlite_heap {
    rh_sqlite_api api;
    int64_t limit;
    void *sentinel;
    int ready;
    rh_retry_admission retry;
    rh_sqlite_file *bootstrap_file;
    const char *bootstrap_filename;
    wchar_t library_path[32768];
};
static rh_sqlite_heap *process_heap;
static int retry_usable(rh_sqlite_heap *heap) {
    return heap && (heap->retry.state == 0 || heap->retry.state == 2);
}
int rh_sqlite_retry_report(rh_sqlite_heap *heap, rh_retry_admission *report) {
    int64_t current, peak, allocations;
    if (!rh_sqlite_heap_report(heap, &current, &peak, &allocations)) return 0;
    *report = heap->retry;
    return 1;
}


int rh_sqlite_heap_report(
    rh_sqlite_heap *heap, int64_t *current, int64_t *peak, int64_t *allocations
) {
    rh_sqlite_int64 used, high, count, unused, pool, pool_peak;
    if (!heap || heap != process_heap || !heap->ready ||
        heap->api.hard_heap_limit(-1) != heap->limit ||
        heap->api.status(0, &used, &high, 0) != RH_OK ||
        heap->api.status(9, &count, &unused, 0) != RH_OK ||
        heap->api.status(1, &pool, &pool_peak, 0) != RH_OK ||
        used <= 0 || high < used || high > heap->limit || count <= 0 || pool || pool_peak)
        return 0;
    *current = (int64_t)used; *peak = (int64_t)high; *allocations = (int64_t)count;
    return 1;
}
int64_t rh_sqlite_heap_limit(rh_sqlite_heap *heap) { return heap->limit; }
int rh_sqlite_heap_increase(rh_sqlite_heap *heap, int64_t limit) {
    int64_t current, peak, allocations;
    if (!rh_sqlite_heap_report(heap, &current, &peak, &allocations) || limit <= heap->limit)
        return 0;
    if (heap->api.hard_heap_limit(limit) != heap->limit ||
        heap->api.hard_heap_limit(-1) != limit) {
        heap->ready = 0;
        return 0;
    }
    heap->limit = limit;
    return 1;
}
rh_sqlite_heap *rh_sqlite_heap_create(const char *source, int64_t limit, const char **error) {
    rh_sqlite_heap *heap;
    rh_sqlite_int64 used, peak, count, ignored, pool, pool_peak;
    void *sentinel = NULL;
    int64_t current, high, allocations;
    if (process_heap) {
        *error = "this process already has a SQLite heap owner or a quarantined admission";
        return NULL;
    }
    heap = calloc(1, sizeof(*heap));
    if (!heap) { *error = "SQLite heap owner allocation failed"; return NULL; }
    if (!bind_api(&heap->api, heap->library_path, error)) goto failed;
    if (strcmp(heap->api.source_id(), source)) {
        *error = "the loaded SQLite source identity differs from the heap grant";
        goto failed;
    }
    if (limit <= 0 || heap->api.hard_heap_limit(-1) != 0) {
        *error = "a positive heap grant and an unconfigured SQLite heap owner are required";
        goto failed;
    }
    /* One fixed lifetime allocation proves enabled accounting even with no
     * connections. It remains charged to H while this process owner is alive.
     */
    sentinel = heap->api.allocate(1);
    if (!sentinel || heap->api.status(0, &used, &peak, 0) != RH_OK ||
        heap->api.status(9, &count, &ignored, 0) != RH_OK ||
        heap->api.status(1, &pool, &pool_peak, 0) != RH_OK ||
        used <= 0 || count <= 0 || pool || pool_peak) {
        *error = "SQLite heap accounting and the default heap-backed cache are required";
        goto failed;
    }
    if (used > limit) {
        *error = "the heap grant is below current SQLite-accounted memory use";
        goto failed;
    }
    heap->limit = limit;
    /* Once a global change is attempted, retain the library and immutable owner
     * through process lifetime, including a failed post-admission verification.
     */
    heap->sentinel = sentinel;
    sentinel = NULL;
    process_heap = heap;
    if (heap->api.hard_heap_limit(limit) != 0) {
        *error = "SQLite heap admission conflicted with another owner; restart required";
        goto quarantined;
    }
    if (heap->api.status(0, &used, &peak, 1) != RH_OK) {
        *error = "SQLite heap admission could not establish its measurement baseline";
        goto quarantined;
    }
    heap->ready = 1;
    if (!rh_sqlite_heap_report(heap, &current, &high, &allocations)) {
        heap->ready = 0;
        *error = "SQLite heap admission verification failed; restart required";
        goto quarantined;
    }
    return heap;
failed:
    if (sentinel) heap->api.release(sentinel);
    release_module(&heap->api);
    free(heap);
    return NULL;
quarantined:
    if (sentinel) heap->api.release(sentinel);
    return NULL;
}

/* This recipe is qualified for the selected desktop-NT source and build.
 * Its compile-time initial tuple still belongs to the exact runtime profile.
 */
#define RH_RETRY_SOURCE "2024-01-30 16:01:20 e876e51a0ed5c5b3126f52e532044363a014bc594cfefa87ffb5b82257cc467a"
int rh_write_vfs_admit_retries(
    rh_write_vfs *vfs, int64_t initial_count, int64_t initial_delay,
    int64_t count, int64_t delay, uint64_t filesystem_calls, uint64_t sleep_microseconds,
    const char **error
) {
    rh_sqlite_heap *heap = vfs->heap;
    rh_retry_cost required, selected;
    rh_sqlite_file *file;
    const char *filename;
    int result, out_flags = 0, values[2], accepted = 0, retain = 0;
    int64_t current, peak, allocations;
    if (!owned(vfs) || vfs->quarantined || !vfs->bound || vfs->open_files ||
        !rh_sqlite_heap_report(heap, &current, &peak, &allocations)) {
        *error = "retry admission requires its owning thread, a current heap and a bound file-free owner";
        return 0;
    }
    if (heap->retry.state) {
        *error = "application retry admission is already configured or quarantined";
        return 0;
    }
    if (strcmp(vfs->api.source_id(), RH_RETRY_SOURCE)) {
        *error = "the loaded SQLite source has no qualified retry bootstrap recipe";
        return 0;
    }
    if (!rh_retry_bootstrap_cost(initial_count, initial_delay, &required) ||
        !rh_retry_bootstrap_cost(count, delay, &selected)) {
        *error = "retry profiles must preserve native signed-int delay and logging arithmetic";
        return 0;
    }
    if (filesystem_calls < required.filesystem_calls ||
        sleep_microseconds < required.sleep_microseconds) {
        rh_forbid_retry(vfs->quota);
        *error = "the explicit retry bootstrap grant is below its complete native envelope";
        return 0;
    }
    file = vfs->api.allocate((uint64_t)vfs->base->file_bytes);
    filename = vfs->api.create_filename(vfs->database_name, vfs->journal_name, "", 0, NULL);
    if (!file || !filename) {
        if (file) vfs->api.release(file);
        if (filename) vfs->api.free_filename(filename);
        rh_forbid_retry(vfs->quota);
        *error = "retry bootstrap storage could not be preallocated under the application heap";
        return 0;
    }
    memset(file, 0, (size_t)vfs->base->file_bytes);
    /* Precharge the whole attempt and its native close before the first open.
     * Keep one fixed record, including failure; never reset this global policy.
     */
    heap->retry = (rh_retry_admission){
        .state = 1, .initial_count = (int)initial_count, .initial_delay = (int)initial_delay,
        .count = (int)count, .delay = (int)delay, .prepaid = required
    };
    result = vfs->base->open(vfs->base, filename, file, RH_MAIN_DB | 0x2, &out_flags);
    if (result != RH_OK) {
        rh_failed(vfs->quota);
        heap->retry.failed_stage = 1; heap->retry.native_result = result;
        *error = "native retry bootstrap open failed; restart required";
        goto finished;
    }
    if (!file->methods || !file->methods->control || !file->methods->close) {
        retain = 1;
        rh_forbid_retry(vfs->quota);
        heap->retry.failed_stage = 2;
        *error = "native retry bootstrap returned an incomplete file; restart required";
        goto finished;
    }
    if (out_flags != 0x2 || !opened_identity(vfs, file, RH_DATABASE)) {
        rh_forbid_retry(vfs->quota);
        heap->retry.failed_stage = 2;
        *error = "native retry bootstrap identity or read-write mode differs; restart required";
        goto finished;
    }
    values[0] = values[1] = -1;
    result = file->methods->control(file, RH_WIN32_AV_RETRY, values);
    if (result != RH_OK) {
        rh_failed(vfs->quota);
        heap->retry.failed_stage = 3; heap->retry.native_result = result;
        *error = "native retry baseline query failed; restart required";
        goto finished;
    }
    if (values[0] != initial_count || values[1] != initial_delay) {
        rh_forbid_retry(vfs->quota);
        heap->retry.failed_stage = 3;
        *error = "native retry baseline differs from the qualified startup profile; restart required";
        goto finished;
    }
    values[0] = (int)count; values[1] = (int)delay;
    result = file->methods->control(file, RH_WIN32_AV_RETRY, values);
    if (result != RH_OK) {
        rh_failed(vfs->quota);
        heap->retry.failed_stage = 4; heap->retry.native_result = result;
        *error = "native retry configuration failed; restart required";
        goto finished;
    }
    values[0] = values[1] = -1;
    result = file->methods->control(file, RH_WIN32_AV_RETRY, values);
    if (result != RH_OK) {
        rh_failed(vfs->quota);
        heap->retry.failed_stage = 5; heap->retry.native_result = result;
        *error = "native retry configuration query failed; restart required";
        goto finished;
    }
    if (values[0] != count || values[1] != delay) {
        rh_forbid_retry(vfs->quota);
        heap->retry.failed_stage = 5;
        *error = "native retry configuration could not be verified; restart required";
        goto finished;
    }
    accepted = 1;
finished:
    if (file->methods) {
        if (file->methods->close) {
            heap->retry.close_attempted = 1;
            heap->retry.close_result = file->methods->close(file);
        } else heap->retry.close_result = RH_IOERR;
        if (!heap->retry.close_attempted || heap->retry.close_result != RH_OK) {
            retain = 1;
            rh_failed(vfs->quota);
            if (!heap->retry.failed_stage) heap->retry.failed_stage = 6;
            if (!heap->retry.native_result) heap->retry.native_result = heap->retry.close_result;
            *error = "native retry bootstrap close failed; restart required";
        }
    }
    if (retain) {
        heap->bootstrap_file = file;
        heap->bootstrap_filename = filename;
    } else {
        vfs->api.release(file);
        vfs->api.free_filename(filename);
    }
    if (!accepted || retain) {
        heap->retry.state = 3;
        vfs->quarantined = 1;
        return 0;
    }
    heap->retry.state = 2;
    return 1;
}

rh_write_vfs *rh_write_vfs_create(
    const char *name, const char *expected_source, rh_quota *quota,
    rh_read_quota *reads, rh_callback_quota *calls, rh_sqlite_heap *heap, const char **error
) {
    rh_write_vfs *vfs = calloc(1, sizeof(*vfs));
    if (!vfs) { *error = "native VFS allocation failed"; return NULL; }
    if (!bind_api(&vfs->api, vfs->library_path, error)) goto failed;
    {
        int64_t current, peak, allocations;
        if (!rh_sqlite_heap_report(heap, &current, &peak, &allocations) ||
            !retry_usable(heap) || heap->api.module != vfs->api.module) {
            *error = "a matching current application SQLite heap owner without retry quarantine is required";
            goto failed;
        }
    }
    vfs->heap = heap;
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
    vfs->calls = calls;
    if (!rh_callback_prepare(calls, quota, 0)) {
        *error = "callback grant has live files or an unreported error";
        goto failed;
    }
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
    if (vfs->quarantined || !retry_usable(vfs->heap)) return RH_IOERR;
    {
        int64_t current, peak, allocations;
        if (!rh_sqlite_heap_report(vfs->heap, &current, &peak, &allocations)) return RH_IOERR;
    }
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
    if (vfs->quarantined) return RH_IOERR;
    if (vfs->open_files) return RH_BUSY;
#ifdef _WIN32
    if (vfs->journal_handle) {
        if (!CloseHandle(vfs->journal_handle)) {
            vfs->quarantined = 1;
            return RH_IOERR;
        }
        vfs->journal_handle = NULL;
    }
    if (vfs->database_handle && !CloseHandle(vfs->database_handle)) {
        vfs->quarantined = 1;
        return RH_IOERR;
    }
#endif
    vfs->database_handle = NULL;
    vfs->bound = 0;
    return RH_OK;
}
int rh_write_vfs_bound(rh_write_vfs *vfs) { return owned(vfs) ? vfs->bound : -1; }
int rh_write_vfs_quarantined(rh_write_vfs *vfs) { return owned(vfs) ? vfs->quarantined : -1; }

int rh_write_vfs_arm(
    rh_write_vfs *vfs, rh_quota *quota, rh_read_quota *reads, rh_callback_quota *calls
) {
    if (!owned(vfs)) return RH_MISUSE;
    if (vfs->quarantined || !retry_usable(vfs->heap)) return RH_IOERR;
    if (vfs->quota->recovering && vfs->open_files) return RH_BUSY;
    {
        int64_t current, peak, allocations;
        if (!rh_sqlite_heap_report(vfs->heap, &current, &peak, &allocations)) return RH_IOERR;
    }
    if (calls == vfs->calls)
        return quota == vfs->quota && reads == vfs->reads ? RH_OK : RH_MISUSE;
    if (!rh_callback_prepare(calls, quota, (uint64_t)vfs->open_files)) return RH_BUSY;
    rh_callback_detach(vfs->calls);
    vfs->quota = quota;
    vfs->reads = reads;
    vfs->calls = calls;
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
    rh_callback_detach(vfs->calls);
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

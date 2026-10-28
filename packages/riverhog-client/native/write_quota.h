#ifndef RIVERHOG_WRITE_QUOTA_H
#define RIVERHOG_WRITE_QUOTA_H
#include <stddef.h>
#include <stdint.h>

enum rh_file { RH_OTHER = 0, RH_DATABASE = 1, RH_JOURNAL = 2 };
enum rh_refusal {
    RH_ALLOWED = 0, RH_FILE, RH_RANGE, RH_CALLS, RH_BYTES,
    RH_DATABASE_END, RH_JOURNAL_END, RH_PAGES, RH_TRACKING,
    RH_READ_RANGE, RH_READ_CALLS, RH_READ_BYTES,
    RH_RESIZE_RANGE, RH_RESIZE_CALLS, RH_CONTROL, RH_CALLBACK_CALLS, RH_CALLBACK_CLOSE, RH_SLEEP_RANGE, RH_SLEEP_GRANT, RH_RETRY, RH_NATIVE_CALLS, RH_NATIVE_SLEEP, RH_NATIVE_CLOSE, RH_NATIVE_RANGE
};

typedef struct {
    uint64_t page_bytes, database_pages, database_end, journal_end, write_bytes, calls;
} rh_limits;

typedef struct {
    rh_limits limits;
    uint64_t *pages;
    size_t *staged_slots;
    size_t slots;
    uint64_t page_count, database_bytes, journal_bytes, journal_extent, bytes, calls;
    uint64_t resize_calls, database_resize_extent, journal_resize_extent;
} rh_bank;

typedef struct {
    rh_bank forward, recovery;
    int recovering, first_recorded, diagnostic_overflow;
    uint64_t refusals, native_failures;
    enum rh_refusal first_reason;
    enum rh_file first_file;
    int64_t first_amount, first_offset;
    int first_recovery;
} rh_quota;

typedef struct {
    uint64_t byte_limit, call_limit, bytes, calls;
} rh_read_bank;
typedef struct { rh_read_bank forward, recovery; } rh_read_quota;

typedef struct { uint64_t limit, charged, sleep_limit, sleep_charged; } rh_callback_bank;
typedef struct {
    rh_callback_bank forward, recovery;
    uint64_t forward_files, recovery_files;
    int pending_error;
} rh_callback_quota;

/* Open prepays its own callback and its eventual close. A forward open also
 * reserves a recovery close, which ordinary recovery work cannot consume.
 */
enum rh_refusal rh_callback_admit(rh_callback_quota *, rh_quota *, int);
enum rh_refusal rh_callback_sleep(rh_callback_quota *, rh_quota *, int64_t);
int rh_callback_open(rh_callback_quota *, rh_quota *);
int rh_callback_close(rh_callback_quota *, rh_quota *, int);
int rh_callback_abandon(rh_callback_quota *, int);
int rh_callback_prepare(rh_callback_quota *, rh_quota *, uint64_t);
void rh_callback_detach(rh_callback_quota *);

enum rh_refusal rh_read_admit(rh_read_quota *, rh_quota *, enum rh_file, int64_t, int64_t);
enum rh_refusal rh_admit(rh_quota *, enum rh_file, int64_t, int64_t);
enum rh_refusal rh_resize(rh_quota *, enum rh_file, int64_t);
typedef struct { uint64_t filesystem_calls, sleep_microseconds; } rh_retry_cost;
/* Desktop-NT native open, opened-handle identity and prepaid native close.
 * This excludes name conversion, error formatting and caller binding work.
 */
int rh_retry_bootstrap_cost(int64_t, int64_t, rh_retry_cost *);
/* Complete source-qualified filesystem-attempt and nominal internal-wait costs.
 * Native conversion/formatting, CPU, RSS and scheduling delay are separate.
 */
typedef struct {
    rh_retry_cost limits, charged, close_prepaid;
} rh_native_work_bank;
typedef struct { rh_native_work_bank forward, recovery; } rh_native_work_quota;
/* Both possible banks are fully charged before forward acquisition. A credit
 * contains no bank pointer, so replacing/freeing an operation bank cannot lose
 * the admitted close or retain an old operation's page-tracking allocation.
 */
typedef struct { rh_retry_cost cost; int pending; } rh_native_close_credit;
enum rh_refusal rh_native_work_admit(rh_native_work_quota *, rh_quota *, rh_retry_cost);
enum rh_refusal rh_native_work_acquire(
    rh_native_work_quota *, rh_quota *, rh_retry_cost, rh_retry_cost, rh_native_close_credit *);
int rh_native_close_consume(rh_native_close_credit *);
enum rh_native_operation {
    RH_OP_READ, RH_OP_WRITE, RH_OP_TRUNCATE, RH_OP_SYNC, RH_OP_SIZE,
    RH_OP_LOCK, RH_OP_UNLOCK, RH_OP_RESERVED, RH_OP_CONTROL, RH_OP_SCALAR,
    RH_OP_OPEN_DATABASE, RH_OP_OPEN_JOURNAL_PINNED, RH_OP_OPEN_JOURNAL_NEW,
    RH_OP_DELETE_JOURNAL, RH_OP_ACCESS, RH_OP_FULLPATH,
    RH_OP_BIND_DATABASE, RH_OP_BIND_JOURNAL, RH_OP_CLOSE, RH_OP_PIN_CLOSE
};
/* SQLite 3.45.1 desktop NT, no mmap/chunk expansion/delete-on-close:
 * acquisition recipes exclude their independently prepaid close. Read/write
 * include a conservative optional seek. Conversion, formatting, time/random
 * APIs and caller buffers are outside this filesystem-attempt family.
 */
int rh_native_filesystem_cost(enum rh_native_operation, int64_t, int64_t, int64_t, rh_retry_cost *);


void rh_forbid_retry(rh_quota *);
void rh_forbid_native_range(rh_quota *);
void rh_forbid_control(rh_quota *, enum rh_file);
void rh_failed(rh_quota *);
const char *rh_reason(enum rh_refusal);
#endif

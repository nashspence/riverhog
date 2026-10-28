#ifndef RIVERHOG_WRITE_QUOTA_H
#define RIVERHOG_WRITE_QUOTA_H
#include <stddef.h>
#include <stdint.h>

enum rh_file { RH_OTHER = 0, RH_DATABASE = 1, RH_JOURNAL = 2 };
enum rh_refusal {
    RH_ALLOWED = 0, RH_FILE, RH_RANGE, RH_CALLS, RH_BYTES,
    RH_DATABASE_END, RH_JOURNAL_END, RH_PAGES, RH_TRACKING
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

enum rh_refusal rh_admit(rh_quota *, enum rh_file, int64_t, int64_t);
void rh_failed(rh_quota *);
const char *rh_reason(enum rh_refusal);
#endif

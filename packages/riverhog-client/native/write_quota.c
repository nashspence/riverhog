#include "write_quota.h"
#include <limits.h>

static size_t rh_hash(uint64_t page, size_t mask) {
    page ^= page >> 30;
    page *= UINT64_C(0xbf58476d1ce4e5b9);
    page ^= page >> 27;
    page *= UINT64_C(0x94d049bb133111eb);
    page ^= page >> 31;
    return (size_t)page & mask;
}

static void rh_increment(uint64_t *value, rh_quota *quota) {
    if (*value == UINT64_MAX) quota->diagnostic_overflow = 1;
    else ++*value;
}

static enum rh_refusal rh_charge(
    rh_bank *bank, enum rh_file file, int64_t amount, int64_t offset
) {
    uint64_t count, end, first, last, page;
    size_t staged = 0;
    if (file != RH_DATABASE && file != RH_JOURNAL) return RH_FILE;
    if (amount < 0 || offset < 0 || amount > INT64_MAX - offset) return RH_RANGE;
    count = (uint64_t)amount;
    if (bank->calls >= bank->limits.calls) return RH_CALLS;
    if (count > bank->limits.write_bytes - bank->bytes) return RH_BYTES;
    end = count ? (uint64_t)offset + count : 0;
    if (file == RH_DATABASE && end > bank->limits.database_end) return RH_DATABASE_END;
    if (file == RH_JOURNAL && end > bank->limits.journal_end) return RH_JOURNAL_END;
    if (file == RH_DATABASE && count) {
        first = (uint64_t)offset / bank->limits.page_bytes;
        last = (end - 1) / bank->limits.page_bytes;
        if (last - first + 1 > bank->limits.database_pages) return RH_PAGES;
        for (page = first; page <= last; ++page) {
            uint64_t key = page + 1;
            size_t probes = 0, slot = rh_hash(key, bank->slots - 1);
            while (bank->pages[slot] && bank->pages[slot] != key && probes < bank->slots) {
                slot = (slot + 1) & (bank->slots - 1);
                ++probes;
            }
            if (probes == bank->slots) {
                while (staged) bank->pages[bank->staged_slots[--staged]] = 0;
                return RH_TRACKING;
            }
            if (!bank->pages[slot]) {
                if (bank->page_count + (uint64_t)staged >= bank->limits.database_pages) {
                    while (staged) bank->pages[bank->staged_slots[--staged]] = 0;
                    return RH_PAGES;
                }
                bank->pages[slot] = key;
                bank->staged_slots[staged++] = slot;
            }
        }
    }
    bank->page_count += (uint64_t)staged;
    bank->bytes += count;
    ++bank->calls;
    if (file == RH_DATABASE) bank->database_bytes += count;
    else {
        bank->journal_bytes += count;
        if (end > bank->journal_extent) bank->journal_extent = end;
    }
    return RH_ALLOWED;
}

static enum rh_refusal rh_record(
    rh_quota *quota, enum rh_refusal result, enum rh_file file, int64_t amount,
    int64_t offset, int was_recovery
) {
    if (result != RH_ALLOWED) {
        /* SQLite may restore originals inside this same failing native call. */
        quota->recovering = 1;
        rh_increment(&quota->refusals, quota);
        if (!quota->first_recorded) {
            quota->first_recorded = 1;
            quota->first_reason = result;
            quota->first_file = file;
            quota->first_amount = amount;
            quota->first_offset = offset;
            quota->first_recovery = was_recovery;
        }
    }
    return result;
}

enum rh_refusal rh_admit(
    rh_quota *quota, enum rh_file file, int64_t amount, int64_t offset
) {
    int was_recovery = quota->recovering;
    rh_bank *bank = was_recovery ? &quota->recovery : &quota->forward;
    return rh_record(quota, rh_charge(bank, file, amount, offset),
                     file, amount, offset, was_recovery);
}

enum rh_refusal rh_read_admit(
    rh_read_quota *reads, rh_quota *quota, enum rh_file file, int64_t amount, int64_t offset
) {
    int was_recovery = quota->recovering;
    rh_read_bank *bank = was_recovery ? &reads->recovery : &reads->forward;
    enum rh_refusal result = RH_ALLOWED;
    if (file != RH_DATABASE && file != RH_JOURNAL) result = RH_FILE;
    else if (amount < 0 || offset < 0 || amount > INT64_MAX - offset) result = RH_READ_RANGE;
    else if (bank->calls >= bank->call_limit) result = RH_READ_CALLS;
    else if ((uint64_t)amount > bank->byte_limit - bank->bytes) result = RH_READ_BYTES;
    if (result == RH_ALLOWED) {
        bank->bytes += (uint64_t)amount;
        ++bank->calls;
    }
    return rh_record(quota, result, file, amount, offset, was_recovery);
}

enum rh_refusal rh_resize(rh_quota *quota, enum rh_file file, int64_t size) {
    int was_recovery = quota->recovering;
    rh_bank *bank = was_recovery ? &quota->recovery : &quota->forward;
    enum rh_refusal result = RH_ALLOWED;
    if (file != RH_DATABASE && file != RH_JOURNAL) result = RH_FILE;
    else if (size < 0) result = RH_RESIZE_RANGE;
    /* Write and resize streams have separate counters under the supplied call ceiling. */
    else if (bank->resize_calls >= bank->limits.calls) result = RH_RESIZE_CALLS;
    else if (file == RH_DATABASE && (uint64_t)size > bank->limits.database_end)
        result = RH_DATABASE_END;
    else if (file == RH_JOURNAL && (uint64_t)size > bank->limits.journal_end)
        result = RH_JOURNAL_END;
    if (result == RH_ALLOWED) {
        ++bank->resize_calls;
        if (file == RH_DATABASE && (uint64_t)size > bank->database_resize_extent)
            bank->database_resize_extent = (uint64_t)size;
        if (file == RH_JOURNAL && (uint64_t)size > bank->journal_resize_extent)
            bank->journal_resize_extent = (uint64_t)size;
    }
    return rh_record(quota, result, file, size, 0, was_recovery);
}

void rh_forbid_control(rh_quota *quota, enum rh_file file) {
    (void)rh_record(quota, RH_CONTROL, file, 0, 0, quota->recovering);
}

void rh_failed(rh_quota *quota) {
    quota->recovering = 1;
    rh_increment(&quota->native_failures, quota);
}

const char *rh_reason(enum rh_refusal reason) {
    switch (reason) {
        case RH_FILE: return "unadmitted file";
        case RH_RANGE: return "invalid write range";
        case RH_CALLS: return "write call grant";
        case RH_BYTES: return "write byte grant";
        case RH_DATABASE_END: return "database extent grant";
        case RH_JOURNAL_END: return "journal extent grant";
        case RH_PAGES: return "database page grant";
        case RH_TRACKING: return "tracking capacity";
        case RH_READ_RANGE: return "invalid read range";
        case RH_READ_CALLS: return "read call grant";
        case RH_READ_BYTES: return "read byte grant";
        case RH_RESIZE_RANGE: return "invalid resize extent";
        case RH_RESIZE_CALLS: return "resize call grant";
        case RH_CONTROL: return "disallowed file control";
        default: return "allowed";
    }
}

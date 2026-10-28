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


static int callback_fits(rh_callback_quota *calls, int recovery, uint64_t amount) {
    rh_callback_bank *bank = recovery ? &calls->recovery : &calls->forward;
    uint64_t available = bank->limit - bank->charged;
    if (recovery) {
        if (calls->forward_files > available) return 0;
        available -= calls->forward_files;
    }
    return amount <= available;
}
enum rh_refusal rh_callback_admit(rh_callback_quota *calls, rh_quota *quota, int can_error) {
    int recovery = quota->recovering;
    rh_callback_bank *bank = recovery ? &calls->recovery : &calls->forward;
    if (calls->pending_error) {
        if (can_error) calls->pending_error = 0;
        return RH_CALLBACK_CALLS;
    }
    if (!callback_fits(calls, recovery, 1)) {
        /* Void/value callbacks cannot return an SQLite error. Do no native work,
         * and require an explicit error before any later fallible delegation.
         */
        calls->pending_error = !can_error;
        return rh_record(quota, RH_CALLBACK_CALLS, RH_OTHER, 1, 0, recovery);
    }
    ++bank->charged;
    return RH_ALLOWED;
}
enum rh_refusal rh_callback_sleep(rh_callback_quota *calls, rh_quota *quota, int64_t requested) {
    int recovery = quota->recovering;
    rh_callback_bank *bank = recovery ? &calls->recovery : &calls->forward;
    enum rh_refusal result = rh_callback_admit(calls, quota, 0);
    uint64_t rounded;
    if (result != RH_ALLOWED) return result;
    /* The selected Win32 VFS computes (microseconds+999)/1000 in signed int.
     * Admit its complete nominal millisecond delay without overflowing that ABI.
     * Scheduler delay and the native file methods' internal retries are separate.
     */
    if (requested < 0 || requested > INT_MAX - 999) {
        calls->pending_error = 1;
        return rh_record(quota, RH_SLEEP_RANGE, RH_OTHER, requested, 0, recovery);
    }
    rounded = ((uint64_t)requested + 999) / 1000 * 1000;
    if (rounded > bank->sleep_limit - bank->sleep_charged) {
        calls->pending_error = 1;
        return rh_record(quota, RH_SLEEP_GRANT, RH_OTHER, (int64_t)rounded, 0, recovery);
    }
    bank->sleep_charged += rounded;
    return RH_ALLOWED;
}
int rh_callback_open(rh_callback_quota *calls, rh_quota *quota) {
    int recovery = quota->recovering;
    rh_callback_bank *bank = recovery ? &calls->recovery : &calls->forward;
    enum rh_refusal reason = RH_ALLOWED;
    if (calls->pending_error) {
        calls->pending_error = 0;
        return -1;
    }
    if (!recovery && !callback_fits(calls, 1, 1)) reason = RH_CALLBACK_CLOSE;
    else if (!callback_fits(calls, recovery, 2)) reason = RH_CALLBACK_CALLS;
    if (reason != RH_ALLOWED) {
        (void)rh_record(quota, reason, RH_OTHER, 2, 0, recovery);
        return -1;
    }
    bank->charged += 2;
    if (recovery) ++calls->recovery_files;
    else ++calls->forward_files;
    return !recovery;
}
int rh_callback_abandon(rh_callback_quota *calls, int forward) {
    uint64_t *files = forward ? &calls->forward_files : &calls->recovery_files;
    if (!*files) return 0;
    --*files;
    return 1;
}
int rh_callback_close(rh_callback_quota *calls, rh_quota *quota, int forward) {
    if (forward) {
        if (!calls->forward_files) return 0;
        /* This slot was reserved before the forward native open. */
        if (quota->recovering) ++calls->recovery.charged;
    } else if (!calls->recovery_files) return 0;
    return rh_callback_abandon(calls, forward);
}
int rh_callback_prepare(rh_callback_quota *calls, rh_quota *quota, uint64_t files) {
    if (calls->forward_files || calls->recovery_files || calls->pending_error ||
        (files && quota->recovering) || !callback_fits(calls, 0, files) ||
        !callback_fits(calls, 1, files)) return 0;
    calls->forward.charged += files;
    calls->forward_files = files;
    return 1;
}
void rh_callback_detach(rh_callback_quota *calls) {
    /* Transfer live close reservations only after the next bank admits them. */
    calls->forward_files = 0;
    calls->recovery_files = 0;
}

int rh_retry_bootstrap_cost(int64_t count, int64_t delay, rh_retry_cost *cost) {
    uint64_t retries, product, milliseconds;
    if (count <= 0 || count >= INT_MAX || delay <= 0 || delay > INT_MAX) return 0;
    retries = (uint64_t)count;
    product = retries * (retries + 1);
    /* winLogIoerr multiplies before dividing by two, in signed native int. */
    if ((uint64_t)delay > (uint64_t)INT_MAX / product) return 0;
    milliseconds = (uint64_t)delay * product / 2;
    /* Both open modes run winIsDir; each failed RW open also runs winAccess.
     * Identity adds three metadata calls and close prepays three attempts.
     */
    cost->filesystem_calls = (retries + 1) * (retries + 5) + 6;
    cost->sleep_microseconds = (milliseconds * (retries + 5) + 200) * 1000;
    return 1;
}
int rh_native_filesystem_cost(
    enum rh_native_operation operation, int64_t count, int64_t delay,
    int64_t amount, rh_retry_cost *cost
) {
    rh_retry_cost bootstrap;
    uint64_t retries, wait, open;
    if (!rh_retry_bootstrap_cost(count, delay, &bootstrap)) return 0;
    if (operation == RH_OP_READ || operation == RH_OP_WRITE) {
        if (amount <= 0 || amount > INT_MAX) return 0;
    } else if (amount != 0) return 0;
    retries = (uint64_t)count;
    wait = (uint64_t)delay * retries * (retries + 1) / 2 * 1000;
    open = (retries + 1) * (retries + 5);
    *cost = (rh_retry_cost){0, 0};
    switch (operation) {
        case RH_OP_READ: cost->filesystem_calls = retries + 2; cost->sleep_microseconds = wait; break;
        case RH_OP_WRITE: cost->filesystem_calls = (uint64_t)amount + retries + 1; cost->sleep_microseconds = wait; break;
        case RH_OP_TRUNCATE: cost->filesystem_calls = 2; break;
        case RH_OP_SYNC: case RH_OP_SIZE: case RH_OP_PIN_CLOSE: cost->filesystem_calls = 1; break;
        case RH_OP_LOCK: cost->filesystem_calls = 6; cost->sleep_microseconds = 2000; break;
        case RH_OP_UNLOCK: cost->filesystem_calls = 4; break;
        case RH_OP_RESERVED: case RH_OP_FULLPATH: cost->filesystem_calls = 2; break;
        case RH_OP_CONTROL: cost->filesystem_calls = 3; break;
        case RH_OP_SCALAR: break;
        /* Database identity: two information calls plus final-path lookup.
         * Pinned J adds its pre/post current checks; new J adds absence,
         * metadata pin creation, pin identity and final current check.
         */
        case RH_OP_OPEN_DATABASE: cost->filesystem_calls = open + 3; cost->sleep_microseconds = wait * (retries + 5); break;
        case RH_OP_OPEN_JOURNAL_PINNED: cost->filesystem_calls = open + 9; cost->sleep_microseconds = wait * (retries + 5); break;
        case RH_OP_OPEN_JOURNAL_NEW: cost->filesystem_calls = open + 11; cost->sleep_microseconds = wait * (retries + 5); break;
        case RH_OP_DELETE_JOURNAL: cost->filesystem_calls = 2 * (retries + 1) + 3; cost->sleep_microseconds = wait; break;
        case RH_OP_ACCESS: cost->filesystem_calls = retries + 1; cost->sleep_microseconds = wait; break;
        /* Duplicate + regular metadata + fullpath + current path + J check.
         * Explicit original J needs pin+three metadata calls; absence needs one.
         */
        case RH_OP_BIND_DATABASE: cost->filesystem_calls = 7; break;
        case RH_OP_BIND_JOURNAL: cost->filesystem_calls = 10; break;
        case RH_OP_CLOSE: cost->filesystem_calls = 3; cost->sleep_microseconds = 200000; break;
        default: return 0;
    }
    return 1;
}

static enum rh_refusal native_work_fits(
    const rh_native_work_bank *bank, rh_retry_cost work, rh_retry_cost close
) {
    uint64_t calls, sleep;
    if (bank->charged.filesystem_calls > bank->limits.filesystem_calls ||
        bank->charged.sleep_microseconds > bank->limits.sleep_microseconds)
        return RH_NATIVE_CLOSE;
    calls = bank->limits.filesystem_calls - bank->charged.filesystem_calls;
    sleep = bank->limits.sleep_microseconds - bank->charged.sleep_microseconds;
    /* Subtract before adding: caller cost sums need not fit uint64_t. */
    if (work.filesystem_calls > calls || close.filesystem_calls > calls - work.filesystem_calls)
        return RH_NATIVE_CALLS;
    if (work.sleep_microseconds > sleep || close.sleep_microseconds > sleep - work.sleep_microseconds)
        return RH_NATIVE_SLEEP;
    return RH_ALLOWED;
}
static void native_work_charge(rh_native_work_bank *bank, rh_retry_cost work, rh_retry_cost close) {
    bank->charged.filesystem_calls += work.filesystem_calls + close.filesystem_calls;
    bank->charged.sleep_microseconds += work.sleep_microseconds + close.sleep_microseconds;
    bank->close_prepaid.filesystem_calls += close.filesystem_calls;
    bank->close_prepaid.sleep_microseconds += close.sleep_microseconds;
}
enum rh_refusal rh_native_work_admit(
    rh_native_work_quota *work, rh_quota *quota, rh_retry_cost cost
) {
    rh_native_work_bank *bank = quota->recovering ? &work->recovery : &work->forward;
    rh_retry_cost none = {0, 0};
    int recovery = quota->recovering;
    enum rh_refusal reason = native_work_fits(bank, cost, none);
    if (reason == RH_ALLOWED) native_work_charge(bank, cost, none);
    return rh_record(quota, reason, RH_OTHER, 0, 0, recovery);
}
enum rh_refusal rh_native_work_acquire(
    rh_native_work_quota *work, rh_quota *quota, rh_retry_cost acquisition,
    rh_retry_cost close, rh_native_close_credit *credit
) {
    rh_native_work_bank *bank = quota->recovering ? &work->recovery : &work->forward;
    rh_retry_cost none = {0, 0};
    int recovery = quota->recovering;
    enum rh_refusal reason;
    /* Reusing a live credit would silently discard a close obligation. */
    if (credit->pending || !close.filesystem_calls)
        return rh_record(quota, RH_NATIVE_CLOSE, RH_OTHER, 0, 0, recovery);
    reason = native_work_fits(bank, acquisition, close);
    if (reason == RH_ALLOWED && !recovery)
        reason = native_work_fits(&work->recovery, none, close);
    if (reason != RH_ALLOWED)
        return rh_record(quota, reason, RH_OTHER, 0, 0, recovery);
    native_work_charge(bank, acquisition, close);
    if (!recovery) native_work_charge(&work->recovery, none, close);
    credit->cost = close;
    credit->pending = 1;
    return RH_ALLOWED;
}
int rh_native_close_consume(rh_native_close_credit *credit) {
    if (!credit->pending) return 0;
    credit->pending = 0;
    /* No charge/refund here: the native file or pin owns complete prepayment. */
    return 1;
}

void rh_forbid_native_range(rh_quota *quota) {
    (void)rh_record(quota, RH_NATIVE_RANGE, RH_OTHER, 0, 0, quota->recovering);
}
void rh_forbid_retry(rh_quota *quota) {
    (void)rh_record(quota, RH_RETRY, RH_DATABASE, 0, 0, quota->recovering);
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
        case RH_CALLBACK_CALLS: return "VFS callback grant";
        case RH_CALLBACK_CLOSE: return "recovery file-close grant";
        case RH_SLEEP_RANGE: return "invalid VFS sleep range";
        case RH_SLEEP_GRANT: return "VFS sleep grant";
        case RH_RETRY: return "native retry admission";
        case RH_NATIVE_CALLS: return "native filesystem attempt grant";
        case RH_NATIVE_SLEEP: return "native internal sleep grant";
        case RH_NATIVE_CLOSE: return "native close credit";
        case RH_NATIVE_RANGE: return "invalid native filesystem request";

        default: return "allowed";
    }
}

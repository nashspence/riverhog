#define PY_SSIZE_T_CLEAN
#ifdef _MSC_VER
/* Keep Python header warnings separate from the extension warning policy. */
#pragma warning(push)
#pragma warning(disable: 4100 4115)
#endif
#include <Python.h>
#ifdef _MSC_VER
#pragma warning(pop)
#endif
#include <limits.h>
#include <stdint.h>
#include <string.h>
#include "write_quota.h"
#include "write_vfs.h"
#define RH_STR_I(value) #value
#define RH_STR(value) RH_STR_I(value)
#ifdef _MSC_FULL_VER
#define RH_COMPILER "MSVC " RH_STR(_MSC_FULL_VER)
#else
#define RH_COMPILER __VERSION__
#endif

typedef struct {
    PyObject_HEAD
    rh_quota quota;
    size_t tracking_bytes;
    int initialized;
    Py_ssize_t native_users;
    unsigned long native_thread;
} WriteQuota;

static int positive_integer(PyObject *object, uint64_t *value) {
    long long parsed;
    if (!PyLong_CheckExact(object)) {
        PyErr_SetString(PyExc_TypeError, "resource limits require exact integers");
        return -1;
    }
    parsed = PyLong_AsLongLong(object);
    if (PyErr_Occurred()) return -1;
    if (parsed <= 0) {
        PyErr_SetString(PyExc_ValueError, "resource limits must be positive signed 64-bit values");
        return -1;
    }
    *value = (uint64_t)parsed;
    return 0;
}

static int retry_pair(PyObject *values, uint64_t *first, uint64_t *second, int profile) {
    Py_ssize_t index;
    uint64_t *fields[2] = {first, second};
    if (!PyTuple_CheckExact(values) || PyTuple_GET_SIZE(values) != 2) {
        PyErr_SetString(PyExc_TypeError, "native profiles and grants require exact two-integer tuples");
        return 0;
    }
    for (index = 0; index < 2; ++index) {
        PyObject *value = PyTuple_GET_ITEM(values, index);
        long long parsed;
        if (!PyLong_CheckExact(value)) {
            PyErr_SetString(PyExc_TypeError, "native profiles and grants require exact signed 64-bit integers");
            return 0;
        }
        parsed = PyLong_AsLongLong(value);
        if (PyErr_Occurred()) return 0;
        if (parsed < 0 || (profile && parsed == 0)) {
            PyErr_SetString(PyExc_ValueError, "native profiles must be positive and resource grants nonnegative");
            return 0;
        }
        *fields[index] = (uint64_t)parsed;
    }
    if (profile) {
        rh_retry_cost ignored;
        if (!rh_retry_bootstrap_cost((int64_t)*first, (int64_t)*second, &ignored)) {
            PyErr_SetString(PyExc_ValueError, "retry profile exceeds native signed-int delay or logging arithmetic");
            return 0;
        }
    }
    return 1;
}
static PyObject *retry_bootstrap_cost(PyObject *module, PyObject *args) {
    PyObject *values;
    uint64_t count, delay;
    rh_retry_cost cost;
    (void)module;
    if (!PyArg_ParseTuple(args, "O", &values)) return NULL;
    if (!retry_pair(values, &count, &delay, 1)) return NULL;
    if (!rh_retry_bootstrap_cost((int64_t)count, (int64_t)delay, &cost)) {
        PyErr_SetString(PyExc_ValueError, "retry bootstrap recipe is invalid");
        return NULL;
    }
    return Py_BuildValue("(KK)", (unsigned long long)cost.filesystem_calls,
                                (unsigned long long)cost.sleep_microseconds);
}

static PyObject *native_filesystem_cost(PyObject *module, PyObject *args) {
    PyObject *method, *profile, *value = NULL;
    const char *name;
    Py_ssize_t length, index;
    uint64_t count, delay;
    int64_t amount = 0;
    rh_retry_cost cost;
    static const struct { const char *name; enum rh_native_operation operation; } operations[] = {
        {"read", RH_OP_READ}, {"write", RH_OP_WRITE},
        {"truncate", RH_OP_TRUNCATE}, {"sync", RH_OP_SYNC}, {"size", RH_OP_SIZE},
        {"lock", RH_OP_LOCK}, {"unlock", RH_OP_UNLOCK}, {"reserved", RH_OP_RESERVED},
        {"control", RH_OP_CONTROL}, {"scalar", RH_OP_SCALAR},
        {"open_database", RH_OP_OPEN_DATABASE},
        {"open_journal_pinned", RH_OP_OPEN_JOURNAL_PINNED},
        {"open_journal_new", RH_OP_OPEN_JOURNAL_NEW},
        {"delete_journal", RH_OP_DELETE_JOURNAL}, {"access", RH_OP_ACCESS},
        {"fullpath", RH_OP_FULLPATH}, {"bind_database", RH_OP_BIND_DATABASE},
        {"bind_journal", RH_OP_BIND_JOURNAL}, {"close", RH_OP_CLOSE}, {"pin_close", RH_OP_PIN_CLOSE}
    };
    (void)module;
    if (!PyArg_ParseTuple(args, "OO|O", &method, &profile, &value)) return NULL;
    if (!PyUnicode_CheckExact(method)) {
        PyErr_SetString(PyExc_TypeError, "native filesystem recipe requires an exact method name");
        return NULL;
    }
    name = PyUnicode_AsUTF8AndSize(method, &length);
    if (!name) return NULL;
    if (!retry_pair(profile, &count, &delay, 1)) return NULL;
    if (value) {
        if (!PyLong_CheckExact(value)) {
            PyErr_SetString(PyExc_TypeError, "native filesystem byte count requires an exact signed 64-bit integer");
            return NULL;
        }
        amount = (int64_t)PyLong_AsLongLong(value);
        if (PyErr_Occurred()) return NULL;
    }
    for (index = 0; index < (Py_ssize_t)(sizeof(operations) / sizeof(operations[0])); ++index) {
        const char *candidate = operations[index].name;
        if ((size_t)length != strlen(candidate) || memcmp(name, candidate, (size_t)length)) continue;
        if (!rh_native_filesystem_cost(operations[index].operation,
                                      (int64_t)count, (int64_t)delay, amount, &cost)) {
            PyErr_SetString(PyExc_ValueError, "native filesystem recipe has an invalid byte count or profile");
            return NULL;
        }
        return Py_BuildValue("(KK)", (unsigned long long)cost.filesystem_calls,
                                    (unsigned long long)cost.sleep_microseconds);
    }
    PyErr_SetString(PyExc_ValueError, "unknown native filesystem recipe");
    return NULL;
}

static void free_bank(rh_bank *bank) {
    PyMem_Free(bank->pages);
    PyMem_Free(bank->staged_slots);
    bank->pages = NULL;
    bank->staged_slots = NULL;
}

static int initialize_bank(rh_bank *bank, PyObject *values, size_t *tracking_bytes) {
    uint64_t parsed[6];
    size_t slots = 2, bytes;
    Py_ssize_t index;
    if (!PyTuple_CheckExact(values) || PyTuple_GET_SIZE(values) != 6) {
        PyErr_SetString(PyExc_TypeError, "write limits require a six-integer tuple");
        return -1;
    }
    for (index = 0; index < 6; ++index) {
        if (positive_integer(PyTuple_GET_ITEM(values, index), &parsed[index]) < 0) return -1;
    }
    if (parsed[0] & (parsed[0] - 1)) {
        PyErr_SetString(PyExc_ValueError, "page size must be a power of two");
        return -1;
    }
    if (parsed[1] > (uint64_t)(PY_SSIZE_T_MAX / (4 * sizeof(uint64_t)))) {
        PyErr_SetString(PyExc_OverflowError, "write tracking allocation exceeds addressable capacity");
        return -1;
    }
    while (slots / 2 < (size_t)parsed[1]) {
        if (slots > (size_t)PY_SSIZE_T_MAX / (2 * sizeof(uint64_t))) {
            PyErr_SetString(PyExc_OverflowError, "write tracking table exceeds addressable capacity");
            return -1;
        }
        slots *= 2;
    }
    bytes = slots * sizeof(uint64_t) + (size_t)parsed[1] * sizeof(size_t);
    if (bytes > (size_t)PY_SSIZE_T_MAX - *tracking_bytes) {
        PyErr_SetString(PyExc_OverflowError, "combined tracking allocation exceeds addressable capacity");
        return -1;
    }
    bank->limits.page_bytes = parsed[0];
    bank->limits.database_pages = parsed[1];
    bank->limits.database_end = parsed[2];
    bank->limits.journal_end = parsed[3];
    bank->limits.write_bytes = parsed[4];
    bank->limits.calls = parsed[5];
    bank->slots = slots;
    bank->pages = PyMem_Calloc(slots, sizeof(uint64_t));
    bank->staged_slots = PyMem_Calloc((size_t)parsed[1], sizeof(size_t));
    if (!bank->pages || !bank->staged_slots) {
        free_bank(bank);
        PyErr_NoMemory();
        return -1;
    }
    *tracking_bytes += bytes;
    return 0;
}

static int quota_init(PyObject *object, PyObject *args, PyObject *kwargs) {
    WriteQuota *self = (WriteQuota *)object;
    PyObject *forward, *recovery;
    static char *keywords[] = {"forward", "recovery", NULL};
    if (self->initialized) {
        PyErr_SetString(PyExc_RuntimeError, "write quota cannot be reinitialized");
        return -1;
    }
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO", keywords, &forward, &recovery)) return -1;
    /* Seal before allocating either bank; failed allocation cannot reset admission. */
    self->initialized = 1;
    if (initialize_bank(&self->quota.forward, forward, &self->tracking_bytes) < 0) return -1;
    if (initialize_bank(&self->quota.recovery, recovery, &self->tracking_bytes) < 0) {
        free_bank(&self->quota.forward);
        return -1;
    }
    return 0;
}

static void quota_dealloc(PyObject *object) {
    WriteQuota *self = (WriteQuota *)object;
    free_bank(&self->quota.forward);
    free_bank(&self->quota.recovery);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static int ready(WriteQuota *self) {
    if (self->native_users && self->native_thread != PyThread_get_thread_ident()) {
        PyErr_SetString(PyExc_RuntimeError, "attached write quota requires its owning thread");
        return 0;
    }
    if (!self->quota.forward.pages || !self->quota.recovery.pages) {
        PyErr_SetString(PyExc_RuntimeError, "write quota is not initialized");
        return 0;
    }
    return 1;
}

static PyObject *quota_admit(PyObject *object, PyObject *args) {
    WriteQuota *self = (WriteQuota *)object;
    PyObject *kind, *amount, *offset;
    long long length, position;
    enum rh_file file = RH_OTHER;
    if (!ready(self)) return NULL;
    if (!PyArg_ParseTuple(args, "OOO", &kind, &amount, &offset)) return NULL;
    if (!PyUnicode_CheckExact(kind) || !PyLong_CheckExact(amount) || !PyLong_CheckExact(offset)) {
        PyErr_SetString(PyExc_TypeError, "write admission requires a file kind and exact integer range");
        return NULL;
    }
    length = PyLong_AsLongLong(amount);
    if (PyErr_Occurred()) return NULL;
    position = PyLong_AsLongLong(offset);
    if (PyErr_Occurred()) return NULL;
    if (PyUnicode_CompareWithASCIIString(kind, "database") == 0) file = RH_DATABASE;
    else if (PyUnicode_CompareWithASCIIString(kind, "journal") == 0) file = RH_JOURNAL;
    return PyBool_FromLong(rh_admit(&self->quota, file, (int64_t)length, (int64_t)position) == RH_ALLOWED);
}

static PyObject *quota_resize(PyObject *object, PyObject *args) {
    WriteQuota *self = (WriteQuota *)object;
    PyObject *kind, *size;
    long long extent;
    enum rh_file file = RH_OTHER;
    if (!ready(self)) return NULL;
    if (!PyArg_ParseTuple(args, "OO", &kind, &size)) return NULL;
    if (!PyUnicode_CheckExact(kind) || !PyLong_CheckExact(size)) {
        PyErr_SetString(PyExc_TypeError, "resize admission requires a file kind and exact integer extent");
        return NULL;
    }
    extent = PyLong_AsLongLong(size);
    if (PyErr_Occurred()) return NULL;
    if (PyUnicode_CompareWithASCIIString(kind, "database") == 0) file = RH_DATABASE;
    else if (PyUnicode_CompareWithASCIIString(kind, "journal") == 0) file = RH_JOURNAL;
    return PyBool_FromLong(rh_resize(&self->quota, file, (int64_t)extent) == RH_ALLOWED);
}
static PyObject *quota_resize_report(PyObject *object, PyObject *unused) {
    WriteQuota *self = (WriteQuota *)object;
    rh_bank *f = &self->quota.forward, *r = &self->quota.recovery;
    (void)unused;
    if (!ready(self)) return NULL;
    return Py_BuildValue("{s:{s:K,s:K,s:K},s:{s:K,s:K,s:K}}",
        "forward", "resize_calls", (unsigned long long)f->resize_calls,
        "database_resize_extent", (unsigned long long)f->database_resize_extent,
        "journal_resize_extent", (unsigned long long)f->journal_resize_extent,
        "recovery", "resize_calls", (unsigned long long)r->resize_calls,
        "database_resize_extent", (unsigned long long)r->database_resize_extent,
        "journal_resize_extent", (unsigned long long)r->journal_resize_extent);
}

static PyObject *quota_failed(PyObject *object, PyObject *unused) {
    WriteQuota *self = (WriteQuota *)object;
    (void)unused;
    if (!ready(self)) return NULL;
    rh_failed(&self->quota);
    Py_RETURN_NONE;
}

static PyObject *bank_report(rh_bank *bank) {
    return Py_BuildValue(
        "{s:K,s:K,s:K,s:K,s:K,s:K}",
        "database_pages_attempted", (unsigned long long)bank->page_count,
        "database_write_bytes", (unsigned long long)bank->database_bytes,
        "journal_write_bytes", (unsigned long long)bank->journal_bytes,
        "journal_write_extent_bytes", (unsigned long long)bank->journal_extent,
        "write_bytes", (unsigned long long)bank->bytes,
        "write_calls", (unsigned long long)bank->calls
    );
}

static PyObject *quota_report(PyObject *object, PyObject *unused) {
    WriteQuota *self = (WriteQuota *)object;
    PyObject *forward, *recovery, *first, *result;
    (void)unused;
    if (!ready(self)) return NULL;
    forward = bank_report(&self->quota.forward);
    if (!forward) return NULL;
    recovery = bank_report(&self->quota.recovery);
    if (!recovery) { Py_DECREF(forward); return NULL; }
    if (self->quota.first_recorded) {
        const char *file = self->quota.first_file == RH_DATABASE ? "database" :
            self->quota.first_file == RH_JOURNAL ? "journal" : "other";
        first = Py_BuildValue(
            "{s:s,s:L,s:L,s:s,s:s}",
            "file", file,
            "amount", (long long)self->quota.first_amount,
            "offset", (long long)self->quota.first_offset,
            "reason", rh_reason(self->quota.first_reason),
            "bank", self->quota.first_recovery ? "recovery" : "forward"
        );
    } else { first = Py_NewRef(Py_None); }
    if (!first) { Py_DECREF(forward); Py_DECREF(recovery); return NULL; }
    result = Py_BuildValue(
        "{s:O,s:O,s:O,s:K,s:O,s:K,s:O}",
        "forward", forward, "recovery", recovery,
        "recovering", self->quota.recovering ? Py_True : Py_False,
        "refusals", (unsigned long long)self->quota.refusals,
        "first_refusal", first,
        "native_failures", (unsigned long long)self->quota.native_failures,
        "diagnostic_overflow", self->quota.diagnostic_overflow ? Py_True : Py_False
    );
    Py_DECREF(forward); Py_DECREF(recovery); Py_DECREF(first);
    return result;
}

static PyObject *quota_recovering(PyObject *object, void *unused) {
    WriteQuota *self = (WriteQuota *)object;
    (void)unused;
    if (!ready(self)) return NULL;
    return PyBool_FromLong(self->quota.recovering);
}

static PyObject *quota_tracking_bytes(PyObject *object, void *unused) {
    WriteQuota *self = (WriteQuota *)object;
    (void)unused;
    if (!ready(self)) return NULL;
    return PyLong_FromSize_t(self->tracking_bytes);
}

static PyMethodDef quota_methods[] = {
    {"admit", quota_admit, METH_VARARGS, "Charge a complete attempted write before delegation."},
    {"resize", quota_resize, METH_VARARGS, "Charge a complete native resize or size hint before delegation."},
    {"resize_report", quota_resize_report, METH_NOARGS, "Snapshot fixed resize counters outside callbacks."},
    {"failed", quota_failed, METH_NOARGS, "Latch the independent restoration bank."},
    {"report", quota_report, METH_NOARGS, "Snapshot fixed counters outside storage callbacks."},
    {NULL, NULL, 0, NULL}
};
static PyGetSetDef quota_getset[] = {
    {"recovering", quota_recovering, NULL, "Restoration-bank latch.", NULL},
    {"tracking_bytes", quota_tracking_bytes, NULL, "Preallocated page-tracking arrays.", NULL},
    {NULL, NULL, NULL, NULL, NULL}
};
static PyTypeObject quota_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "riverhog_client._prepared_primary_native.WriteQuota",
    .tp_basicsize = (Py_ssize_t)sizeof(WriteQuota),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_init = quota_init,
    .tp_dealloc = quota_dealloc,
    .tp_methods = quota_methods,
    .tp_getset = quota_getset
};

typedef struct {
    PyObject_HEAD
    rh_read_quota reads;
    PyObject *quota;
    int initialized;
} ReadQuota;

static int read_limits(rh_read_bank *bank, PyObject *values) {
    if (!PyTuple_CheckExact(values) || PyTuple_GET_SIZE(values) != 2) {
        PyErr_SetString(PyExc_TypeError, "read limits require a two-integer tuple");
        return 0;
    }
    return positive_integer(PyTuple_GET_ITEM(values, 0), &bank->byte_limit) == 0 &&
        positive_integer(PyTuple_GET_ITEM(values, 1), &bank->call_limit) == 0;
}
static int read_ready(ReadQuota *self) {
    if (!self->quota) {
        PyErr_SetString(PyExc_RuntimeError, "read quota is not initialized");
        return 0;
    }
    return ready((WriteQuota *)self->quota);
}
static int read_init(PyObject *object, PyObject *args, PyObject *kwargs) {
    ReadQuota *self = (ReadQuota *)object;
    WriteQuota *quota;
    PyObject *forward, *recovery;
    static char *keywords[] = {"writes", "forward", "recovery", NULL};
    if (self->initialized) {
        PyErr_SetString(PyExc_RuntimeError, "read quota cannot be reinitialized");
        return -1;
    }
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!OO", keywords,
                                    &quota_type, &quota, &forward, &recovery)) return -1;
    self->initialized = 1;
    if (!ready(quota) || !read_limits(&self->reads.forward, forward) ||
        !read_limits(&self->reads.recovery, recovery)) return -1;
    self->quota = Py_NewRef((PyObject *)quota);
    return 0;
}
static void read_dealloc(PyObject *object) {
    ReadQuota *self = (ReadQuota *)object;
    Py_XDECREF(self->quota);
    Py_TYPE(self)->tp_free(object);
}
static PyObject *read_admit(PyObject *object, PyObject *args) {
    ReadQuota *self = (ReadQuota *)object;
    PyObject *kind, *amount, *offset;
    long long length, position;
    enum rh_file file = RH_OTHER;
    if (!read_ready(self)) return NULL;
    if (!PyArg_ParseTuple(args, "OOO", &kind, &amount, &offset)) return NULL;
    if (!PyUnicode_CheckExact(kind) || !PyLong_CheckExact(amount) || !PyLong_CheckExact(offset)) {
        PyErr_SetString(PyExc_TypeError, "read admission requires a file kind and exact integer range");
        return NULL;
    }
    length = PyLong_AsLongLong(amount);
    if (PyErr_Occurred()) return NULL;
    position = PyLong_AsLongLong(offset);
    if (PyErr_Occurred()) return NULL;
    if (PyUnicode_CompareWithASCIIString(kind, "database") == 0) file = RH_DATABASE;
    else if (PyUnicode_CompareWithASCIIString(kind, "journal") == 0) file = RH_JOURNAL;
    return PyBool_FromLong(rh_read_admit(&self->reads, &((WriteQuota *)self->quota)->quota,
        file, (int64_t)length, (int64_t)position) == RH_ALLOWED);
}
static PyObject *read_report(PyObject *object, PyObject *unused) {
    ReadQuota *self = (ReadQuota *)object;
    (void)unused;
    if (!read_ready(self)) return NULL;
    return Py_BuildValue("{s:{s:K,s:K},s:{s:K,s:K}}",
        "forward", "read_bytes", (unsigned long long)self->reads.forward.bytes,
        "read_calls", (unsigned long long)self->reads.forward.calls,
        "recovery", "read_bytes", (unsigned long long)self->reads.recovery.bytes,
        "read_calls", (unsigned long long)self->reads.recovery.calls);
}
static PyMethodDef read_methods[] = {
    {"admit", read_admit, METH_VARARGS, "Charge a complete attempted read before delegation."},
    {"report", read_report, METH_NOARGS, "Snapshot fixed read counters outside callbacks."},
    {NULL, NULL, 0, NULL}
};
static PyTypeObject read_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "riverhog_client._prepared_primary_native.ReadQuota",
    .tp_basicsize = (Py_ssize_t)sizeof(ReadQuota),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_init = read_init,
    .tp_dealloc = read_dealloc,
    .tp_methods = read_methods
};

typedef struct {
    PyObject_HEAD
    rh_native_work_quota work;
    PyObject *quota;
    int initialized, attached;
} NativeWorkQuota;
static int native_work_ready(NativeWorkQuota *, int);
static PyTypeObject native_work_type;

typedef struct {
    PyObject_HEAD
    rh_callback_quota calls;
    PyObject *quota;
    NativeWorkQuota *work;
    int initialized, attached;
} CallbackQuota;
static int callback_ready(CallbackQuota *self, int mutate) {
    if (!self->quota) {
        PyErr_SetString(PyExc_RuntimeError, "callback quota is not initialized");
        return 0;
    }
    if (!ready((WriteQuota *)self->quota)) return 0;
    if (mutate && self->attached) {
        PyErr_SetString(PyExc_RuntimeError, "attached callback quota is driven only by native files");
        return 0;
    }
    return 1;
}
static int callback_limits(PyObject *values, rh_callback_bank *bank) {
    PyObject *duration;
    long long microseconds;
    if (!PyTuple_CheckExact(values) || PyTuple_GET_SIZE(values) != 2) {
        PyErr_SetString(PyExc_TypeError, "callback limits require a two-integer tuple");
        return -1;
    }
    if (positive_integer(PyTuple_GET_ITEM(values, 0), &bank->limit) < 0) return -1;
    duration = PyTuple_GET_ITEM(values, 1);
    if (!PyLong_CheckExact(duration)) {
        PyErr_SetString(PyExc_TypeError, "sleep grants require exact signed 64-bit integers");
        return -1;
    }
    microseconds = PyLong_AsLongLong(duration);
    if (PyErr_Occurred()) return -1;
    if (microseconds < 0) {
        PyErr_SetString(PyExc_ValueError, "sleep grants must be nonnegative");
        return -1;
    }
    bank->sleep_limit = (uint64_t)microseconds;
    return 0;
}
static int callback_init(PyObject *object, PyObject *args, PyObject *kwargs) {
    CallbackQuota *self = (CallbackQuota *)object;
    WriteQuota *quota;
    PyObject *forward, *recovery;
    NativeWorkQuota *work;
    static char *keywords[] = {"writes", "forward", "recovery", "work", NULL};
    if (self->initialized) {
        PyErr_SetString(PyExc_RuntimeError, "callback quota cannot be reinitialized");
        return -1;
    }
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!OOO!", keywords,
                                    &quota_type, &quota, &forward, &recovery,
                                    &native_work_type, &work)) return -1;
    self->initialized = 1;
    if (!native_work_ready(work, 1)) return -1;
    if (work->quota != (PyObject *)quota) {
        PyErr_SetString(PyExc_ValueError, "native work and callback grants require the same write owner");
        return -1;
    }
    if (!ready(quota) || callback_limits(forward, &self->calls.forward) < 0 ||
        callback_limits(recovery, &self->calls.recovery) < 0) return -1;
    self->quota = Py_NewRef((PyObject *)quota);
    self->work = (NativeWorkQuota *)Py_NewRef((PyObject *)work);
    return 0;
}
static void callback_dealloc(PyObject *object) {
    CallbackQuota *self = (CallbackQuota *)object;
    Py_XDECREF(self->quota);
    Py_XDECREF(self->work);
    Py_TYPE(self)->tp_free(object);
}
static PyObject *callback_admit(PyObject *object, PyObject *args) {
    CallbackQuota *self = (CallbackQuota *)object;
    PyObject *can_error = Py_True;
    if (!callback_ready(self, 1)) return NULL;
    if (!PyArg_ParseTuple(args, "|O", &can_error)) return NULL;
    if (!PyBool_Check(can_error)) {
        PyErr_SetString(PyExc_TypeError, "callback error capability requires a bool");
        return NULL;
    }
    return PyBool_FromLong(rh_callback_admit(&self->calls, &((WriteQuota *)self->quota)->quota,
                                            can_error == Py_True) == RH_ALLOWED);
}
static PyObject *callback_sleep(PyObject *object, PyObject *value) {
    CallbackQuota *self = (CallbackQuota *)object;
    long long microseconds;
    if (!callback_ready(self, 1)) return NULL;
    if (!PyLong_CheckExact(value)) {
        PyErr_SetString(PyExc_TypeError, "sleep admission requires an exact integer");
        return NULL;
    }
    microseconds = PyLong_AsLongLong(value);
    if (PyErr_Occurred()) return NULL;
    return PyBool_FromLong(rh_callback_sleep(&self->calls, &((WriteQuota *)self->quota)->quota,
                                             (int64_t)microseconds) == RH_ALLOWED);
}
static PyObject *callback_open(PyObject *object, PyObject *unused) {
    CallbackQuota *self = (CallbackQuota *)object;
    int bank;
    (void)unused;
    if (!callback_ready(self, 1)) return NULL;
    bank = rh_callback_open(&self->calls, &((WriteQuota *)self->quota)->quota);
    if (bank < 0) Py_RETURN_NONE;
    return PyBool_FromLong(bank);
}
static PyObject *callback_close_or_abandon(PyObject *object, PyObject *value, int abandon) {
    CallbackQuota *self = (CallbackQuota *)object;
    int result;
    if (!callback_ready(self, 1)) return NULL;
    if (!PyBool_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "file-close bank requires a bool");
        return NULL;
    }
    result = abandon ? rh_callback_abandon(&self->calls, value == Py_True) :
        rh_callback_close(&self->calls, &((WriteQuota *)self->quota)->quota, value == Py_True);
    if (!result) {
        PyErr_SetString(PyExc_ValueError, "no reserved file close exists in this bank");
        return NULL;
    }
    Py_RETURN_NONE;
}
static PyObject *callback_close(PyObject *object, PyObject *value) {
    return callback_close_or_abandon(object, value, 0);
}
static PyObject *callback_abandon(PyObject *object, PyObject *value) {
    return callback_close_or_abandon(object, value, 1);
}
static PyObject *callback_report(PyObject *object, PyObject *unused) {
    CallbackQuota *self = (CallbackQuota *)object;
    (void)unused;
    if (!callback_ready(self, 0)) return NULL;
    return Py_BuildValue("{s:{s:K,s:K},s:{s:K,s:K},s:K,s:K,s:O}",
        "forward", "callback_slots_charged", (unsigned long long)self->calls.forward.charged,
        "sleep_microseconds_charged", (unsigned long long)self->calls.forward.sleep_charged,
        "recovery", "callback_slots_charged", (unsigned long long)self->calls.recovery.charged,
        "sleep_microseconds_charged", (unsigned long long)self->calls.recovery.sleep_charged,
        "reserved_recovery_closes", (unsigned long long)self->calls.forward_files,
        "prepaid_recovery_closes", (unsigned long long)self->calls.recovery_files,
        "pending_error", self->calls.pending_error ? Py_True : Py_False);
}
static PyMethodDef callback_methods[] = {
    {"admit", callback_admit, METH_VARARGS, "Charge a callback outside a registered VFS."},
    {"admit_sleep", callback_sleep, METH_O, "Precharge a complete rounded Win32 VFS sleep outside registration."},
    {"opened", callback_open, METH_NOARGS, "Prepay an open and reserve its close."},
    {"closed", callback_close, METH_O, "Consume an admitted file's prepaid close."},
    {"abandoned", callback_abandon, METH_O, "Release only a failed open's unused close reservation."},
    {"report", callback_report, METH_NOARGS, "Snapshot conservative callback slots and close reserves."},
    {NULL, NULL, 0, NULL}
};
static PyObject *callback_work(PyObject *object, void *unused) {
    CallbackQuota *self = (CallbackQuota *)object;
    (void)unused;
    if (!callback_ready(self, 0)) return NULL;
    return Py_NewRef((PyObject *)self->work);
}
static PyGetSetDef callback_getset[] = {
    {"work", callback_work, NULL, "Native filesystem work bank attached with this callback bundle.", NULL},
    {NULL, NULL, NULL, NULL, NULL}
};
static PyTypeObject callback_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "riverhog_client._prepared_primary_native.CallbackQuota",
    .tp_basicsize = (Py_ssize_t)sizeof(CallbackQuota),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_init = callback_init,
    .tp_dealloc = callback_dealloc,
    .tp_methods = callback_methods,
    .tp_getset = callback_getset
};

typedef struct {
    PyObject_HEAD
    rh_native_close_credit credit;
    int initialized;
    unsigned long thread;
    int64_t interpreter;
} NativeCloseCredit;
static PyTypeObject native_close_type;
static int native_work_ready(NativeWorkQuota *self, int mutate) {
    if (!self->quota) {
        PyErr_SetString(PyExc_RuntimeError, "native work quota is not initialized");
        return 0;
    }
    if (!ready((WriteQuota *)self->quota)) return 0;
    if (mutate && self->attached) {
        PyErr_SetString(PyExc_RuntimeError, "attached native work quota is driven only by native files");
        return 0;
    }
    return 1;
}
static int native_cost(PyObject *values, rh_retry_cost *cost) {
    return retry_pair(values, &cost->filesystem_calls, &cost->sleep_microseconds, 0);
}
static int native_work_init(PyObject *object, PyObject *args, PyObject *kwargs) {
    NativeWorkQuota *self = (NativeWorkQuota *)object;
    WriteQuota *quota;
    PyObject *forward, *recovery;
    static char *keywords[] = {"writes", "forward", "recovery", NULL};
    if (self->initialized) {
        PyErr_SetString(PyExc_RuntimeError, "native work quota cannot be reinitialized");
        return -1;
    }
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!OO", keywords,
                                    &quota_type, &quota, &forward, &recovery)) return -1;
    self->initialized = 1;
    if (!ready(quota) || !native_cost(forward, &self->work.forward.limits) ||
        !native_cost(recovery, &self->work.recovery.limits)) return -1;
    self->quota = Py_NewRef((PyObject *)quota);
    return 0;
}
static void native_work_dealloc(PyObject *object) {
    NativeWorkQuota *self = (NativeWorkQuota *)object;
    Py_XDECREF(self->quota);
    Py_TYPE(self)->tp_free(object);
}
static PyObject *native_work_admit(PyObject *object, PyObject *value) {
    NativeWorkQuota *self = (NativeWorkQuota *)object;
    rh_retry_cost cost;
    if (!native_work_ready(self, 1) || !native_cost(value, &cost)) return NULL;
    return PyBool_FromLong(rh_native_work_admit(&self->work,
        &((WriteQuota *)self->quota)->quota, cost) == RH_ALLOWED);
}
static PyObject *native_work_acquire(PyObject *object, PyObject *args) {
    NativeWorkQuota *self = (NativeWorkQuota *)object;
    PyObject *acquisition, *close;
    NativeCloseCredit *credit;
    rh_retry_cost work_cost, close_cost;
    enum rh_refusal result;
    if (!native_work_ready(self, 1)) return NULL;
    if (!PyArg_ParseTuple(args, "OO", &acquisition, &close) ||
        !native_cost(acquisition, &work_cost) || !native_cost(close, &close_cost)) return NULL;
    if (!close_cost.filesystem_calls) {
        PyErr_SetString(PyExc_ValueError, "native close requires a positive filesystem-attempt cost");
        return NULL;
    }
    /* Allocate the fixed credit before changing either bank; allocation failure
     * cannot leave a newly acquired native resource without its close record.
     */
    credit = PyObject_New(NativeCloseCredit, &native_close_type);
    if (!credit) return NULL;
    credit->initialized = 0;
    credit->credit = (rh_native_close_credit){{0, 0}, 0};
    result = rh_native_work_acquire(&self->work, &((WriteQuota *)self->quota)->quota,
                                    work_cost, close_cost, &credit->credit);
    if (result != RH_ALLOWED) {
        Py_DECREF(credit);
        Py_RETURN_NONE;
    }
    credit->thread = PyThread_get_thread_ident();
    credit->interpreter = PyInterpreterState_GetID(PyInterpreterState_Get());
    credit->initialized = 1;
    return (PyObject *)credit;
}
static PyObject *native_work_bank_report(const rh_native_work_bank *bank) {
    return Py_BuildValue("{s:K,s:K,s:K,s:K}",
        "filesystem_api_attempts_charged", (unsigned long long)bank->charged.filesystem_calls,
        "internal_sleep_microseconds_charged", (unsigned long long)bank->charged.sleep_microseconds,
        "close_filesystem_api_attempts_prepaid", (unsigned long long)bank->close_prepaid.filesystem_calls,
        "close_internal_sleep_microseconds_prepaid", (unsigned long long)bank->close_prepaid.sleep_microseconds);
}
static PyObject *native_work_report(PyObject *object, PyObject *unused) {
    NativeWorkQuota *self = (NativeWorkQuota *)object;
    PyObject *forward, *recovery, *report;
    (void)unused;
    if (!native_work_ready(self, 0)) return NULL;
    forward = native_work_bank_report(&self->work.forward);
    if (!forward) return NULL;
    recovery = native_work_bank_report(&self->work.recovery);
    if (!recovery) { Py_DECREF(forward); return NULL; }
    report = Py_BuildValue("{s:O,s:O}", "forward", forward, "recovery", recovery);
    Py_DECREF(forward); Py_DECREF(recovery);
    return report;
}
static PyMethodDef native_work_methods[] = {
    {"admit", native_work_admit, METH_O, "Precharge a complete filesystem-attempt/nominal-wait cost."},
    {"acquire", native_work_acquire, METH_VARARGS, "Atomically prepay acquisition and independent possible close paths."},
    {"report", native_work_report, METH_NOARGS, "Snapshot immutable attempted work and complete close prepayment."},
    {NULL, NULL, 0, NULL}
};
static PyTypeObject native_work_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "riverhog_client._prepared_primary_native.NativeWorkQuota",
    .tp_basicsize = (Py_ssize_t)sizeof(NativeWorkQuota),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_init = native_work_init,
    .tp_dealloc = native_work_dealloc,
    .tp_methods = native_work_methods
};
static int native_close_ready(NativeCloseCredit *self) {
    if (!self->initialized) {
        PyErr_SetString(PyExc_RuntimeError, "native close credit is not initialized");
        return 0;
    }
    if (self->thread != PyThread_get_thread_ident() ||
        self->interpreter != PyInterpreterState_GetID(PyInterpreterState_Get())) {
        PyErr_SetString(PyExc_RuntimeError, "native close credit requires its owning interpreter and thread");
        return 0;
    }
    return 1;
}
static int native_close_init(PyObject *object, PyObject *args, PyObject *kwargs) {
    (void)object; (void)args; (void)kwargs;
    PyErr_SetString(PyExc_TypeError, "native close credits require successful native work acquisition");
    return -1;
}
static PyObject *native_close_consume(PyObject *object, PyObject *unused) {
    NativeCloseCredit *self = (NativeCloseCredit *)object;
    (void)unused;
    if (!native_close_ready(self)) return NULL;
    if (!rh_native_close_consume(&self->credit)) {
        PyErr_SetString(PyExc_ValueError, "native close credit was already consumed");
        return NULL;
    }
    Py_RETURN_NONE;
}
static PyObject *native_close_report(PyObject *object, PyObject *unused) {
    NativeCloseCredit *self = (NativeCloseCredit *)object;
    (void)unused;
    if (!native_close_ready(self)) return NULL;
    return Py_BuildValue("{s:K,s:K,s:O}",
        "filesystem_api_attempts_prepaid", (unsigned long long)self->credit.cost.filesystem_calls,
        "internal_sleep_microseconds_prepaid", (unsigned long long)self->credit.cost.sleep_microseconds,
        "pending", self->credit.pending ? Py_True : Py_False);
}
static PyMethodDef native_close_methods[] = {
    {"consume", native_close_consume, METH_NOARGS, "Use one fully prepaid close; no budget access, charge or refund."},
    {"report", native_close_report, METH_NOARGS, "Observe fixed prepayment without a reference to its original bank."},
    {NULL, NULL, 0, NULL}
};
static PyTypeObject native_close_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "riverhog_client._prepared_primary_native.NativeCloseCredit",
    .tp_basicsize = (Py_ssize_t)sizeof(NativeCloseCredit),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_init = native_close_init,
    .tp_methods = native_close_methods
};

typedef struct {
    PyObject_HEAD
    rh_sqlite_heap *heap;
    int64_t interpreter;
    int initialized;
} SqliteProcessBudget;
static PyObject *process_budget;
static int heap_ready(SqliteProcessBudget *self) {
    if (!self->heap) {
        PyErr_SetString(PyExc_RuntimeError, "SQLite process budget is not initialized");
        return 0;
    }
    if (self->interpreter != PyInterpreterState_GetID(PyInterpreterState_Get())) {
        PyErr_SetString(PyExc_RuntimeError, "SQLite process budget belongs to another interpreter");
        return 0;
    }
    return 1;
}
static int heap_init(PyObject *object, PyObject *args, PyObject *kwargs) {
    SqliteProcessBudget *self = (SqliteProcessBudget *)object;
    const char *source, *error;
    PyObject *value;
    uint64_t limit;
    static char *keywords[] = {"expected_source_id", "heap_bytes", NULL};
    if (self->initialized) {
        PyErr_SetString(PyExc_RuntimeError, "SQLite process budget cannot be reinitialized");
        return -1;
    }
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sO", keywords, &source, &value)) return -1;
    self->initialized = 1;
    if (positive_integer(value, &limit) < 0) return -1;
    self->heap = rh_sqlite_heap_create(source, (int64_t)limit, &error);
    if (!self->heap) {
        PyErr_SetString(PyExc_RuntimeError, error);
        return -1;
    }
    self->interpreter = PyInterpreterState_GetID(PyInterpreterState_Get());
    process_budget = Py_NewRef(object);
    return 0;
}
static PyObject *heap_report(PyObject *object, PyObject *unused) {
    SqliteProcessBudget *self = (SqliteProcessBudget *)object;
    int64_t current, peak, allocations;
    (void)unused;
    if (!heap_ready(self)) return NULL;
    if (!rh_sqlite_heap_report(self->heap, &current, &peak, &allocations)) {
        PyErr_SetString(PyExc_RuntimeError,
            "SQLite process heap grant, accounting or default-cache policy changed");
        return NULL;
    }
    return Py_BuildValue("{s:L,s:L,s:L,s:L}",
        "heap_limit_bytes", (long long)rh_sqlite_heap_limit(self->heap),
        "sqlite_heap_bytes", (long long)current, "sqlite_heap_peak_bytes", (long long)peak,
        "sqlite_malloc_count", (long long)allocations);
}
static PyObject *heap_increase(PyObject *object, PyObject *value) {
    SqliteProcessBudget *self = (SqliteProcessBudget *)object;
    uint64_t limit;
    if (!heap_ready(self)) return NULL;
    if (positive_integer(value, &limit) < 0) return NULL;
    if ((int64_t)limit <= rh_sqlite_heap_limit(self->heap)) {
        PyErr_SetString(PyExc_ValueError, "an explicit heap increase must exceed the admitted grant");
        return NULL;
    }
    if (!rh_sqlite_heap_increase(self->heap, (int64_t)limit)) {
        PyErr_SetString(PyExc_RuntimeError, "SQLite heap grant changed or increase verification failed");
        return NULL;
    }
    Py_RETURN_NONE;
}
static PyObject *heap_retry_report(PyObject *object, PyObject *unused) {
    SqliteProcessBudget *self = (SqliteProcessBudget *)object;
    rh_retry_admission report;
    const char *states[] = {"unconfigured", "admitting", "admitted", "quarantined"};
    const char *stages[] = {"", "open", "identity", "baseline", "configure", "verify", "close"};
    (void)unused;
    if (!heap_ready(self)) return NULL;
    if (!rh_sqlite_retry_report(self->heap, &report) || report.state < 0 || report.state > 3 ||
        report.failed_stage < 0 || report.failed_stage > 6) {
        PyErr_SetString(PyExc_RuntimeError, "application retry admission or its SQLite heap is invalid");
        return NULL;
    }
    return Py_BuildValue("{s:s,s:i,s:i,s:i,s:i,s:K,s:K,s:s,s:i,s:i,s:O}",
        "state", states[report.state],
        "qualified_initial_retry_count", report.initial_count,
        "qualified_initial_delay_milliseconds", report.initial_delay,
        "selected_retry_count", report.count,
        "selected_delay_milliseconds", report.delay,
        "bootstrap_filesystem_api_attempts_prepaid", (unsigned long long)report.prepaid.filesystem_calls,
        "bootstrap_sleep_microseconds_prepaid", (unsigned long long)report.prepaid.sleep_microseconds,
        "first_failure_stage", stages[report.failed_stage],
        "native_result", report.native_result, "native_close_result", report.close_result,
        "native_close_attempted", report.close_attempted ? Py_True : Py_False);
}
static PyMethodDef heap_methods[] = {
    {"retry_report", heap_retry_report, METH_NOARGS, "Observe the application retry admission record; no disk query or reconfiguration."},
    {"increase", heap_increase, METH_O, "Apply an explicit larger application grant without resetting use."},
    {"report", heap_report, METH_NOARGS, "Verify and observe the application SQLite heap ceiling."},
    {NULL, NULL, 0, NULL}
};
static PyTypeObject heap_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "riverhog_client._prepared_primary_native.SqliteProcessBudget",
    .tp_basicsize = (Py_ssize_t)sizeof(SqliteProcessBudget),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_init = heap_init,
    .tp_methods = heap_methods
};
static PyObject *current_process_budget(PyObject *module, PyObject *unused) {
    (void)module; (void)unused;
    if (!process_budget) {
        PyErr_SetString(PyExc_RuntimeError, "SQLite process budget is not configured");
        return NULL;
    }
    if (!heap_ready((SqliteProcessBudget *)process_budget)) return NULL;
    return Py_NewRef(process_budget);
}
static PyObject *runtime_identity(PyObject *module, PyObject *unused) {
    char source[256];
    wchar_t path[32768];
    const char *error;
    PyObject *library, *result;
    (void)module; (void)unused;
    if (!rh_sqlite_runtime_identity(source, sizeof(source), path, &error)) {
        PyErr_SetString(PyExc_RuntimeError, error);
        return NULL;
    }
    library = PyUnicode_FromWideChar(path, -1);
    if (!library) return NULL;
    result = Py_BuildValue("{s:s,s:O}", "sqlite_source_id", source,
                          "sqlite_library_path", library);
    Py_DECREF(library);
    return result;
}

static PyMethodDef module_methods[] = {
    {"runtime_identity", runtime_identity, METH_NOARGS,
        "Read the already loaded SQLite module identity without opening a database or changing global policy."},
    {"filesystem_cost", native_filesystem_cost, METH_VARARGS,
        "Qualified desktop-NT filesystem-attempt/internal-wait upper bound, excluding close and non-filesystem APIs."},
    {"retry_bootstrap_cost", retry_bootstrap_cost, METH_VARARGS,
        "Conservative qualified desktop-NT open, identity and close filesystem-attempt/nominal-wait recipe."},
    {"current_process_budget", current_process_budget, METH_NOARGS,
        "Get the existing application owner without creating or changing a global grant."},
    {NULL, NULL, 0, NULL}
};

typedef struct {
    PyObject_HEAD
    rh_write_vfs *vfs;
    PyObject *quota, *callbacks, *heap;
    char name[96];
    int initialized;
} WriteVfs;

static int quota_attachable(WriteQuota *quota) {
    if (!ready(quota)) return 0;
    if (quota->native_users == PY_SSIZE_T_MAX) {
        PyErr_SetString(PyExc_OverflowError, "write quota attachment count exhausted");
        return 0;
    }
    return 1;
}
static void quota_attach(WriteQuota *quota) {
    quota->native_thread = PyThread_get_thread_ident();
    ++quota->native_users;
}
static void quota_detach(WriteQuota *quota) { --quota->native_users; }

static int vfs_ready(WriteVfs *self) {
    if (!self->vfs) {
        PyErr_SetString(PyExc_RuntimeError, "native write VFS is closed or uninitialized");
        return 0;
    }
    return 1;
}
static int vfs_init(PyObject *object, PyObject *args, PyObject *kwargs) {
    WriteVfs *self = (WriteVfs *)object;
    ReadQuota *reads;
    CallbackQuota *callbacks;
    SqliteProcessBudget *heap;
    WriteQuota *quota;
    const char *expected, *error;
    static char *keywords[] = {"expected_source_id", "reads", "callbacks", "heap", NULL};
    if (self->initialized) {
        PyErr_SetString(PyExc_RuntimeError, "native write VFS cannot be reinitialized");
        return -1;
    }
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sO!O!O!", keywords,
                                    &expected, &read_type, &reads, &callback_type, &callbacks,
                                    &heap_type, &heap)) return -1;
    if (!read_ready(reads) || !callback_ready(callbacks, 1) || !heap_ready(heap) ||
        !native_work_ready(callbacks->work, 1)) return -1;
    if (callbacks->quota != reads->quota) {
        PyErr_SetString(PyExc_ValueError, "read and callback grants require the same write owner");
        return -1;
    }
    quota = (WriteQuota *)reads->quota;
    if (!quota_attachable(quota)) return -1;
    self->initialized = 1;
    PyOS_snprintf(self->name, sizeof(self->name), "riverhog-native-write-%p", (void *)self);
    self->vfs = rh_write_vfs_create(self->name, expected, &quota->quota, &reads->reads, &callbacks->calls, &callbacks->work->work, heap->heap, &error);
    if (!self->vfs) {
        PyErr_SetString(PyExc_RuntimeError, error);
        return -1;
    }
    quota_attach(quota);
    self->quota = Py_NewRef((PyObject *)reads);
    self->callbacks = Py_NewRef((PyObject *)callbacks);
    self->heap = Py_NewRef((PyObject *)heap);
    callbacks->attached = 1;
    callbacks->work->attached = 1;
    /* Registration pins callbacks and their bank until explicit file-free close. */
    Py_INCREF(self);
    return 0;
}
static void vfs_dealloc(PyObject *object) {
    WriteVfs *self = (WriteVfs *)object;
    Py_XDECREF(self->quota);
    Py_XDECREF(self->callbacks);
    Py_XDECREF(self->heap);
    Py_TYPE(self)->tp_free(object);
}
static PyObject *vfs_bind(PyObject *object, PyObject *args) {
    WriteVfs *self = (WriteVfs *)object;
    const char *path;
    PyObject *handle_value, *journal_value;
    uint32_t journal_identity[3], *journal = NULL;
    void *handle;
    int i;
    if (!vfs_ready(self)) return NULL;
    if (!PyArg_ParseTuple(args, "sOO", &path, &handle_value, &journal_value)) return NULL;
    if (journal_value != Py_None) {
        if (!PyTuple_CheckExact(journal_value) || PyTuple_GET_SIZE(journal_value) != 3) {
            PyErr_SetString(PyExc_TypeError, "journal identity requires an exact three-integer tuple");
            return NULL;
        }
        for (i = 0; i < 3; ++i) {
            PyObject *value = PyTuple_GET_ITEM(journal_value, i);
            unsigned long long identity;
            if (!PyLong_CheckExact(value)) {
                PyErr_SetString(PyExc_TypeError, "journal identity requires exact unsigned integers");
                return NULL;
            }
            identity = PyLong_AsUnsignedLongLong(value);
            if (PyErr_Occurred()) return NULL;
            if (identity > UINT32_MAX) {
                PyErr_SetString(PyExc_OverflowError, "journal identity exceeds a 32-bit field");
                return NULL;
            }
            journal_identity[i] = (uint32_t)identity;
        }
        journal = journal_identity;
    }
    if (!PyLong_CheckExact(handle_value)) {
        PyErr_SetString(PyExc_TypeError, "native binding requires an exact handle integer");
        return NULL;
    }
    handle = PyLong_AsVoidPtr(handle_value);
    if (PyErr_Occurred()) return NULL;
    if (rh_write_vfs_bind(self->vfs, path, handle, journal)) {
        PyErr_SetString(PyExc_RuntimeError,
            "native binding requires its owning thread, an unbound file-free nonquarantined VFS and matching file identity");
        return NULL;
    }
    Py_RETURN_NONE;
}
static PyObject *vfs_admit_retries(PyObject *object, PyObject *args) {
    WriteVfs *self = (WriteVfs *)object;
    PyObject *initial, *selected, *bootstrap;
    uint64_t initial_count, initial_delay, count, delay, calls, sleeps;
    const char *error;
    if (!vfs_ready(self)) return NULL;
    if (!PyArg_ParseTuple(args, "OOO", &initial, &selected, &bootstrap)) return NULL;
    if (!retry_pair(initial, &initial_count, &initial_delay, 1) ||
        !retry_pair(selected, &count, &delay, 1) ||
        !retry_pair(bootstrap, &calls, &sleeps, 0)) return NULL;
    if (!rh_write_vfs_admit_retries(self->vfs, (int64_t)initial_count, (int64_t)initial_delay,
                                   (int64_t)count, (int64_t)delay, calls, sleeps, &error)) {
        PyErr_SetString(PyExc_RuntimeError, error);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *vfs_unbind(PyObject *object, PyObject *unused) {
    WriteVfs *self = (WriteVfs *)object;
    (void)unused;
    if (!vfs_ready(self)) return NULL;
    if (rh_write_vfs_unbind(self->vfs)) {
        PyErr_SetString(PyExc_RuntimeError,
            "native unbind requires its owning thread, no live files, no quarantined close and successful pin close");
        return NULL;
    }
    Py_RETURN_NONE;
}
static PyObject *vfs_bound(PyObject *object, void *unused) {
    WriteVfs *self = (WriteVfs *)object;
    int bound;
    (void)unused;
    if (!vfs_ready(self)) return NULL;
    bound = rh_write_vfs_bound(self->vfs);
    if (bound < 0) {
        PyErr_SetString(PyExc_RuntimeError, "native binding requires its owning thread");
        return NULL;
    }
    return PyBool_FromLong(bound);
}

static PyObject *vfs_arm(PyObject *object, PyObject *args) {
    WriteVfs *self = (WriteVfs *)object;
    ReadQuota *reads;
    CallbackQuota *callbacks;
    int result;
    if (!vfs_ready(self)) return NULL;
    if (!PyArg_ParseTuple(args, "O!O!", &read_type, &reads, &callback_type, &callbacks)) return NULL;
    if (!read_ready(reads) || !callback_ready(callbacks, 0)) return NULL;
    if (callbacks->quota != reads->quota) {
        PyErr_SetString(PyExc_ValueError, "read and callback grants require the same write owner");
        return NULL;
    }
    if (callbacks->attached && (PyObject *)callbacks != self->callbacks) {
        PyErr_SetString(PyExc_RuntimeError, "callback quota is attached to another VFS");
        return NULL;
    }
    if (!native_work_ready(callbacks->work, 0)) return NULL;
    if (callbacks->work->attached && callbacks->work != ((CallbackQuota *)self->callbacks)->work) {
        PyErr_SetString(PyExc_RuntimeError, "native work quota is attached to another VFS");
        return NULL;
    }
    if (!quota_attachable((WriteQuota *)reads->quota)) return NULL;
    result = rh_write_vfs_arm(self->vfs, &((WriteQuota *)reads->quota)->quota,
                              &reads->reads, &callbacks->calls, &callbacks->work->work);
    if (result) {
        PyErr_SetString(PyExc_RuntimeError,
            "native write VFS requires its owning thread, closed poisoned files, no quarantined close and close capacity");
        return NULL;
    }
    quota_attach((WriteQuota *)reads->quota);
    quota_detach((WriteQuota *)((ReadQuota *)self->quota)->quota);
    ((CallbackQuota *)self->callbacks)->work->attached = 0;
    ((CallbackQuota *)self->callbacks)->attached = 0;
    callbacks->attached = 1;
    callbacks->work->attached = 1;
    Py_SETREF(self->quota, Py_NewRef((PyObject *)reads));
    Py_SETREF(self->callbacks, Py_NewRef((PyObject *)callbacks));
    Py_RETURN_NONE;
}
static PyObject *vfs_close(PyObject *object, PyObject *unused) {
    WriteVfs *self = (WriteVfs *)object;
    int result;
    (void)unused;
    if (!self->vfs) Py_RETURN_NONE;
    result = rh_write_vfs_close(self->vfs);
    if (result) {
        PyErr_SetString(PyExc_RuntimeError,
            "native write VFS cannot close with live files, a quarantined close or from another thread");
        return NULL;
    }
    self->vfs = NULL;
    quota_detach((WriteQuota *)((ReadQuota *)self->quota)->quota);
    Py_CLEAR(self->quota);
    ((CallbackQuota *)self->callbacks)->work->attached = 0;
    ((CallbackQuota *)self->callbacks)->attached = 0;
    Py_CLEAR(self->callbacks);
    Py_CLEAR(self->heap);
    Py_DECREF(self);
    Py_RETURN_NONE;
}
static PyObject *vfs_identity(PyObject *object, PyObject *unused) {
    WriteVfs *self = (WriteVfs *)object;
    PyObject *path, *result;
    (void)unused;
    if (!vfs_ready(self)) return NULL;
    path = PyUnicode_FromWideChar(rh_write_vfs_library_path(self->vfs), -1);
    if (!path) return NULL;
    result = Py_BuildValue("{s:s,s:s,s:O}",
        "sqlite_source_id", rh_write_vfs_source_id(self->vfs),
        "base_vfs", "win32", "sqlite_library_path", path);
    Py_DECREF(path);
    return result;
}
static PyObject *vfs_name(PyObject *object, void *unused) {
    WriteVfs *self = (WriteVfs *)object;
    (void)unused;
    if (!vfs_ready(self)) return NULL;
    return PyUnicode_FromString(self->name);
}
static PyObject *vfs_open_files(PyObject *object, void *unused) {
    WriteVfs *self = (WriteVfs *)object;
    int count;
    (void)unused;
    if (!vfs_ready(self)) return NULL;
    count = rh_write_vfs_open_files(self->vfs);
    if (count < 0) {
        PyErr_SetString(PyExc_RuntimeError, "native write VFS requires its owning thread");
        return NULL;
    }
    return PyLong_FromLong(count);
}
static PyObject *vfs_quarantined(PyObject *object, void *unused) {
    WriteVfs *self = (WriteVfs *)object;
    int quarantined;
    (void)unused;
    if (!vfs_ready(self)) return NULL;
    quarantined = rh_write_vfs_quarantined(self->vfs);
    if (quarantined < 0) {
        PyErr_SetString(PyExc_RuntimeError, "native write VFS requires its owning thread");
        return NULL;
    }
    return PyBool_FromLong(quarantined);
}
static PyMethodDef vfs_methods[] = {
    {"admit_retries", vfs_admit_retries, METH_VARARGS,
        "At controlled quiescent startup, prepay a bound existing-file bootstrap and admit one application retry policy."},
    {"bind", vfs_bind, METH_VARARGS, "Pin the database admitted by the caller's live native owner."},
    {"unbind", vfs_unbind, METH_NOARGS, "Release the binding after every selected native file closes."},
    {"arm", vfs_arm, METH_VARARGS, "Select a bank before entering SQLite on its owning thread."},
    {"close", vfs_close, METH_NOARGS, "Unregister after the owner closes every SQLite connection using this VFS."},
    {"identity", vfs_identity, METH_NOARGS, "Describe the already loaded SQLite module."},
    {NULL, NULL, 0, NULL}
};
static PyGetSetDef vfs_getset[] = {
    {"bound", vfs_bound, NULL, "A validated native database pin is held.", NULL},
    {"name", vfs_name, NULL, "Explicit nondefault VFS name.", NULL},
    {"open_files", vfs_open_files, NULL, "Selected SQLite files awaiting xClose; quarantine separately blocks release.", NULL},
    {"quarantined", vfs_quarantined, NULL, "Native close or close accounting failed; the owner stays pinned through process exit.", NULL},
    {NULL, NULL, NULL, NULL, NULL}
};
static PyTypeObject vfs_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "riverhog_client._prepared_primary_native.WriteVfs",
    .tp_basicsize = (Py_ssize_t)sizeof(WriteVfs),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_init = vfs_init,
    .tp_dealloc = vfs_dealloc,
    .tp_methods = vfs_methods,
    .tp_getset = vfs_getset
};

static struct PyModuleDef module_definition = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_prepared_primary_native",
    .m_doc = "Fixed-capacity native storage accounting without an embedded SQLite engine.",
    .m_size = -1,
    .m_methods = module_methods
};

PyMODINIT_FUNC PyInit__prepared_primary_native(void) {
    PyObject *module;
    quota_type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&quota_type) < 0) return NULL;
    read_type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&read_type) < 0) return NULL;
    callback_type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&callback_type) < 0) return NULL;
    native_work_type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&native_work_type) < 0) return NULL;
    native_close_type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&native_close_type) < 0) return NULL;
    heap_type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&heap_type) < 0) return NULL;
    vfs_type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&vfs_type) < 0) return NULL;
    module = PyModule_Create(&module_definition);
    if (!module) return NULL;
    if (PyModule_AddObjectRef(module, "WriteQuota", (PyObject *)&quota_type) < 0) {
        Py_DECREF(module); return NULL;
    }
    if (PyModule_AddObjectRef(module, "ReadQuota", (PyObject *)&read_type) < 0) {
        Py_DECREF(module); return NULL;
    }
    if (PyModule_AddObjectRef(module, "CallbackQuota", (PyObject *)&callback_type) < 0) {
        Py_DECREF(module); return NULL;
    }
    if (PyModule_AddObjectRef(module, "NativeWorkQuota", (PyObject *)&native_work_type) < 0) {
        Py_DECREF(module); return NULL;
    }
    if (PyModule_AddObjectRef(module, "NativeCloseCredit", (PyObject *)&native_close_type) < 0) {
        Py_DECREF(module); return NULL;
    }
    if (PyModule_AddObjectRef(module, "SqliteProcessBudget", (PyObject *)&heap_type) < 0) {
        Py_DECREF(module); return NULL;
    }
    if (PyModule_AddObjectRef(module, "WriteVfs", (PyObject *)&vfs_type) < 0) {
        Py_DECREF(module); return NULL;
    }
    if (PyModule_AddStringConstant(module, "compiler", RH_COMPILER) < 0) {
        Py_DECREF(module); return NULL;
    }
    return module;
}

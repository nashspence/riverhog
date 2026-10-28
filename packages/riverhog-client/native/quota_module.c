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
    rh_write_vfs *vfs;
    PyObject *quota;
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
    WriteQuota *quota;
    const char *expected, *error;
    static char *keywords[] = {"expected_source_id", "reads", NULL};
    if (self->initialized) {
        PyErr_SetString(PyExc_RuntimeError, "native write VFS cannot be reinitialized");
        return -1;
    }
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sO!", keywords,
                                    &expected, &read_type, &reads)) return -1;
    if (!read_ready(reads)) return -1;
    quota = (WriteQuota *)reads->quota;
    if (!quota_attachable(quota)) return -1;
    self->initialized = 1;
    PyOS_snprintf(self->name, sizeof(self->name), "riverhog-native-write-%p", (void *)self);
    self->vfs = rh_write_vfs_create(self->name, expected, &quota->quota, &reads->reads, &error);
    if (!self->vfs) {
        PyErr_SetString(PyExc_RuntimeError, error);
        return -1;
    }
    quota_attach(quota);
    self->quota = Py_NewRef((PyObject *)reads);
    /* Registration pins callbacks and their bank until explicit file-free close. */
    Py_INCREF(self);
    return 0;
}
static void vfs_dealloc(PyObject *object) {
    WriteVfs *self = (WriteVfs *)object;
    Py_XDECREF(self->quota);
    Py_TYPE(self)->tp_free(object);
}
static PyObject *vfs_arm(PyObject *object, PyObject *value) {
    WriteVfs *self = (WriteVfs *)object;
    int result;
    if (!vfs_ready(self)) return NULL;
    if (!Py_IS_TYPE(value, &read_type)) {
        PyErr_SetString(PyExc_TypeError, "native write VFS requires an initialized ReadQuota");
        return NULL;
    }
    if (!read_ready((ReadQuota *)value)) return NULL;
    if (!quota_attachable((WriteQuota *)((ReadQuota *)value)->quota)) return NULL;
    result = rh_write_vfs_arm(self->vfs, &((WriteQuota *)((ReadQuota *)value)->quota)->quota,
                              &((ReadQuota *)value)->reads);
    if (result) {
        PyErr_SetString(PyExc_RuntimeError,
            "native write VFS requires its owning thread and closed poisoned files");
        return NULL;
    }
    quota_attach((WriteQuota *)((ReadQuota *)value)->quota);
    quota_detach((WriteQuota *)((ReadQuota *)self->quota)->quota);
    Py_SETREF(self->quota, Py_NewRef(value));
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
            "native write VFS cannot close with live files or from another thread");
        return NULL;
    }
    self->vfs = NULL;
    quota_detach((WriteQuota *)((ReadQuota *)self->quota)->quota);
    Py_CLEAR(self->quota);
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
static PyMethodDef vfs_methods[] = {
    {"arm", vfs_arm, METH_O, "Select a bank before entering SQLite on its owning thread."},
    {"close", vfs_close, METH_NOARGS, "Unregister after the owner closes every SQLite connection using this VFS."},
    {"identity", vfs_identity, METH_NOARGS, "Describe the already loaded SQLite module."},
    {NULL, NULL, 0, NULL}
};
static PyGetSetDef vfs_getset[] = {
    {"name", vfs_name, NULL, "Explicit nondefault VFS name.", NULL},
    {"open_files", vfs_open_files, NULL, "Live delegated native files.", NULL},
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
    .m_size = -1
};

PyMODINIT_FUNC PyInit__prepared_primary_native(void) {
    PyObject *module;
    quota_type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&quota_type) < 0) return NULL;
    read_type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&read_type) < 0) return NULL;
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
    if (PyModule_AddObjectRef(module, "WriteVfs", (PyObject *)&vfs_type) < 0) {
        Py_DECREF(module); return NULL;
    }
    if (PyModule_AddStringConstant(module, "compiler", RH_COMPILER) < 0) {
        Py_DECREF(module); return NULL;
    }
    return module;
}

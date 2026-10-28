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
} WriteQuota;

static int positive_integer(PyObject *object, uint64_t *value) {
    long long parsed;
    if (!PyLong_CheckExact(object)) {
        PyErr_SetString(PyExc_TypeError, "write limits require exact integers");
        return -1;
    }
    parsed = PyLong_AsLongLong(object);
    if (PyErr_Occurred()) return -1;
    if (parsed <= 0) {
        PyErr_SetString(PyExc_ValueError, "write limits must be positive signed 64-bit values");
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

static struct PyModuleDef module_definition = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_prepared_primary_native",
    .m_doc = "Fixed-capacity native write accounting without an embedded SQLite engine.",
    .m_size = -1
};

PyMODINIT_FUNC PyInit__prepared_primary_native(void) {
    PyObject *module;
    quota_type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&quota_type) < 0) return NULL;
    module = PyModule_Create(&module_definition);
    if (!module) return NULL;
    if (PyModule_AddObjectRef(module, "WriteQuota", (PyObject *)&quota_type) < 0) {
        Py_DECREF(module); return NULL;
    }
    if (PyModule_AddStringConstant(module, "compiler", RH_COMPILER) < 0) {
        Py_DECREF(module); return NULL;
    }
    return module;
}

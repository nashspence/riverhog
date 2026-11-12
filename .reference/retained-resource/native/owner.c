#define PY_SSIZE_T_CLEAN
#define _POSIX_C_SOURCE 200809L
#include <Python.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#ifdef _WIN32
#if !defined(_MSC_VER) || !defined(_DLL)
#error "Windows requires the same dynamic UCRT profile as supported CPython, not a separate static CRT"
#endif
#include <windows.h>
#include <io.h>
#include <share.h>
#include <process.h>
#include <stdlib.h>
#include <wchar.h>
#define getpid _getpid
/* A local handler turns invalid-descriptor diagnosis into UNKNOWN, not process
   termination. Never change the process-global handler. */
static void invalid_argument(const wchar_t *e, const wchar_t *f, const wchar_t *p,
                             unsigned int line, uintptr_t reserved) {
    (void)e; (void)f; (void)p; (void)line; (void)reserved;
}
static int native_close(int fd) {
    _invalid_parameter_handler prior = _set_thread_local_invalid_parameter_handler(invalid_argument);
    int rc = _close(fd); int error = errno;
    _set_thread_local_invalid_parameter_handler(prior); errno = error; return rc;
}
#else
#include <unistd.h>
#define native_close close
#endif
#include <string.h>

#if !defined(__linux__) && !defined(__APPLE__) && !defined(_WIN32)
#error "This reference backend supports Linux/Darwin POSIX and Windows UCRT."
#endif
#ifdef Py_GIL_DISABLED
#error "The reference requires GIL-enabled CPython."
#endif

/* No resource effects in allocation, initialization, or finalization. */
typedef enum { NEW, OPENING, LIVE, REJECTED, ABSENT, CLOSING, RECEIVED, UNKNOWN } State;
static const char *names[] = {"new", "opening", "live", "rejected", "absent", "closing", "received", "unknown"};
typedef struct Resource Resource;
struct Resource {
    PyObject_HEAD
    int fd, directory, error, fault, syncing;
    State state;
    pid_t pid;
    Py_ssize_t pins, children;
    Resource *parent;
    unsigned long opens, duplicates, stats, closes, syncs;
};
typedef struct {
    PyObject_HEAD
    Resource *owner;
    int active, ended;
    pid_t pid;
} Borrow;
static PyTypeObject ResourceType, BorrowType;

static int same_process(Resource *r) {
    if (getpid() != r->pid) {
        PyErr_SetString(PyExc_RuntimeError, "resource belongs to the original process; fork is not receiving");
        return 0;
    }
    return 1;
}
static PyObject *fail(const char *message) {
    PyErr_SetString(PyExc_RuntimeError, message);
    return NULL;
}
static int live(Resource *r) {
    if (!same_process(r)) return 0;
    if (r->state != LIVE) {
        PyErr_SetString(PyExc_RuntimeError, "resource has no lendable confirmed acquisition");
        return 0;
    }
    return 1;
}
static void release_parent(Resource *r) {
    Resource *p = r->parent;
    if (p != NULL) {
        /* Called only when no child resource was acquired or its close returned zero. */
        r->parent = NULL;
        p->children--;
        Py_DECREF(p);
    }
}
static PyObject *resource_new(PyTypeObject *type, PyObject *args, PyObject *kw) {
    const char *kind;
    static char *keys[] = {"kind", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "s", keys, &kind)) return NULL;
    if (strcmp(kind,"file") && strcmp(kind,"directory")) return fail("kind must be file or directory");
#ifdef _WIN32
    if (!strcmp(kind, "directory")) return fail("Windows Native journal creation uses CRT regular files; POSIX corpus/directory receiving is not enabled on Windows");
#endif
    Resource *r = (Resource *)type->tp_alloc(type, 0);
    if (!r) return NULL;
    r->fd = -1; r->state = NEW; r->pid = getpid();
    r->directory = !strcmp(kind, "directory");
    return (PyObject *)r;
}
static void resource_dealloc(Resource *r) {
    /* Deliberately neither close nor report receiving. Caller retention is mandatory.
       An unreconciled parent dependency stays pinned even if a caller violates it. */
    Py_XDECREF(r->parent);
    Py_TYPE(r)->tp_free((PyObject *)r);
}

/* Test seams are not in the normal wheel. They never supply production evidence. */
#ifdef RETAINED_RESOURCE_TESTING
static PyObject *set_fault(Resource *r, PyObject *arg) {
    const char *s = PyUnicode_AsUTF8(arg);
    if (!s) return NULL;
    if (!same_process(r)) return NULL;
    if (r->state == OPENING || r->state == CLOSING) return fail("operation is active");
    const char *f[] = {"none", "before_open", "after_open", "before_close", "after_close", "unknown_open", "unknown_close_open", "unknown_close_closed", "signal_after_open", "signal_after_close", "stat_refusal", "before_sync", "after_sync"};
    for (int i=0;i<13;i++) if (!strcmp(s,f[i])) { r->fault=i; Py_RETURN_NONE; }
    return fail("unknown test seam");
}
#endif
static int consume_fault(Resource *r, int opening) {
    int f = r->fault;
    int for_open = f==1 || f==2 || f==5 || f==8 || f==10;
    if ((opening && for_open) || (!opening && !for_open)) { r->fault=0; return f; }
    return 0;
}
static PyObject *resource_acquire(Resource *r, PyObject *args, PyObject *kw) {
    PyObject *path, *parent_obj = Py_None;
    int writable=0, exclusive=0, private_owned=0;
    static char *keys[]={"path","writable","parent","exclusive","private_owned",NULL};
    if(!PyArg_ParseTupleAndKeywords(args,kw,"O|pOpp",keys,&path,&writable,&parent_obj,&exclusive,&private_owned)) return NULL;
    if(!same_process(r)) return NULL;
    if(r->state!=NEW) return fail("acquisition is single-attempt; inspect the retained owner");
    if(r->directory && (writable || exclusive)) return fail("directory acquisition cannot create or write");
    if(exclusive && !writable) return fail("exclusive creation requires writable regular file");
    Resource *p=NULL;
#ifdef _WIN32
    if(parent_obj!=Py_None) return fail("CRT acquisition requires owner-validated absolute path, not a POSIX dir_fd");
    if(!PyUnicode_CheckExact(path)) return fail("Windows path requires an exact Unicode string");
    Py_ssize_t n=0;
    wchar_t *s=PyUnicode_AsWideCharString(path,&n);
    if(!s) return NULL;
    /* Admit native absolute paths, including UNC/extended names. The caller
       retains and authenticates the private namespace; this is not that guard. */
    int absolute=n>=3 && ((s[0]>=L'A'&&s[0]<=L'Z')||(s[0]>=L'a'&&s[0]<=L'z'))
                 && s[1]==L':' && (s[2]==L'\\'||s[2]==L'/');
    absolute = absolute || (n>=2 && s[0]==L'\\' && s[1]==L'\\');
    if(!absolute || (Py_ssize_t)wcslen(s)!=n) {
        PyMem_Free(s); return fail("Windows profile requires a native absolute path");
    }
    /* CRT pmode is not an ACL. The application has already admitted its
       private current-user directory. Do not claim a POSIX UID/mode proof. */
    if(private_owned) { PyMem_Free(s); return fail("Windows privacy belongs to the existing admitted directory/ACL owner, not CRT mode bits"); }
#else
    if(!PyBytes_CheckExact(path)) return fail("POSIX path requires pre-encoded exact bytes");
    char *s=PyBytes_AS_STRING(path); Py_ssize_t n=PyBytes_GET_SIZE(path);
    if(!n || memchr(s,0,(size_t)n)) return fail("empty or NUL-containing path");
    int at=AT_FDCWD;
    if(parent_obj!=Py_None) {
        if(!Py_IS_TYPE(parent_obj,&ResourceType)) return fail("parent requires a retained directory owner");
        p=(Resource*)parent_obj;
        if(p==r || !p->directory || !live(p)) return fail("invalid or unconfirmed parent");
        if(memchr(s,'/',(size_t)n)||!strcmp(s,".")||!strcmp(s,"..")) return fail("relative child must be one component");
        if(p->children==PY_SSIZE_T_MAX) return fail("dependency capacity exhausted");
        at=p->fd;
    } else if(s[0]!='/') return fail("root acquisition requires an admitted absolute path");
#endif
    int f=consume_fault(r,1);
    if(f==1) {
#ifdef _WIN32
        PyMem_Free(s);
#endif
        return fail("injected before-open refusal: no syscall entered");
    }
    if(p) { Py_INCREF(p); r->parent=p; p->children++; }
    r->state=OPENING; r->opens++;
    int fd=-1, error=0, rc;
    Py_BEGIN_ALLOW_THREADS
#ifdef _WIN32
    int flags=(writable?_O_RDWR:_O_RDONLY)|_O_BINARY|_O_NOINHERIT;
    if(exclusive) flags|=_O_CREAT|_O_EXCL;
    errno_t result=_wsopen_s(&fd,s,flags,_SH_DENYNO,_S_IREAD|_S_IWRITE);
    error=result ? (int)result : 0;
#else
    int flags=(writable?O_RDWR:O_RDONLY)|O_CLOEXEC|O_NOFOLLOW|O_NONBLOCK;
    if(r->directory) flags|=O_DIRECTORY;
    if(exclusive) flags|=O_CREAT|O_EXCL;
    fd=openat(at,s,flags,0600); error=fd<0?errno:0;
#endif
    Py_END_ALLOW_THREADS
    /* Same retained native object, before any allocating result or Python callback. */
    r->fd=fd; r->error=error; r->state=fd<0?ABSENT:OPENING;
#ifdef _WIN32
    PyMem_Free(s);
#endif
    if(fd<0) { release_parent(r); errno=error; return PyErr_SetFromErrno(PyExc_OSError); }
    if(f==5) { r->state=UNKNOWN; return fail("injected acquisition outcome unknown; original remains charged"); }
    r->stats++;
#ifdef _WIN32
    struct _stat64 st;
#else
    struct stat st;
#endif
    Py_BEGIN_ALLOW_THREADS
#ifdef _WIN32
    rc=_fstat64(fd,&st);
#else
    rc=fstat(fd,&st);
#endif
    error=rc<0?errno:0;
    Py_END_ALLOW_THREADS
    if(f==10) { rc=-1; error=EIO; }
    r->state=REJECTED;
    if(rc<0) { r->error=error; errno=error; return PyErr_SetFromErrno(PyExc_OSError); }
#ifdef _WIN32
    int valid=(st.st_mode&_S_IFMT)==_S_IFREG;
#else
    int valid=r->directory?S_ISDIR(st.st_mode):S_ISREG(st.st_mode);
    if(private_owned && (st.st_uid!=geteuid() || (st.st_mode&077))) valid=0;
#endif
    if(!valid) return fail("rejected post-open kind/private-owner validation; original remains retained");
    r->state=LIVE;
    if(f==2) return fail("injected lost acquisition reply after retained native result");
    if(f==8) { PyErr_SetInterrupt(); if(PyErr_CheckSignals()<0) return NULL; }
    Py_RETURN_NONE;
}
static PyObject *resource_duplicate(Resource *r, PyObject *arg) {
#ifdef _WIN32
    (void)r; (void)arg;
    return fail("this Windows constructor profile does not duplicate descriptors");
#else
    if (!same_process(r)) return NULL;
    if (r->state != NEW || !Py_IS_TYPE(arg,&ResourceType)) return fail("duplication requires new destination and retained source");
    Resource *s = (Resource *)arg;
    if (!live(s)) return NULL;
    if (r->directory != s->directory || s->pins == PY_SSIZE_T_MAX) return fail("duplicate kind or pin capacity differs");
    int f = consume_fault(r, 1);
    if (f == 1) return fail("injected before-duplicate refusal");
    if (s->parent) {
        if (s->parent->children==PY_SSIZE_T_MAX) return fail("parent dependency capacity exhausted");
        r->parent=(Resource*)Py_NewRef((PyObject*)s->parent); r->parent->children++;
    }
    s->pins++; r->state=OPENING; r->duplicates++;
    int fd, error;
    Py_BEGIN_ALLOW_THREADS
    fd=fcntl(s->fd, F_DUPFD_CLOEXEC, 0); error=fd<0?errno:0;
    Py_END_ALLOW_THREADS
    r->fd=fd; r->error=error; r->state=fd<0?ABSENT:LIVE; s->pins--;
    if (fd<0) { release_parent(r); errno=error; return PyErr_SetFromErrno(PyExc_OSError); }
    if (f==5) { r->state=UNKNOWN; return fail("injected uncertain duplication"); }
    if (f==2) return fail("injected lost duplicate reply");
    if (f==8) { PyErr_SetInterrupt(); if(PyErr_CheckSignals()<0) return NULL; }
    Py_RETURN_NONE;
#endif
}
static PyObject *resource_close(Resource *r, PyObject *unused) {
    (void)unused;
    if (!same_process(r)) return NULL;
    if (r->state==RECEIVED) Py_RETURN_NONE;
    if (r->state==UNKNOWN) return fail("original close/acquisition is unknown; no integer retry is allowed");
    if (r->pins || r->children || r->syncing || r->state==OPENING || r->state==CLOSING) {
        PyErr_SetString(PyExc_BlockingIOError,"dependent borrow, child, or native operation has not received"); return NULL;
    }
    if (r->state==NEW || r->state==ABSENT) { r->state=RECEIVED; Py_RETURN_NONE; }
    int f=consume_fault(r, 0);
    if (f==3) return fail("injected before-close refusal: no syscall entered");
    int fd=r->fd, rc, error;
    r->fd=-1; r->state=CLOSING;
    /* fd can never again be an executable target on this object, even on error. */
    if (f==6) { rc=-1; error=EINTR; }  /* unknown to the owner, test leaves fd open */
    else {
        r->closes++;
        Py_BEGIN_ALLOW_THREADS
        rc=native_close(fd); error=rc<0?errno:0;
        Py_END_ALLOW_THREADS
        if(f==7 && rc==0) { rc=-1; error=EINTR; } /* lost native outcome */
    }
    r->error=error;
    if (rc<0) {
        r->state=UNKNOWN;
        errno=error; return PyErr_SetFromErrno(PyExc_OSError);
    }
    r->state=RECEIVED; release_parent(r);
    if(f==4) return fail("injected lost close reply after receiving");
    if(f==9) { PyErr_SetInterrupt(); if(PyErr_CheckSignals()<0) return NULL; }
    Py_RETURN_NONE;
}
static PyObject *resource_sync(Resource *r, PyObject *unused) {
    (void)unused;
    if(!live(r)) return NULL;
    if(r->syncing) return fail("sync is active");
    int f=r->fault;
    if(f==11 || f==12) r->fault=0;
    if(f==11) return fail("injected before-sync refusal");
    r->syncing=1; r->syncs++;
    int rc,error;
    Py_BEGIN_ALLOW_THREADS
#ifdef _WIN32
    rc=_commit(r->fd);
#else
    rc=fsync(r->fd);
#endif
    error=rc<0?errno:0;
    Py_END_ALLOW_THREADS
    r->syncing=0;
    if(rc<0) { errno=error; return PyErr_SetFromErrno(PyExc_OSError); }
    if(f==12) return fail("injected lost sync reply; no mutation success is inferred");
    Py_RETURN_NONE;
}
static PyObject *get_state(Resource *r, void *c) { (void)c; return PyUnicode_FromString(names[r->state]); }
static PyObject *get_received(Resource *r, void *c) { (void)c; return PyBool_FromLong(r->state==RECEIVED && getpid()==r->pid); }
static PyObject *get_error(Resource *r, void *c) { (void)c; return PyLong_FromLong(r->error); }
static PyObject *get_metrics(Resource *r, void *c) {
    (void)c;
    return Py_BuildValue("{s:k,s:k,s:k,s:k,s:k,s:n,s:n,s:n}","open_calls",r->opens,"dup_calls",r->duplicates,"fstat_calls",r->stats,"close_calls",r->closes,"sync_calls",r->syncs,"borrows",r->pins,"children",r->children,"owner_storage_bytes",(Py_ssize_t)sizeof(Resource));
}
static PyMethodDef resource_methods[] = {
    {"sync",(PyCFunction)resource_sync,METH_NOARGS,"Sync the original live resource; not a closure or construction receipt."},
    {"acquire",(PyCFunction)(void(*)(void))resource_acquire,METH_VARARGS|METH_KEYWORDS,"Acquire into the already retained owner; never reopen it."},
    {"duplicate",(PyCFunction)resource_duplicate,METH_O,"Duplicate actual held custody into an already retained new owner."},
    {"close",(PyCFunction)resource_close,METH_NOARGS,"Receive once, refuse busy, or quarantine; never retry an uncertain integer."},
#ifdef RETAINED_RESOURCE_TESTING
    {"_test_fault",(PyCFunction)set_fault,METH_O,"Only present in the fault-witness build."},
#endif
    {NULL,NULL,0,NULL}
};
static PyGetSetDef resource_getset[] = {
    {"state",(getter)get_state,NULL,NULL,NULL}, {"received",(getter)get_received,NULL,NULL,NULL},
    {"error",(getter)get_error,NULL,NULL,NULL}, {"metrics",(getter)get_metrics,NULL,NULL,NULL},
    {NULL,NULL,NULL,NULL,NULL}
};
static PyObject *borrow_new(PyTypeObject *type, PyObject *args, PyObject *kw) {
    PyObject *arg; static char *keys[]={"owner",NULL};
    if(!PyArg_ParseTupleAndKeywords(args,kw,"O!",keys,&ResourceType,&arg)) return NULL;
    Borrow *b=(Borrow*)type->tp_alloc(type,0); if(!b) return NULL;
    b->owner=(Resource*)Py_NewRef(arg); b->pid=getpid(); return (PyObject*)b;
}
static void borrow_dealloc(Borrow *b) {
    /* Dropping an active borrow does not manufacture receiving or decrement pins. */
    Py_DECREF(b->owner); Py_TYPE(b)->tp_free((PyObject*)b);
}
static PyObject *borrow_acquire(Borrow *b, PyObject *unused) {
    (void)unused;
    if(!live(b->owner)) return NULL;
    if(b->ended) return fail("borrow is single-lifetime");
    if(!b->active) { if(b->owner->pins==PY_SSIZE_T_MAX) return fail("borrow capacity exhausted"); b->owner->pins++; b->active=1; }
    Py_RETURN_NONE;
}
static PyObject *borrow_fd(Borrow *b, PyObject *unused) {
    (void)unused;
    if(!b->active || b->ended) return fail("borrow is not held");
    if(!live(b->owner)) return NULL;
    return PyLong_FromLong(b->owner->fd);
}
static PyObject *borrow_close(Borrow *b, PyObject *unused) {
    (void)unused;
    if(!same_process(b->owner)) return NULL;
    if(b->active) { b->owner->pins--; b->active=0; }
    b->ended=1; Py_RETURN_NONE;
}
static PyObject *borrow_active(Borrow *b, void *c) { (void)c; return PyBool_FromLong(b->active); }
static PyObject *borrow_received(Borrow *b, void *c) { (void)c; return PyBool_FromLong(b->ended && b->pid==getpid()); }
static PyMethodDef borrow_methods[]={
    {"acquire",(PyCFunction)borrow_acquire,METH_NOARGS,"Pin after caller retention, before lending."},
    {"fileno",(PyCFunction)borrow_fd,METH_NOARGS,"Non-owning access only while this exact borrow remains held."},
    {"close",(PyCFunction)borrow_close,METH_NOARGS,"Receive a quiesced borrow, idempotently."},{NULL,NULL,0,NULL}
};
static PyGetSetDef borrow_getset[]={{"active",(getter)borrow_active,NULL,NULL,NULL},{"received",(getter)borrow_received,NULL,NULL,NULL},{NULL,NULL,NULL,NULL,NULL}};
static PyTypeObject ResourceType={PyVarObject_HEAD_INIT(NULL,0)
    .tp_name="retained_resource.Resource",.tp_basicsize=sizeof(Resource),.tp_flags=Py_TPFLAGS_DEFAULT,
    .tp_new=resource_new,.tp_dealloc=(destructor)resource_dealloc,.tp_methods=resource_methods,.tp_getset=resource_getset};
static PyTypeObject BorrowType={PyVarObject_HEAD_INIT(NULL,0)
    .tp_name="retained_resource.Borrow",.tp_basicsize=sizeof(Borrow),.tp_flags=Py_TPFLAGS_DEFAULT,
    .tp_new=borrow_new,.tp_dealloc=(destructor)borrow_dealloc,.tp_methods=borrow_methods,.tp_getset=borrow_getset};
static PyModuleDef module={PyModuleDef_HEAD_INIT,.m_name="_native",.m_doc="Explicit retained POSIX and UCRT constructor resource effects.",.m_size=-1};
PyMODINIT_FUNC PyInit__native(void) {
    if(PyType_Ready(&ResourceType)<0 || PyType_Ready(&BorrowType)<0) return NULL;
    PyObject *m=PyModule_Create(&module); if(!m) return NULL;
    if(PyModule_AddObjectRef(m,"Resource",(PyObject*)&ResourceType)<0 || PyModule_AddObjectRef(m,"Borrow",(PyObject*)&BorrowType)<0) { Py_DECREF(m); return NULL; }
    return m;
}

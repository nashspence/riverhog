"""Source-owned primitive declarations, explicitly loaded only for evidence."""
from riverhog_growth_contracts import COUNT, Bound, Declaration, Dimension, Expression, declare_growth


def declaration(operation, limits):
    return Declaration(
        id=f"retained-resource.{operation}.growth/v1",
        authority="retained-resource",
        subjects=(f"retained_resource.growth.{operation}",),
        scope="one call on the original caller-retained native resource",
        starts_at="admitted path/record and caller retention before native dispatch",
        ends_at="native result recorded on the original owner before Python reply",
        dimensions=(Dimension("unrelated_files",COUNT,"Unrelated names, never enumerated."),),
        bounds=tuple(Bound(k,COUNT,"upper",Expression("constant",value=v,unit=COUNT),why)
                     for k,v,why in limits),
        conditions=(
            "GIL-enabled CPython; Linux/Darwin POSIX or Windows dynamic UCRT regular-file profile.",
            "Original owner is retained; no external close/detach/dup2; no forced native thread cancellation.",
        ),
        invariants=(
            "At most one dispatched acquisition and native close per original lifetime.",
            "Unknown outcomes never reopen, revalidate a reused slot, or release capacity.",
        ),
        limitations=(
            "Counts describe native API entries, not the implementation's internal syscalls or kernel work.",
            "Windows UCRT open may internally use multiple OS operations; no one-kernel-call claim.",
            "Path conversion/validation is linear in admitted path length. Sync latency and SQLite work are separate.",
        ),
    )


@declare_growth(declaration("acquire",(
    ("open_calls",1,"One backend open entry, no implicit retry."),
    ("dup_calls",0,"Creator acquisition does not duplicate another resource."),
    ("fstat_calls",1,"At most one post-open type/private validation entry."),
    ("close_calls",0,"No constructor/finalizer hides an uncertain close."),
    ("sync_calls",0,"Acquisition does not imply a durability barrier."),
)))
def acquire(owner,path,**options):return owner.acquire(path,**options)


@declare_growth(declaration("receive",(
    ("open_calls",0,"Receiving cannot acquire a replacement."),
    ("dup_calls",0,"Receiving cannot duplicate a reused slot."),
    ("fstat_calls",0,"No identity probe is a close receipt."),
    ("close_calls",1,"One dispatch maximum; zero on positive replay or unknown."),
    ("sync_calls",0,"Receiving is not a sync receipt."),
)))
def receive(owner):return owner.close()


@declare_growth(declaration("sync",(
    ("open_calls",0,"Original retained resource only."),
    ("dup_calls",0,"No temporary unmanaged resource."),
    ("fstat_calls",0,"No fresh identity check substitutes for custody."),
    ("close_calls",0,"A failed sync leaves closure separately owned."),
    ("sync_calls",1,"One backend durability call; no wall-time bound."),
)))
def sync(owner):return owner.sync()

# Actual validation, source identity and remaining limits

## Source and publication relationship

Audited public implementation and experiment source:
`c5157d69712185aa8d4ad185e2e6d465dfbd7179`.
Its tree is `61d8427f225de70c86176894d034f5e8de54cab3`.
The clone ZIP SHA-256 is
`d0e52b6e7278f79c1ba13a8a22fddd4c08135eba646ab3b17a02ca512bac75aa`.
Git-index executable modes were restored from the clone's stored metadata.
No unpublished integration worktree was used or represented as tested.

The predecessor #987 reference was inspected from its exact Git objects at
`c961ff4390b4e4d8581249c47acd999bdc2aa37e`. Its POSIX Resource/Group idea is extended
inside this reference, not merged into product code. The public base already has
its own independent SQLite admission owner; that owner is used unchanged.

All proposed source, documents and local evidence live in `.reference/`. One new
`.github/workflows/reference-native-constructor.yml` runs only the named reference
branch. No existing workflow, required gate, product source, runtime dependency
lock or authorization rule is changed. No wheel, binary, object/build directory,
third-party dependency source or temporary fixture is included in the publication.

## Public bug reproduction actually executed

`reproduce_public.py` executed actual UploadJournalValidationOwner.advance with
its maintained retained journal/catalog fixtures and a known before-dispatch
refusal on the original raw creator close. It reproduced the escaping descriptor,
removed original slot and competing workspace lease admission in **0.953175392 s**.
The script explicitly closed all probe-owned descriptors and verified original
source hashes remained equal. This is `reproduced-not-qualified`, not a fix pass.
The complete result and hashes are in LOCAL_RESULT.json.

## Installed reference and Native bridge actually executed locally

`run_witness.py --native --output ...` built two independent wheels in temporary
copies of the reference package and installed each into a separate target. The
normal smoke and fault suite ran outside the checkout under `python -I`, with
only the exact installed package added to the path. The normal wheel exposes no
fault-injection API. Wheel and installed extension hashes are recorded.

The final local run passed **24 fault-wheel tests**, no errors/failures/skips,
and the separate normal-wheel smoke. Build/install/smoke command elapsed was
2.952307273 s for normal and 4.468371930 s for fault; these are observations,
not timing gates. Native compilation used `-Wall -Wextra -Werror` on Linux.

The explicit bridge then passed **10 actual Native/Core tests**, no failures,
errors or skips; its command elapsed was **11.169564173 s**. The unchanged
JournalStore schema/read-only/final-reader code, original SQLite admission and
actual Core advance/lease fixtures were exercised with reference constructor
origins. Both an unreturned connection factory and failed storage-wrapper
allocation leave their original SQLite scope receivable. Unknown creator close
keeps the actual Core slot/lease, and independent Native work still succeeds.

This bridge is a test instrument, not a completed production hard cut. It uses
ContextVar/monkeypatch routing and owner-private observation outside the neutral
package. It is not an optional production fallback, and does not qualify all
Core callbacks, the full retained-corpus adaptation, or every budget/worker path.
Unknown-resource tests use isolated child exit for fixture cleanup only; no
process exit is presented as an in-process receiving receipt. Signal tests use
explicit Python signal-delivery seams, not real kernel EINTR injection.

## Actual primitive growth evidence

The installed normal package produced **480 samples across three source-owned
acquire/receive/sync declarations**, validated by the audited maintained growth
validator. Elapsed **0.141563528 s**. Unrelated-name population and file extent
were varied independently; every positive receiving was repeated 18 times.
Native owner size was 112 bytes on this ABI. Acquisition used at most one backend
open and stat; receiving used no open/dup/stat and at most one lifetime close;
sync used at most one backend entry. Projection hash, maxima, counts and sample-
stream digest are in LOCAL_RESULT.json; growth_probe.py regenerates the full projection. The probe regenerates every sample.

These are reference native API-entry counts, not Windows CRT-internal syscall
counts, kernel latency, aggregate Python/SQLite memory or complete retirement
resource qualification. Windows regular-file samples omit the unsupported corpus
directory profile explicitly rather than claiming POSIX evidence for it.

## Runtime and dependency limits

Local scratch: Linux, CPython **3.13.5**, setuptools **82.0.1**, Pydantic 2.13.5,
SQLite from that runtime, and the available environment's dependencies. The repo
pins CPython **3.12.3** and setuptools **84.2.1**. Reference pyproject pins the latter;
local `setup.py bdist_wheel` deliberately used the available build tool and records
it rather than pretending a locked build ran.

The missing rfc8785 dependency was supplied outside the checkout from exact
upstream 0.1.4 Git blobs: `5a1f9d919643fa3bcaa0999ea66d9c535568c42a` (__init__) and
`3137d3326b98938affadb1be711ee411eb2ab86e` (_impl). Blob identities were checked.
No serializer stand-in was used; no third-party source is included in the branch.
This does not claim a locked or installed product dependency environment locally.

`make lint` was attempted; it stopped before lint execution because `mise` is
unavailable: make exit 2, prerequisite exit 127. No product lint pass is claimed.
Static checks parsed 11 reference Python files, resolved 22 symbols in six source
owners, and checked reference links/fences/newlines, whitespace and the intended
source-only delta. Exact publication parent/tree/blob checks and actual Actions
status are recorded in #988 after publication.

## Native platform workflow and incomplete acceptance

The supplied branch-only workflow uses the existing pinned checkout/mise/artifact
actions, repository Python/uv/dependency lock and native Linux/macOS/Windows hosts.
It runs the two installed wheel variants, the actual Native bridge and growth;
on POSIX it separately repeats the unchanged-source bug reproduction. Expected
Windows omissions are only the three POSIX primitive cases and two Core POSIX
lease cases, not the shared Native API. Explicit counts reject blanket skips.

A workflow definition is not execution evidence. Its exact-SHA run/job results,
failures/infrastructure blocks and artifact identities belong in the issue's
publication record. At the time this local record was written, only Linux scratch
execution had occurred. Never infer Windows/macOS qualification from that run.

No full maintained corpus integration, complete ordinary/terminal grant matrix,
product-installed distribution, power-loss durability, free-threaded CPython,
forced native cancellation, arbitrary descriptor tampering or discovery activation
is qualified here. Complete existing exact-source/tested-merge acceptance remains
integration work. Reference success does not close #988/#964 or authorize release.

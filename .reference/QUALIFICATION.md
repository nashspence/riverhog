# Finite qualification vectors

The supplied commands are reference evidence. Integrate selected behavior into the
existing maintained Native/Core, workspace, native-platform and growth gates; do
not turn a reference workflow into an alternative delivery rail.

## Reproduce and then repair the exact owner gap

At the audited source, run `reproduce_public.py` in the locked all-packages/dev
runtime. It must retain source hashes, demonstrate the live creator after slot
removal and competing lease admission, explicitly close its fixture descriptor,
and label the result `reproduced-not-qualified`. Do not count it as a passing fix.

After integration, use the same actual Core fixtures but inject at the neutral
native creator. Known before-close refusal must keep the exact original owner,
and the final positive retry must precede parent lease/capacity release. The
supplied test bridge exercises this and the unknown case on actual Core advance.

## Required vectors by boundary

| Boundary | Required observation |
| --- | --- |
| Passive frame allocation / capacity refusal | No open, mkdir, connect or validation side effect before caller retention and grants. |
| Exclusive creator; duplicate name; read-only existing bytes | Exact exclusive refusal, no fallback create/truncate/upgrade; successful readonly open cannot write. |
| Actual file acquisition, post-open fstat rejection | Original resource retained despite no returned store; reject invalid kind/private state without lost ownership. |
| Lost acquisition reply / Python signal after result | Same owner holds result, no second acquisition; no signal-delivery gap. |
| Known before-close refusal | Resource remains live/retryable; parent cannot receive until explicit positive close. |
| Lost close reply; same slot reused for unrelated file | Retrying the original owner dispatches no second close; unrelated file remains usable. |
| Same slot reused for same inode reopened | Same result; inode matching cannot authorize a second close. |
| Genuinely unknown acquisition/close, before or after effect | Keep original charge, slot and namespace; no integer retry or force receipt; independent work continues. |
| Directory acquisition/sync refusal or lost reply on POSIX | Directory remains owned; failed sync never becomes successful construction just because close later succeeds. |
| SQLite factory acquires then raises without return | Pre-retained existing factory scope retains its exact original connection. |
| Native storage-wrapper allocation fails after connect | No returned store, but original connection remains scoped and explicitly receivable. |
| Open callback or schema/source validation rejects | Original frame and partial Native owner survive; cleanup does not require successful return. |
| Final borrowed reader | Store facade close does not release its original connection or application custody early. |
| In-progress constructor/borrower and requested cancellation | Nonblocking receiving reports busy; actual original I/O exit and worker join are separately observed. |
| Parent callback raises after partial receiving | Retry the same callback/release owner; no clearing before confirmation. |
| Corpus guard and `_from_workspace` constructor errors | Preserve original guard/borrow, sync owner and existing database receiver; no missing-field success. |
| Reopen/restart | No persisted descriptor replay, replacement of original confirmed bytes or synthetic init success. |

## Platforms and packaging

`run_witness.py --native --output result.json` builds separate normal and fault
wheels, installs into distinct targets and executes the low-level suite using
`python -I` outside the checkout. It checks that the normal wheel has no test API.
The Native bridge is a separate phase using the audited workspace Native/Core
modules and the exact installed receiver, not an isolated installed product wheel.

Expected current reference cohort: 24 primitive tests on each platform, with only
three specifically POSIX directory/private-mode tests skipped on Windows. All
Windows creator/refusal/reply-loss/unknown/readonly/Unicode-path tests must run.
The 10-test Native bridge cohort has only two Core POSIX-lease tests skipped on
Windows; its eight direct JournalStore/SQLite receiving tests must run. A future
fixture addition must update the explicit cohort assertion, not remove coverage.

Use actual native Windows MSVC/dynamic UCRT and macOS, not Linux emulation or an
uncompiled conditional branch. Retain exact interpreter, compiler/build identity,
normal/fault wheel and installed extension hashes, invoked tests and skips.
The new branch-only workflow runs the repository's pinned Python/uv/dependencies
and does not alter existing CI. Full product installed-distribution and platform
acceptance still belong to integration.

## Primitive and composed growth

The source-owned acquire/receive/sync declarations validate actual native entry
counts. The probe varies unrelated names 0/17/257 and independent file extents
0/17/65,537/8,388,608 bytes, tests file and POSIX-directory profiles, and repeats
receiving 18 times. It emits the full declaration projection, counts, maxima and
full sample-stream digest; every sample is regenerated and validated.

On POSIX this is 480 samples across three declarations. Windows has 240 samples
for the regular-file profile, not fabricated directory samples. Numbers of native
entries do not bound CRT-internal/kernel work or fsync elapsed time. The retained
C object size is an observed ABI value, not aggregate allocator/process memory.

Composition must also account for caller records, original connection/cursor
receivers, failed attempts, ordinary versus terminal grants and actual worker
join. No collection-sized namespace scan, callback-prefix rescan, hidden handle
limit or archive ceiling is authorized. Exhaustion leaves the original frame
charged and explicitly refuses/defer new work; it never manufactures receiving.

## Evidence not supplied by the reference

A Python signal seam is not real kernel EINTR/forced-thread-cancel qualification.
An isolated child exit only cleans up probe-owned unknown resources. Tests do not
supply power-loss durability, new Windows private-ACL policy, arbitrary hostile
in-process FD protection, filesystem latency bounds or full maintained corpus
retirement. Those claims must not be inferred from the reference's successful
creator and receiving tests. Runtime activation remains off until existing
integration acceptance is met.

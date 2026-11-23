# Original native worker exit receiving

## Recommendation

Retain the native thread handle from creation through positive operating-system exit receiving, and record that result in the native owner before returning to Python. Core must use that one receipt for status, collection and shutdown retries. A completed Future remains the original semantic outcome; it supplies no exit authority.

The proposed shared owner lives in `retained-resource`, which Core already depends on. Linux creates a **joinable** pthread and makes at most one `pthread_tryjoin_np` call per receiving probe. The Windows draft retains the `_beginthreadex` handle and polls it with zero-timeout `WaitForSingleObject`, then receives the handle close. Native errors retain the original owner in quarantine. There is no worker reconstruction, replacement Future, thread-ID lookup or Python-sentinel fallback.

This addresses the focused [#1005 request](https://github.com/nashspence/riverhog/issues/1005) from [#994's integration blocker](https://github.com/nashspence/riverhog/issues/994#issuecomment-7100477049). It is concrete input under [#903](https://github.com/nashspence/riverhog/issues/903), not an acceptance decision or a replacement durability design.

## What the source and experiment establish

The audited `PhysicalExecutionOwner` creates a Python `Thread`, treats `is_alive() == False` as exit, invokes `join(0)`, and then removes the original slot when its Future is complete. Removal refunds global lane and per-target allowances. `close()` uses blocking Python joins outside the registry lock. This implementation is byte-identical on authoritative main, the issue's `fee80a186ae31765addfbd4b2e3f877f9eb8b286`, and the tested integration checkpoint `3d039b382ee33d66c6049dd69e498169aa75bf6d` (blob `ef161cde4353ef80401796a526702f1b2c8d7b73`).

The maintained subprocess probe executes this actual owner. It waits until the real Future is published, holds either the original target tail or a `threading.local` finalizer, observes the original native task, and sends SIGINT only after the coordinator enters its actual join frame. On unchanged source, the worker is reported dead and one outcome is returned while the native task still exists. Both capacity charges disappear prematurely. The proposal keeps the slot and charges through two interrupted shutdown retries, then receives the same outcome after explicit release and actual native exit. A separate consumer probe holds the real retirement directory phase and lease namespace through interruption. These observations are recorded separately from the injected lost-reply cases.

The source was not reset to a reference or modified on the integration branch. The reference's product paths stay on main; its patch was implemented and tested in an isolated checkout of the identified integration checkpoint.

## Why a native owner is necessary

[CPython issue #90882](https://github.com/python/cpython/issues/90882) documents the same false-dead observation: an interrupted join can release a thread-state sentinel it did not acquire. Retrying that Python join or rechecking `is_alive()` cannot recover independent native-exit evidence. The issue's later discussion identifies the changed 3.13 implementation; this does not repair the repository's pinned 3.12.3 runtime.

[CPython 3.12.3's pthread launcher](https://github.com/python/cpython/blob/v3.12.3/Python/thread_pthread.h) explicitly detaches the pthread it creates. Therefore a wrapper cannot safely attach `pthread_join` to an existing ordinary 3.12 Python Thread. The proposal must own a joinable native worker from the beginning.

A target-return event, Future callback or Python finalizer flag is earlier than native exit. Returning from a pthread start routine invokes thread exit and its thread-specific destructors; successful join is the termination observation ([pthread_exit](https://man7.org/linux/man-pages/man3/pthread_exit.3.html), [pthread_join](https://man7.org/linux/man-pages/man3/pthread_join.3.html)). The fault-native witness holds a real pthread TLS destructor after Python thread-state cleanup and native entry return. Linux polling stays pending until that destructor is explicitly released.

An upgrade or backport also needs two separate proofs: interruption-safe exit receiving **and bounded polling**. In [CPython 3.13.0's `_threadmodule.c`](https://github.com/python/cpython/blob/v3.13.0/Modules/_threadmodule.c), the timeout covers waiting for `thread_is_exiting`; that event is set before the native start routine returns. The code then calls blocking `PyThread_join_thread`. **Inference from that source ordering:** a held native TLS destructor can make the final native join block even after the event, so simply substituting that handle's `join(0)` is not a proof of this repository's zero-wait polling requirement. This packet neither benchmarks newer runtimes nor claims they are unsuitable for every use.

## Proposed ownership boundaries

| Owner | Responsibility | Authority it does not gain |
|---|---|---|
| Native `WorkerHandle` | Original process/interpreter, exact native handle, startup result, positive exit/handle receiving, unexpected bootstrap exception, primitive counters | Semantic success, resource closure, current authorization, durable cost settlement |
| Python `Worker` | Pinned runtime bootstrap identity/tracing/non-daemon bookkeeping; interruption-responsive explicit shutdown waits | Exit evidence from a Python sentinel, daemon abandonment, general Thread compatibility |
| Core `PhysicalExecutionOwner` | Admit and retain one original slot, exact receiver/target/invocation identities, bounded rotating polls, original Future and capacity charge | Replacement execution, resource disposal or durable physical bits |
| Existing resource and retirement consumers | Current fencing, exact original resources/leases, metadata transitions and subsequent cleanup | Treating file sync or closed resources as evidence of native exit |

### Native creation and receiving

Allocation performs no native thread effect. Core first inserts and charges the slot, then calls `start()`. The C owner holds the GIL across the bounded OS creation entry and records its outcome before signal checking. The new worker cannot execute Python before its native handle and self-reference are retained. An interrupted Python startup acknowledgement therefore leaves `started` true and the original charged slot present. Definite creation failure permits withdrawal; absence of a Python `ident` does not decide this.

During execution, a native self-reference roots the original callback/owner. The worker clears its callback reference and original Python thread state before native return. GC traversal allows unstarted callback cycles and received exception cycles to be collected, while a running or quarantined owner remains rooted. No destructor joins, closes or fabricates receiving.

On Linux, `pthread_tryjoin_np` returns pending without waiting for termination; success is recorded once. The [Linux manual](https://man7.org/linux/man-pages/man3/pthread_tryjoin_np.3.html) documents this GNU interface and its `EBUSY` result. All native-handle operations are serialized by the GIL on the admitted profile. The successful receipt is stored before signal checks; retry after a lost Python reply returns the retained result without rejoining a consumed pthread. Unexpected errors quarantine the owner and suppress further native probes. This deliberately preserves custody rather than interpreting absence or an error as success.

The Windows draft records thread-object signaling and handle closure separately. [Microsoft's wait API](https://learn.microsoft.com/en-us/windows/win32/api/synchapi/nf-synchapi-waitforsingleobject) documents zero-timeout polling; [thread termination](https://learn.microsoft.com/en-us/windows/win32/procthread/terminating-a-thread) documents signaling and remaining open-handle lifetime. [_beginthreadex](https://learn.microsoft.com/cpp/c-runtime-library/reference/beginthread-beginthreadex) supplies the retained handle. A numeric ID match is disambiguated through that original handle to avoid confusing a reused ID with the current worker. Wait or close uncertainty remains charged. This path has not been compiled or executed on Windows here.

### Python bootstrap and shutdown

The adapter inherits the pinned CPython 3.12 Thread bootstrap solely for normal `threading.current_thread()` identity, tracing/profile hooks, limbo/active registries and non-daemon shutdown bookkeeping. Launch, liveness and join methods use the native owner; they never invoke the vulnerable sentinel receiving path. This avoids an immortal alien-thread registry entry for every ordinary callback using thread introspection.

The adapter refuses other interpreter/minor-version profiles instead of assuming their private bootstrap fields are compatible. Startup retains the native creation result even if bootstrap acknowledgement is interrupted. An unexpected bootstrap exit is diagnosed without inventing a Future result. A worker may inspect its own liveness but cannot join or poll itself for exit receiving.

`close()` still closes admission under the registry lock and waits outside it. Each explicit blocking wait alternates zero-wait native receiving with finite interruptible waits; those waits have no receiving authority. Interruption propagates. Retrying shutdown receives the same original workers and retains their original outcomes for the proper receiver. Service shutdown must finish this receiving before interpreter finalization or closing shared dependencies. Ordinary interpreter shutdown bookkeeping is not a substitute for an explicit native receipt. The native stack uses the OS default, so this is a focused Core worker rather than a general replacement honoring every `threading.Thread` configuration surface.

### Core status, collection and consumers

`status()` and each selected collection slot perform one native receive probe. Pending work remains running; a positively received worker without the original Future remains `outcome-missing` and charged. Only a received native exit plus the original Future allows a completion. Exact receiver identity, stale invocation refusal, per-target isolation and the rotating bounded bucket are preserved.

The tests verify that independent work completes while another original is held; wrong receivers cannot consume it; original success objects and original exception objects remain identical. The real workspace lease is denied to a competing reader while the held target or TLS finalizer owns it and is obtainable after native exit. The retirement-consumer witness separately preserves the directory-phase `.lease` namespace and construction/physical capacity ledgers until actual receiving. File-resource receiving may already have happened at that point; it does not authorize early directory cleanup.

## Runtime/backend requirements and open integration decisions

| Profile | This packet's implementation | Executed qualification |
|---|---|---|
| Linux x86-64, glibc, GIL CPython 3.12 main interpreter | Joinable pthread and zero-wait tryjoin; real native TLS and SIGINT witnesses | Pinned CPython 3.12.3; exact environment and results in VALIDATION.md |
| Windows, dynamic UCRT, GIL CPython 3.12 main interpreter | Proposed original `_beginthreadex` handle, zero-wait wait, retained close | None; require native wheel, real interruption and handle-lifetime evidence |
| Darwin | Explicit refusal for Worker; existing file-resource functionality remains separate | No worker backend supplied |
| Free-threaded CPython, other Python implementations, subinterpreters, fork-transferred owners | Refuse unsupported profiles/ownership | Refusal witnesses where applicable; no execution support claim |
| Other CPython minor versions | Python bootstrap adapter refuses | Require explicit bootstrap reconciliation and bounded native-tail evidence |

Apple's [public pthread header](https://github.com/apple-oss-distributions/libpthread/blob/main/include/pthread/pthread.h) exposes joining but no public tryjoin counterpart. A possible Darwin design is a separately admitted native receiving service that performs the blocking join away from coordination and stores the original receipt. That design must prove its own helper lifetime, shutdown, resource/cost accounting and independence when one original is stuck. A single blocking reaper cannot simply stall receipt of unrelated completed workers. This packet does not introduce an unqualified helper, private Darwin SPI, `/proc`-style existence heuristic or polling fallback. The owner must resolve the supported Darwin strategy before cross-platform integration.

## Bounds and precise limits of the evidence

The shared witness observes native API entries directly, including pending and already-received retries. It varies unrelated retained originals without scanning them. There is one creation and at most one positive native exit receipt per actual worker; after receiving, retries make no further joins/waits/closes. Native owner storage is constant for the measured build. Core's existing growth witnesses retain their populations and quanta and observe actual worker start/poll counters alongside mapping work, full key widths and absence of whole-registry enumeration. No wall-clock target or claim about scheduler-internal cost is introduced.

Real SIGINT tests establish coordinator interruption behavior. Fault seams separately establish the ordering of native effects, receipt storage and Python reply loss; synthetic unknown-state tests do not pretend to reproduce an OS failure. All fault and native-tail control methods are absent from the production wheel.

The proposal does **not** add an acknowledged completion-batch delivery protocol. An arbitrary Python exception after Core removes a slot but before its caller receives the returned tuple is a distinct delivery boundary. A native exit receipt also does not create persistent recovery across process death, settle final lifecycle costs, renew a lease, authorize replay, or qualify power-loss durability. Those consumers remain subject to their owning issues and current source reconciliation. No provider, release, merge or independent scale campaign was performed.

## Suggested consumption

First reproduce the before/after worker-exit failure on the exact checkpoint. Then review the native owner and pinned bootstrap together, reconcile the small Core change with preserved integration WIP, and decide the remaining platform obligations in #1005. If material is used, record a finite adopted/partially used/independently reproduced/superseded/rejected disposition in that issue and identify the actual integration source and remaining qualification. The reference remains input; it supplies no authorization to enable the suspended durable physical-bit or cost-settlement consumer.

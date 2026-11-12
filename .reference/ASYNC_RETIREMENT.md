# Asynchronous retirement: scope, counting and receiving

Proposed focused reconciliation at the exact source in HANDOFF.md. The existing
execution, retirement, resource grants and disposal authority remain unchanged.
The source proposal is in PATCH.json/changes and the hash-checked apply recipe.

## 1. Selected return meaning and derivation

For one successful `UploadJournalValidationRetirement.advance(limit=L)` call,
let C be exact joined completions delivered by physical collection, R the ordinary
reservations actually withdrawn, S accepted close-only worker resubmissions and
N accepted new worker submissions. The existing implementation returns:

```
events = C + R + S + N
0 <= C <= L; 0 <= R <= L; 0 <= S <= L; 0 <= N <= L
events <= 4L
```

These terms are separate events, not disjoint sets of workspaces. The same
original can contribute C and R; N can be a later quantum for that workspace.
C can be an exception or type mismatch. S is admission, not completed receiving.
R can withdraw an unstarted reservation after physical-capacity refusal: it need
not correspond to a completion in that poll. For integer L<1 the return is zero;
existing validation of a positive file budget and exact integer type is retained.
An exception is not an integer return: record its executed prefix separately.

The bound follows the maintained loops, not sampled maxima. Physical collection
returns at most L exact joined results; registry withdrawal rotates at most L
contexts. Resumption visits at most L contexts. The new-work outer loop has at
most L iterations, each accepting at most one worker. Each outer iteration probes
at most three candidates; exclusion tuple widths are 0+1+2. Thus selection calls
are at most 3L and total exclusion references at most 3L. Locks can defer work;
none of these bounds requires scheduling to fill its capacity.

The 1 then 3 case is C=R=N=1, S=0. The four-event case deliberately retains a
failed original requiring a close retry, puts a joined successful original first
in the collection order, and leaves a new candidate available. The next call can
have C=R=S=N=1. It proves 4 is attainable for the existing ordering; it is not a
license to label four file mutations complete.

Retain the integer API and document it as a scheduling hint. Do not use it for
bytes removed, catalog rows deleted, completed workspaces, custody, receiving or
capacity release. A worker's actual boolean result and its file/catalog child
measurements remain the corresponding evidence. A failed fsync may follow an
actual truncation without establishing successful retirement.

## 2. Three execution scopes and a cross-scope identity link

**Coordination:** one finite scheduling call owns catalog selection, registry
collection/resumption/retained lookup/submission, physical slot operations,
ordinary retention operations, exact scope controls and typed result validation.
The registry control methods are counted as the same synchronous scope; their
actual physical/retention/catalog/callback leaves are declared delegates. They
are not assumed free because the public method forwards to a registry.

The declaration's own mapping envelope covers collection get/rotation/delete,
resume rotation, retained probes and submit get/install. The bound 24L+12 is a
conservative source envelope: at most L collection lookups, 2L collection
pop/reinsert operations, L deletes, 2L resume pop/reinsert operations, 3L retained
probes, 6L new submit lookups/installations, plus fixed entry reads. Physical and
retention mapping work has separate existing declarations and is not included
again in these own counters. No whole-registry enumeration is admitted.

**Original worker quantum:** `JournalRetirementRegistry._run` owns passive actor
construction, its one `JournalRetirementTurn.advance`, or its original close-only
retry. A Native-specific forwarding override gives this callback a distinct
source binding without falsely declaring Native children for Discovery's shared
registry. The shared implementation itself is unchanged. A turn composes the
actual `_advance` unit (catalog, Native extent, namespace), handled full error
formatting, exact blocker recording and receiving. The old inline scheduling
children are removed rather than left as a second accepted interpretation.

**Receiving:** the original actor receives file and namespace owners on its
worker. A close retry uses the same actor and never re-enters `_advance`. The
physical collector checks the original worker, performs the existing zero-wait
join, and only then delivers its result. Original resource receiving and actual
worker join are distinct prerequisites to ordinary reservation withdrawal.
Future.done alone is not sufficient, nor is a successful join sufficient for
resource receiving. Positive resource receiving does not itself withdraw the
ordinary retention reservation.

Every observed physical invocation is linked to its original context object,
reservation, exact key/scope/target, worker object and actor. A close-only retry
has a new physical invocation but the same original actor and reservation. The
witness retains these objects while inspecting identities, so recycled numeric
object/thread identities cannot substitute for them. Integer IDs in the exported
receipt are trace labels, not production authority. Observe the actual object
relations before export.

Worker samples are separate roots, never fictitious synchronous children of a
scheduler that already returned. The same worker is measured once, wherever it
runs between scheduling calls. Later result collection does not re-charge its
file effects. Test barriers and explicit test joins sit outside all three
operation scopes; they do not introduce waits into the coordinator.

## 3. Exceptional work is not omitted

`JournalRetirementTurn.advance` formats the full actual handled exception before
slicing the stored blocker to 1,000 characters. The declaration charges the full
encoded formatted representation once, not merely the stored suffix. Catalog
blocker comparison and unchanged-blocker behavior keep their existing owner.

At this source, joined exception-frame receiving happens in registry collection
when the original is closed: the current outcome and possibly one retained older
unknown outcome call `release_exception_frames`. This is an actual synchronous
receiving delegate, at most 2L calls; **its contents are not constant-size work**.
Let E be distinct exception nodes, A all cause/context/group edges (including
repeated edges), and F summed traceback chain lengths, charging shared tails per
node. The helper has O(E+A+F) logical traversal; actual clear calls/frame visits
and graph edges are observed. Cycles are deduplicated by original exception
identity. Its return does not establish resource closure.

The failed-receiving diagnostic logs the workspace/type controls, not a formatted
whole exception body; count those complete controls and calls separately. Logging
sink latency and interpreter decref/finalizer work are not silently claimed as
zero, bounded by 4L, or measured kernel work. This proposal retains the existing
source placement and declares those backend limitations rather than moving file
work inline or inventing a new execution owner. The responsive profile requires
that diagnostic sinks and retained-local finalizers do not perform blocking
workspace/resource disposal. Original native owners deliberately do not use GC
as a receiver. A different sink/finalizer profile requires its owning backend's
evidence; these logical witnesses do not qualify arbitrary handlers/destructors.

Consequently, 4L bounds the **return counter**, not all CPU/memory/latency of a
call. The actual composed cost includes bounded catalog controls/lookup terms,
full scope encoding, physical and retention primitives, diagnostic controls, and
any real exception graph receiving. Count rejected attempts as well as success.
Do not manufacture an absolute latency promise from unit counts.

## 4. Custody and capacity remain separate

Ordinary retained construction capacity is reserved before an actor is created.
Terminal physical worker capacity admits a bounded file quantum or close retry.
Retained source/namespace resources belong to the original actor. Actual worker
join releases only the physical execution slot; collection/positive receiving
and ordinary reservation withdrawal have their own phases. File/directory/SQLite
receivers and #987/#988 unknown-state rules retain their current meanings.

A refused physical submission can retain an unstarted context until collection
withdraws it; count that R honestly without inventing actor work. Unknown close
can release no ordinary reservation or namespace custody, even after its worker
joined. Resumption cannot reconstruct a failed original or treat the current
pathname/descriptor occupant as its replacement. Repeated close attempts retain
backoff and the same record. Independent admitted work can use other slots.

The isolated unknown witness uses the actual compiled backend fault build for
both unknown-before-effect and unknown-after-effect. It proves retained charge,
lease exclusion, unchanged close dispatch count on retry and independent success.
Its child process then exits to dispose test-owned resources. Process exit is
explicitly NOT an in-process receiving receipt or a withdrawn ordinary charge.

`receive_stopped()` is the existing explicit shutdown scope. It may enumerate
retained contexts and attempt local closes after original worker joins. It can
raise on unknown receiving. It is not covered by the finite normal-scheduling
root, and the normal coordinator must not invoke it to make a proof pass.

## 5. Evidence composition and total growth

Use the existing declaration/project/validate framework, including numeric
monotone child envelopes already used by the catalog and unit witnesses. Each
child sample preserves its real independent dimensions. Parent child-call caps
come from the source loops; metrics are sums over actual child invocations,
including exceptional prefixes. Componentwise input maxima only upper-bound
those numeric child envelopes, never define actual work from desired limits.

Across scheduling calls j, bookkeeping is bounded by sum(4L_j), not four times
the number of unique workspaces. Add every actual worker quantum and receiving
attempt once. Arbitrarily many blocked polls/retries do not become one useful
attempt. Existing per-quantum file admission, cold/warm SQL controls, index costs
and necessary bytes remain explicit delegates. This is neither an archive-size
ceiling nor a 1,000-member/end-to-end qualification.

The new witness preserves the prior independent catalog populations, wide 65,537
controls, large epochs, cold/warm regimes, file extents and 7/65,536-byte grants.
Busy readers/writers, three busy candidates, capacity, repeated blockers, unknown
files and missing coordinator remain cases. The only changed interpretation is
when their actual asynchronous effects are asserted. See QUALIFICATION.md.

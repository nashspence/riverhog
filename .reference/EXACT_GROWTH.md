# Exact annotation growth obligations

Research and integration input for [#969](https://github.com/nashspence/riverhog/issues/969)
under [#903](https://github.com/nashspence/riverhog/issues/903). The issue's new
**Exact growth guarantees** comment owns the requirements. This file explains
the counting model, derivation, evidence boundary and implementation consequences;
it is not a second public declaration registry. Earlier conversation is calibration.

## 1. What is being committed

The issue selects the asymptotic ceilings below **now**. Integration must express
and substantiate them at the actual operation owners; it may not choose weaker
classes and call them conforming. Inability to meet a bound is a concrete conflict
to report. A stronger implementation remains permitted. Production qualification
is still required: accepting a requirement is not evidence it is implemented.

These are worst-case bounds in a specified **primitive-work model**, including
worst-case bounds over a complete finite operation sequence. They are not expected
hash-table costs, elapsed-time targets, or a universal CPU/disk guarantee for an
unqualified external backend. No arbitrary H-dependent validation term may be
added to the native annotation-edit profile. General imported provenance and
external backend costs have distinct explicitly composed contracts (section 7).

The constants in O(...) are uniform in the independently growing totals below.
Fix the supported format/security profile and admitted resource settings before
varying totals. A representational integer limit is not an excuse to call a
linear history scan constant-time. The fixed native path/node limits are exposed
for byte accounting, not new archive/count limits or configurable escape hatches.

The vector separates:

- **byte work**: octets read, copied, hashed, serialized or compared by the
  annotation owner, with a bounded number of passes; index comparison bytes are
  separately included below;
- **logical work**: state/object/relationship visits and ordered-index operations;
- **index work**: key comparisons and comparison octets, not merely SQL statements;
- **space**: live preserved bytes/objects, bookkeeping and bounded working memory;
- **effects**: per-copy bytes/objects, provider requests, and retry/restart work.

A source comment, SELECT ... LIMIT, a small response, or an expected dictionary
lookup is not a proof of these worst-case bounds. Required work in a called helper
is charged at the caller's multiplicity. Finite tests challenge claims; the
operation/loop argument and qualified backend evidence establish their scope.

## 2. Domains and notation

All sizes count actual UTF-8/canonical octets, not character counts. A control
constant `c` is a fixed per-event/checkpoint envelope allowance, not historical
input. Let `ell(z) = ceil(log2(2+z))`.

| Symbol | Independent meaning |
|---|---|
| H, Ht | successful state changes after initial establishment; tag-changing subset |
| A, At | admitted request attempts, including no-op, rejected and repeated requests; tag-operation subset |
| N, M | source member count; explicitly requested independently portable outputs |
| T0, B0 | accepted initial tags; genuine initial annotation/attribution value octets |
| b_i | genuine request/actor/context values needed for one edit, excluding authorization grants and machine-generated repetition |
| Ba, Br | sum of b_i for committed changes; sum for all admitted attempts |
| D, U | fixed format path bound and maximum tag-node envelope; native D=33, U=131072 are proof parameters, not required read counts |
| G_i, Bg_i | applicable authorization records and their total bytes processed for one check |
| C_i | copies enumerated/scheduled at the stated edit boundary; per-copy transfer is additional |
| Z, w | actual relevant indexed population; maximum compared key width in bytes |
| K, E, S, B | distinct required evidence/structural objects, required relationship occurrences, exact semantic selections, and union of selected evidence bytes |
| delta_Hc, delta_Kc, delta_Ec, delta_Bc | missing suffix/checkpoints, objects, relationships and bytes at one verified copy |
| Qg, Eg | settled cleanup candidates and intrinsically affected direct ownership edges |
| F; qb, qv, qi | interruptions of the same retained work; maximum uncommitted byte, logical-visit and index-operation budgets |
| O | actual requested output bytes; never an artificially expanded internal work relation |

`K/E/S/B` are semantic selections and their necessary representation, not "the
number of operations the implementation happened to execute". Multiple members,
exact prefixes, interpretation profiles or roles cannot be erased to reduce S/E.
Duplicate roots/edges still incur their required selection checks. Copy/read
receipts and retry ledgers have separately counted ownership and byte populations.

The source-derived tag path is bounded because keys are 32-byte SHA-256 digests;
root plus nonempty progressing prefixes gives at most 33 path nodes. Rebuilding
or deletion normalization changes only a constant multiple of that path; a
normalization may also load a sibling. **One operation is not one node access.**
Encoding, hashing and copying any such node can cost up to U bytes. Two operations
with equal tag counts but different existing leaf sizes can have different costs.

## 3. Native edit, initialization and cumulative contract

The native profile is the server-generated initial establishment and narrow
add/remove/replace/clear stream in #969. A point edit introduces a bounded set of
new metadata-update/recordkeeping assertions and exact references to established
Context/Agent identity; fields are schema-bound. It does not accept an arbitrary
user-authored graph, retraction program or new schema as a tag/description operand.
This defines the new operation's existing intended scope; it does not narrow the
core journal language or imported member provenance.

All required checks for this generated profile are included in the bounds:
identity/operation uniqueness, exact predecessor and prefix, actor/context
resolution, field preconditions, canonical/profile validity and state transition.
An implementation must use a semantically equivalent incremental validator or
prove that its chosen general validator meets the same profile bound. It cannot
append first and defer a required rejection, omit identity checks, or hide an H
scan as G22/general validation. Tag removal is not a canonical journal correction.

For one admitted successful edit with `d=1` for a tag change and `d=0` for prose:

```
annotation byte work             O(b_i + c + d*D*U)
annotation node/record visits    O(1 + d*D)
annotation index operations      O(1 + d*D)
new preserved objects            O(1 + d*D)
new preserved bytes              O(b_i + c + d*D*U)
```

Index operations are not magically constant-time. Under a qualified ordered-index
profile, Q operations require **O(Q*ell(Z)) key comparisons** and at most
**O(Q*w*ell(Z)) comparison octets**, with row values charged to byte work. If an
operation needs several population domains, use the sum of the actual per-index
terms; Z=max(domains) is a conservative simplification. No whole-history, full
current-tag-set, or member traversal is permitted inside this budget, including
O(sqrt(H)) or O(log(H)^2) extra record visits. Logarithmic *index comparisons* do
not authorize logarithmically many unrelated provenance records.

For the same edit's current access and copy-scheduling boundary, add:

```
authorization/copy byte work      O(Bg_i + c*(1+C_i))
authorization/copy index ops      O(1 + G_i + C_i)
```

Thus the ordinary server boundary is not independent of the number of grants or
copies. Grant values are parsed/hashed a bounded number of times, and current tag
membership probes are driven by granted labels, not an all-T membership scan.
Actual authentication, backend and provider profiles still compose as section 7
requires. Queue/lock/network waiting is not a bounded latency promise.

Initial establishment has a separate one-time construction bound:

```
byte work                        O(B0 + c + D*U*T0)
node/record visits and index ops O(1 + D*T0)
settled initial preserved bytes O(B0 + c*(1+T0))
settled initial objects          O(1+T0)
```

The last two are achievable with the native compressed fixed-digest tree;
intermediate construction nodes and their bounded cleanup are charged in the
first two. Do not rerun this construction as part of each edit. Canonical ordering
must fit these logical-visit bounds and the explicit comparison/index envelope;
there is no new permission to sort an entire tag set on an ordinary edit.

For H successful changes, **a worst-case finite-sequence bound, not an amortized
per-call claim**:

```
annotation byte work             O(B0 + Ba + c*(1+H) + D*U*(T0+Ht))
annotation visits/index ops      O(1 + H + D*(T0+Ht))
```

Per-call index costs sum at each operation's population; using maximum Z gives
O((1+H+D*(T0+Ht))*ell(Z)) comparisons. Add every actual grant/copy check at its
multiplicity. This prohibits quadratic prefix/hash/directory/snapshot work.

For A admitted attempts use **Br, A, At**, not Ba, H, Ht, in the attempt-work
formula. No-op/replay/rejection outcomes do not produce fictional history. A
retry lookup may read its exact original bounded event/receipt; it may not replay
all intervening history. Failed/new-copy/full-repair work has its distinct admitted
boundary below, not an ordinary-edit label. Count all failed prefixes/effects.
No bound on eventual success exists under infinitely repeated conflicts/outages.

## 4. Retained space contract

After settled construction/cleanup, one logical annotation stream must satisfy:

```
preserved objects O(1 + T0 + H + D*Ht)
preserved bytes   O(B0 + Ba + c*(1+T0+H) + D*U*Ht)
```

This is bounded against authored input, not only against stored bytes that an
implementation could inflate. In particular the newly authored journal length
must be O(B0+Ba+c*(1+H)); it cannot contain historical prefixes, a cumulative
segment directory or all current tags per edit. The initial set is represented
through bounded structure, not a giant event. Repeated genuine values and all
historical actions remain recoverable; content sharing is allowed, not required
to claim an information-theoretic storage optimum.

Indexes/liveness state require at most a constant number of bounded-key/value
records per retained object, event and required ownership edge. Retained request
receipts add O(A_retained) records and their actual needed request/receipt bytes;
A_retained may greatly exceed H. Preserve the governing replay lifetime rather
than pruning obligations to improve the bound. C physical copies incur their
actual replicated bytes/receipts; do not claim copy-independent physical space.

Peak space = settled evidence + bounded indexed bookkeeping + **explicit live
attempt/staging/cleanup bytes + operation scratch/output**. Each point-edit
attempt can stage only its bounded delta, not a full archive/history clone.
Backlogs may consume space and trigger explicit admission refusal; they cannot be
ignored, truncated or renamed as disposable evidence. Whole-collection deletion
remains separately authorized.

## 5. Recovery, selection and composition contract

For cold complete annotation recovery through a fixed selected checkpoint:

```
byte work                 O(B + D*U*Ht + O)
logical visits/index ops  O(1 + K + E + S + H + D*Ht)
```

Here B includes initial/tag/description/checkpoint bytes and exact journal bytes
selected by this operation. Add the explicit comparison bound for those index
operations. Multiple exact prefix anchors share a forward byte pass; feeding
all journal octets to the flat prefix hasher totals **J bytes**, with O(H+S)
bounded digest finalizations, not sum-of-prefix lengths. Object/content hashes
are additional bounded passes already included in O(B). No history identity
changes and no trusting an imported hash-state shortcut.

The initial tag tree is verified once, followed by each exact path-local change;
unchanged historical subtrees are not re-expanded at every checkpoint. Required
historical stored nodes must actually exist and match. Regenerating missing nodes
is not successful recovery. The default readable output includes initial state,
changes and selected current state; all tag listings at all revisions are a
separately requested output expansion.

A current-state-only read costs selected current-value bytes plus state selection
and point/stream traversal, with no H scan, but does not claim full-history
validation. History paging fixes a checkpoint: an explicitly charged one-time
cold ordering/index build may cost the cold traversal above. Subsequent pages
cost their selected records/bytes and indexed seeks; each page must not restart
from genesis. No logarithmic arbitrary historical-ancestor lookup guarantee is
introduced; a cold point lookup on a bare chain may cost its stated distance.

For shared structural recovery/import add every necessary membership/selection
relationship to E/S and visit distinct evidence once. For N members with shared
annotation history the annotation contribution is the above bound plus O(N)
association work, **not N complete annotation validations or N*H expansion**.
Physical sharing never collapses exact source, prefix, extent, profile or member
identity. General imported graph semantics have the separately qualified costs
in section 7, at their true multiplicity, not per convenient duplicate caller. The new annotation closure may not multiply
those existing semantic-validation obligations by N merely because N members
select the same exact context.

For M explicitly standalone outputs in one operation, reusable source verification
has the union-of-selections cost; add O(M) package setup/bridge records, necessary
per-member membership-proof work and **O(output_bytes)**. Each output's actual
proof bytes are included. This does not promise constant-size membership proofs
or omit original member-provenance validation. Separately invoked independent
verifiers legitimately pay their own package input cost. M uncompressed packages
containing the same B_shared bytes write M*B_shared bytes; ordinary shared recovery
must not incur that expansion without the corresponding output request.

## 6. Replication, cleanup, memory and restart

For one retained copy with an exact trusted matching ancestor/closure receipt:

```
catch-up byte work         O(delta_Bc + c*(1+delta_Hc))
visits/index ops           O(1+delta_Hc+delta_Kc+delta_Ec)
logical transfer objects   O(delta_Kc + delta_Hc + 1)
```

Transfer the exact missing closure, including needed journal/snapshot dependencies,
then commit the selector. A caught-up check has no H scan. A fresh copy, explicit
integrity repair, or lost/untrusted receipt requires the separately counted full
selected closure (and canonical validation when not already trusted). Sum across
copies. Each serialized byte may cross a declared bounded transfer unit; count
actual chunks, provider calls, encryption inputs and failure retries. A receipt
cannot justify skipping unverified source bytes. Divergence fails, not merge by
largest revision or timestamp. These are maintenance boundaries; they do not
promise sublinear detection of arbitrary corruption in an unverified old copy.

For Qg settled cleanup candidates and Eg **intrinsically affected direct ownership
edges**, cleanup visits/index ops are O(1+Qg+Eg), with explicit indexed comparison
and actual provider-effect costs. Byte work is at most affected metadata/deleted
object bytes when a verified read is required. Work performed merely to locate
candidates is included; rescanning live history for every candidate is forbidden.
Eligibility rechecks caused by changed ownership count as distinct recorded
attempts, not free progress. Establishing/maintaining liveness is charged to the
operations that create/change ownership, not hidden in later garbage collection.
A real last-owner deletion traverses its K/E closure once with the same bound.

With fixed concurrency, working memory for an annotation step is
**O(qb + qv*w + D*U + cache_budget)**, independent of H, T0 and N. Disk-backed
frontiers/indices may grow O(K+E+S+H) records plus their required bytes; that space
and all reads/writes are charged. A high-degree adjacency cannot be one
uninterruptible "node visit". Output materialization is output storage, not RAM
silently excluded from a claimed in-memory operation. Admission/authentication
input buffers, a materialized principal/grant set, and concurrently active delegate
workspaces add their separately declared RAM; the annotation-worker bound is not
a claim of end-to-end memory independent of G or Bg.

With **intact trusted progress and source/selection fences**, F interruptions add
at most O(F*qb) repeated byte work, O(F*qv) logical visits and O(F*(1+qi)) index
operations, plus the declared small restoration/authentication check per reopen.
The budgets cover actual maximum unfinished framing, validation, effect and undo
atoms; bytes and records have different units. q includes a necessary bounded
atomic frame/node/provider segment. Index comparisons still pay ell(Z). Preserve
measured failed/undo work even when it rolls back; committed counters alone are
insufficient. A stale or untrusted checkpoint never earns this fast path.

Loss of trusted state permits an explicitly identified full rebuild with the cold
bound, not repeated ordinary-restart reclassification. These guarantees begin at
the annotation owner's stable canonical input boundary; upstream adapter export
has its own source-revalidation/restart contract, which remains part of any
claimed end-to-end composition. No zero-cost provider integrity check is invented.

## 7. Complete accounting without a delegate escape hatch

Native generated annotation validation is included in sections 3 and 5. This
scope is materially narrower than arbitrary imported member journals containing
online graph insertions/corrections, user profiles or query programs. Those retain
their accepted language and #964's named operation/profile bounds. Preserve every
original validity/rejection point. Do not weaken core expressiveness to simplify
this new stream or charge arbitrary general-graph work to every native tag edit.

A whole operation's cost is the sum of the above vector and each **named actual
reachable delegate bound at its declared input dimensions and invocation count**.
Authentication, schema/Unicode/canonical handling, crypto parameters/object count,
backend key/row/version/page maintenance, filesystem calls and provider transfer
are not an unnamed `validation_cost` or `whatever_happened` dimension. For the
fixed native annotation profile, byte/schema work must fit the stated envelope;
a current library choice is not an exception to it. General profile costs cannot
be smuggled into that fixed profile merely by calling a general interpreter.

A complete CPU/physical-I/O claim requires all reached profiles to be qualified;
absent/contradictory/unbounded delegates mean **unestablished**, not a pass. An
ordered-index comparison ceiling does not itself prove SQLite/PostgreSQL planner,
MVCC, page-split, fsync or wall-clock behavior. Every such claim names its backend
conditions and lifecycle, rather than assuming SQL guarantees O(log Z). Interface
operation counts and abstract comparisons remain requirements independently.

No broad O(sqrt H), H*log H rescans, online-graph correction allowance or growing
schema bound may be added to **ordinary generated annotation edits**. Batch/rebuild,
real corrections in imported journals, full integrity repair and deliberate output
expansion retain their explicitly different scopes. Stronger classes can be
implemented; weaker classes require a new specific issue decision.

## 8. Source argument, research and acceptance

Audited main remains `9a485bc174b7b8d703ac05159e3df9be4f36be98`; the reference extends
`ce0d34fa8da9e423adcf681c30f518d5949042f8` without rewriting its original ancestry.
It does not import main changes into the reference. Source inspection establishes:

- The native binary tag operations follow digest paths and rebuild affected nodes.
  A removal may copy an old surviving terminal tag; operand length alone cannot
  bound touched bytes. The D*U term repairs that omission.
- `collection_access.py::require_collection_access`, `tag_hashes` and
  `Principal.allows` examine grant data. One blanket constant authorization lookup
  was not defensible. Grant and copy multiplicity are explicit above.
- The core `activity` validator checks references/details without requiring every
  metadata update to insert a causality edge. The generated annotation profile can
  preserve its semantics through local indexed checks. The existing whole-prefix
  append API is not evidence of such an implementation.
- `riverhog_protocol.growth` on audited main fixes projected mode to worst-case.
  A finite-sequence worst-case declaration is valid at that scope; do not label it
  an amortized per-call bound. Use the existing dimensional model and actual
  Closure/Audit/witness consumers, not these diagnostic functions as a new registry.

Primary research basis (retrieved 2026-10-13):
[Python hash update/copy](https://docs.python.org/3.12/library/hashlib.html#hash-objects),
[RFC 6234](https://www.rfc-editor.org/rfc/rfc6234.html#section-6.2),
[RFC 8785](https://www.rfc-editor.org/rfc/rfc8785.html#section-3.2.3), and
[SQLite query planning](https://www.sqlite.org/queryplanner.html).
These support exact hash continuation, canonical-order obligations and why
lookup count, comparisons, scans and sorting cannot be conflated. They do not
qualify Riverhog's runtime. Existing decisions:
[#964 cost model](https://github.com/nashspence/riverhog/issues/964#issuecomment-6111028968)
and the current #963/AGENTS advisory-timing policy remain unchanged.

The new tests check reference arithmetic domains, independent grant/copy/attempt
and output dimensions, a source-pinned native path/byte counterexample, exact
prefix identity/count parity, and the generated-profile shape. They are diagnostic
and cannot prove an asymptotic theorem by finite samples or qualify production.
The proof obligations are path-length/byte bounds, local semantic induction,
independent authored-input storage bounds, one-pass prefix hashing, distinct
qualified selection traversal and compositional attempt/restart accounting.

Integration must bind the exact ceilings above to real operations/metrics and
make the real positive and negative witnesses enforce them. Freeze cannot pass
on undocumented scopes or a renamed delegate. The existing ontology, historical
preservation, source-retirement, security and full installed/backend/Actions
acceptance in #969 still applies. No implementation is approved merely because
this issue now chooses a precise asymptotic class. No further general research
phase, new format identity or mandatory completion of all #964 is introduced.

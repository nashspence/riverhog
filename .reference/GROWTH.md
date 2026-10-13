# Collection annotation history: growth design and evidence

Non-authoritative design input for [#969](https://github.com/nashspence/riverhog/issues/969)
under [#903](https://github.com/nashspence/riverhog/issues/903). The owning issue and
later explicit decisions control; this is not a second normative registry.

Read sections 1-3 before selecting the stored representation. Then use the
operation table and the first integration slice in section 9. Concrete diagnostic
results and limitations are in [growth/VALIDATION.md](growth/VALIDATION.md).

## 1. Reconciled source and decisions

The original reference is `a493d050ed486cb8bbb8193bb04e18d550216c75`, based on
`e43df5b5acce5d730c28b48b084e87445fbaac3b`. This supplement is a descendant of that
reference, not a reset/rebase or a merge to main. Its fresh source audit is
`main@9a485bc174b7b8d703ac05159e3df9be4f36be98`.

The [four-commit comparison](https://github.com/nashspence/riverhog/compare/e43df5b5acce5d730c28b48b084e87445fbaac3b...9a485bc174b7b8d703ac05159e3df9be4f36be98)
changes growth declaration/evidence and associated documentation machinery, not
the tag kernel, journal validator, archive structure or recovery code studied
here. No corresponding G22 implementation is established by those four commits.

Reuse the now-present source owners:

- [`riverhog_protocol/growth.py`](https://github.com/nashspence/riverhog/blob/9a485bc174b7b8d703ac05159e3df9be4f36be98/packages/riverhog-protocol/src/riverhog_protocol/growth.py): dimensionally checked declarations, primitive metrics and delegate expressions.
- [`source_hashing.py`](https://github.com/nashspence/riverhog/blob/9a485bc174b7b8d703ac05159e3df9be4f36be98/packages/riverhog-client/src/riverhog_client/source_hashing.py): actual operation-attached example, including empty chunks, scope and omitted delegates.
- [`scripts/growth_contract.py`](https://github.com/nashspence/riverhog/blob/9a485bc174b7b8d703ac05159e3df9be4f36be98/scripts/growth_contract.py), `validation_witnesses.py`, and `validation_evidence.py`: discovered source binding and actual execution evidence. Source-only association is not execution.

The [#964 cost decision](https://github.com/nashspence/riverhog/issues/964#issuecomment-6111028968)
requires useful traversal plus explicit index, key-byte, schema and delegate
costs. Its [G22 design](https://github.com/nashspence/riverhog/blob/e35bddf8f1bb72ca3f0046b597d5966cd5a08312/.reference/g22/DESIGN.md)
and [algorithm profiles](https://github.com/nashspence/riverhog/blob/e35bddf8f1bb72ca3f0046b597d5966cd5a08312/.reference/g22/ALGORITHMS.md)
preserve exact prefix validity and distinguish static, online and correction
costs. G23/G24 cover corpus/selected history; G29 covers recovery. Reconcile
these against integrated source; do not mechanically apply historical patches.

Current `AGENTS.md` keeps timing advisory. Old reference language about needing
approval merely to miss a numerical target is not a new gate. Missing required
correctness/growth evidence remains a failure. This supplement creates no
numerical performance target, release dependency or provider authorization.

## 2. Findings that change the engineering recommendation

### Fixed digest paths, not human-text trie paths

The [native tag implementation](https://github.com/nashspence/riverhog/blob/9a485bc174b7b8d703ac05159e3df9be4f36be98/packages/riverhog-protocol/src/riverhog_protocol/collection_tags.py)
uses `sha256(tag_utf8).digest()` as a 32-byte radix key. Human text is retained
at a terminal node. A common text prefix does not imply a common radix prefix.
Long strings increase validation/encoding/hash/value bytes, not key depth.

For a valid tree, the root may consume zero key bytes but every descended child
consumes at least one. Thus a search path visits at most 33 nodes, with at most
256 children per node. Mutation rebuilds that path; deletion normalization may
read one additional child per rebuilt ancestor. These are source-derived
fixed-key structural bounds, not a probabilistic O(log tag_count) assertion.
Digest collisions retain the existing rejection semantics.

Keep this exact persistent structure. Count actual node encoding, child-array
processing, value bytes, Unicode validation and store lookups separately. A
conservative path-copy allowance uses D=33 and the existing 128-KiB node envelope,
not an invented total tag ceiling. In particular, do not replace it with a
text-key trie to address a complexity problem the current code does not have.

### The old reference is a counterexample, not a production algorithm

`annotation_reference.Copy.recover` recomputes each complete prefix digest;
`prepare` and `publish` invoke recovery. `apply` copies/sorts the current tag set.
These were deliberate small-fixture shortcuts. The production journal `_append`
also validates supplied and resulting full buffers; repeated use belongs to
G22's materialized-append cost profile, not an incremental writer.

Let b_i be new bytes at checkpoint i and B_i=sum_{j<=i} b_j. Rehashing each prefix
costs sum_i B_i. Repeating that recovery after each edit costs
sum_k sum_{i<=k} B_i. With fixed-size diagnostic records these are quadratic and
cubic respectively. These formulas describe specific work patterns; they do not
claim that an unimplemented production annotation path currently has that cost.
The new harness measures the patterns, not every call in the old model.

Other traps are independent: storing an ever-growing segment/ancestor array in
every checkpoint, serializing a whole unchanged tag set per edit, validating all
historical tag trees from scratch, or treating the expanded member-by-history
product as necessary input. Bounded individual objects do not fix these totals.

## 3. Selected representation direction

**Append-stable canonical octets; bounded immutable extent descriptors; native
persistent tag roots; immutable changed descriptions; one publication selector.**
This realizes #969 without changing its ontology or canonical journal identity.

Each checkpoint names its exact predecessor, the new journal extent, the exact
flat journal-prefix anchor and current typed state selections. Extents can be
segmented/paged under existing bounds. A checkpoint must not repeat the complete
segment directory, journal prefix, ancestry or tag-set contents. A large initial
tag set is a bounded structural selection, not one giant journal entry. Initial
staging insertions are construction work, not invented historical annotations.

A simple backward checkpoint chain is sufficient for complete history and normal
suffix catch-up. Independent recovery can spool its order once and read forward.
A derived ordinal/offset index can be rebuilt once for paging. No new Merkle
accumulator, skip-list commitment or logarithmic arbitrary-ancestry promise is
required. An optional future structural index must not redefine canonical
`prefix_sha256` or source evidence.

Retain tag roots with immutable path-shared nodes; only changed description data
needs a new snapshot. Checkpoints reuse the unchanged field's selection. Every
committed historical state remains recoverable, even when its current value is
removed. Compaction may reorganize physical packing only while preserving exact
logical records/identities and dependency closure; it may not erase old events.

**Alternatives not selected:** full snapshots per edit; fresh independent journals
merely to avoid implementing continuation; periodic checkpoints that drop prior
history; a third best-effort audit sidecar; or changing a flat hash to a digest of
digests. A delta-only state representation could be correct, but would introduce
new reconstruction/random-access obligations when native persistent snapshots
already fit. An integration conflict must be specific, not a reason to restart a
general ontology study.

### Exact hash continuation and trust

For a cold selected history, one running SHA-256 context can verify every exact
prefix anchor in forward order: feed each octet once and finalize a copy at each
checkpoint. Finalization adds bounded SHA block work per checkpoint; it does not
rehash the prefix. [RFC 6234 sections 4.1 and 6.2](https://www.rfc-editor.org/rfc/rfc6234.html#section-6.2)
and [Python's hash update/copy semantics](https://docs.python.org/3.12/library/hashlib.html#hash-objects)
provide the primitives. [RFC 8785](https://www.rfc-editor.org/rfc/rfc8785.html)
remains the byte-canonicalization basis; do not reserialize historical frames.

A hot writer or ordinary restart needs G22's reviewed, privately persisted hash
continuation AND semantic-validation progress, tied to the same source, prefix,
operation and publication fence. A digest alone cannot resume the unpadded hash.
`hashlib.copy()` is useful in process, not an exposed durable state codec.
The existing G22 diagnostic is not a production backend selection.

A shape-valid imported continuation is not proof of the preceding bytes. Never
accept an untrusted archive-supplied hash/semantic state as a substitute for cold
validation. Lost or untrusted owner state requires an explicitly costed rebuild;
ordinary restart with intact trusted state must resume bounded work instead of
silently rehashing everything. Snapshot/restore offset, state version, residual
bytes, source identity and semantic watermark must commit together. Preserve all
accepted-prefix rejection points; no later correction may repair an earlier
invalid prefix retroactively.

## 4. Cost dimensions and operation boundaries

Use independent input/output dimensions, not a single collection-size number:

| Symbol | Meaning |
|---|---|
| H, H_tag | committed checkpoints and tag-changing checkpoints |
| A, B_req | admitted attempts, including retries/no-ops/failures, and request bytes |
| N, M | collection members and explicitly requested standalone outputs |
| T0, B0 | initial tag count and exact initial state bytes |
| L_i, B_J, B_D | edit operand bytes, distinct journal bytes, distinct description bytes |
| K, E, B | distinct selected structural/evidence objects, required relationships, exact selected bytes |
| C, delta_H_c, delta_B_c | retained copies and each copy's missing committed suffix/bytes |
| F, q | interruptions and admitted replay/transaction work budget |
| Z | independently defined indexed population, with key-byte costs |
| O | requested output octets, excluding avoidable internal expansion |

K/E/B are useful traversal domains, not an excuse for a wasteful representation.
State storage must also be bounded against authored input: approximately
O(H + T0 + D*H_tag) structural objects, and
O(B_J + B_D + B0 + U*D*H_tag + H*checkpoint_bytes), before distinct inherited
source evidence, indexing and explicitly retained attempt state. U is the native
node envelope. This is a conservative design allowance, not an adopted numeric
contract or evidence that every node is written at maximum size. Avoid inflating
B by storing all complete historical prefixes and then calling traversal linear.

| Operation/profile | Required useful-work direction and qualifications |
|---|---|
| Initial establishment | Read accepted initial values; construct/verify one exact native tag tree. Account for construction nodes and their cleanup. No member or payload scan solely to annotate an already fixed root. |
| Successful edit | New journal/state/checkpoint bytes plus affected native path; no scan of H, N or all current tags. Indexed operation lookup and common-predecessor serialization remain explicit. |
| Retry/no-op/rejection | Charge A and B_req, authorization, identity lookup and actually executed validation/effects. A no-op is not a fake journal event. Retained replay obligations are not free or part of H by definition. |
| Cold history recovery | Useful octet/object/edge traversal once, plus exact native replay, source-reference and G22/G23 validation costs. Multiple anchors of the same journal must not trigger repeated whole-prefix hashing. |
| Current-state-only read | Read selected current state without a history scan where the contract permits. Report unvalidated/omitted history honestly; this is not full-history recovery. |
| Replica catch-up | Walk the missing suffix to an already verified matching ancestor, transfer distinct missing dependencies, publish only after closure. Charge delta_H_c/delta_B_c and exact receipt/index checks. |
| Fresh copy / lost receipt | Full selected closure or explicit verification/rebuild; an untrusted claimed ancestor cannot justify skipping bytes. Physical replicas incur copy-specific costs. |
| Shared recovery/inheritance | Distinct selected evidence plus every required scope/membership edge. Do not expand common collection history once per member. |
| Standalone composition | Selection/verification work plus actual separate outputs. M independent uncompressed packages each including B_shared octets necessarily write M*B_shared octets in that chosen representation. |
| Reclamation/deletion | Reclaim candidate/orphan work incrementally with settled ownership. Whole deletion costs the actually owned object/edge set; no full-history mark for every object. |

For SQL-backed indices, add the owning lookup/update cost, typically an explicit
log(2+Z) factor under the declared key/plan assumptions, rather than claiming
strict total linearity. [SQLite's planner documentation](https://www.sqlite.org/queryplanner.html)
distinguishes scans, indexed searches and sorting/covering-index costs; a LIMIT
alone is not evidence of a bounded scan. Charge canonicalization, Unicode/profile
validation, graph acceptance, crypto, filesystem and provider delegates explicitly.
Do not claim a universal linear evaluator for arbitrary schemas or online graphs.

The new shared declaration model currently projects `mode: worst-case`. A scoped
cumulative-sequence bound is not a per-edit worst-case latency bound; an amortized
algorithm must not be mislabeled. Reuse/extend the owning model only when required,
with review, rather than adding a second declaration schema in this reference.
These candidate formulas acquire authority through actual operation declarations
and their reviewed conditions/witnesses, not through this table.

## 5. Algorithms at the difficult seams

### Historical state verification without all-snapshot expansion

Cold-validate the initial tag tree once. Replay each exact tag mutation against
its verified predecessor using the native persistent-tree algorithm, then compare
the resulting root identity with the checkpoint's declared root. Check required
historical nodes against retained exact bytes; do not regenerate a missing stored
dependency silently. Description transitions compare the affected exact value and
selection. A description-only edit reuses the tag selection.

This induction establishes all selected states without enumerating every complete
tag set at every checkpoint. Source object decoding and visited-node caches need
root/scope qualification and bounded/disk-backed lifetime. Physical hash equality
does not remove the need to validate each semantic relationship.

Default readable history should show the initial state, changes and selected
current state. A full tag listing at every historical revision is a separate
output-expanding request, costed by sum_i T_i and output bytes. Do not impose that
expansion on ordinary recovery simply to print a convenient table.

### Shared and inherited dependency graphs

Use a disk-backed worklist/DFS state, indexed membership and per-adjacency cursor.
Each unique selected object is consumed once; each required relationship is
checked. Track visiting versus completed nodes so a cycle is rejected rather
than mistaken for already verified shared work. A high-degree node is several
bounded steps, not a single uninterruptible loop.

Validation keys must retain source qualification, exact snapshot/prefix, requested
extent and interpretation contract. Deduplicating physical bytes is different
from merging semantic selections. Multiple prefix anchors can share one byte scan
while still validating their exact boundaries; never promote a shorter selected
history to a later tail. Do not collapse distinct member IDs because payload
hashes match. General corpus/correction validation retains its separately stated
G22/G23/G24 costs.

Normal collection recovery/import keeps a shared context selection and explicit
member associations, roughly shared evidence plus N associations, not N copies of
H annotation events. Standalone output deliberately breaks that physical sharing.
Verify reusable source evidence once per trusted run and account for output
copying separately; independent verification of each detached package is still
allowed to cost its own bytes. No claim is made against arbitrary compression's
information-theoretic limit.

### Replicas, publication and restart

A copy's receipt must establish the committed predecessor and complete dependency
closure. Catch-up can stop at that exact known checkpoint; a larger revision
number or equal timestamp is not ancestry. A fresh/untrusted copy cannot use the
shortcut. Scope physical receipt sets by storage incarnation and copy ownership.
Index confirmed objects so shared nodes need not be enumerated through every
historical root. Restarts reuse the same exact attempt and suffix frontier.

A selector remains one small commit point. Count immutable writes, encryption,
provider attempts and object calls; many tiny objects can dominate even with
linear byte growth. Grouping writes or using bounded immutable segments may
reduce calls, but must preserve exact logical identities, crash fences and
provider opacity. Provider waiting is not a local instruction-count guarantee.

Commit progress and ownership atomically at each bounded work unit. With intact
trusted state, extra replay is bounded by F*q useful units plus declared restart
index/validation work; count all failed attempts, undo and cleanup. Durable-state
loss is a distinct rebuild, not a cache miss hidden in an ordinary restart claim.

### Garbage collection and retained history

All committed ancestry remains live until its owning destructive disposition.
A recent checkpoint does not make earlier tag nodes, descriptions or journal
extents obsolete. Index committed ownership as work is accepted; maintain
explicit orphan/construction candidates and settled attempt fences. Do not
recompute reachability over all H checkpoints for each candidate or edit.

A once-per-copy/estate mark-and-sweep can be an explicitly costed repair operation,
not the normal per-mutation path. Sharing across copies/collections needs the
actual physical ownership reference rules. Collection deletion uses bounded
object/edge traversal with restart cursors. Counting database rows alone omits
provider deletion calls, directory work, vacuum/compaction and retained tombstones.

## 6. Executable evidence and what it proves

[probe.py](growth/probe.py), [test_growth.py](growth/test_growth.py), and
[results.json](growth/results.json) separate five diagnostic subjects:

1. Fixed-width repeated-prefix hashing versus native incremental SHA-256,
   preserving every digest. This is a counterexample pattern, not a production
   journal or an instrumented replay of the old model's complete call graph.
2. Exact hash-pinned native radix code (AST slice before the Pydantic head wrapper).
   No canonical JSON stub, replacement hash or new trie is used. Pure node-store
   calls/bytes are observed; whole-module/service conformance is not established.
3. Initial native tag validation plus exact historical replay versus repeated full
   snapshot walks. Missing historical nodes and wrong roots are negative controls.
4. Real SQLite-spooled structural DFS with per-edge progress, duplicate roots,
   deep/high-degree/diamond graphs, cycles, missing dependencies and reopen/exit.
   Integer fixture identities and trusted scratch are not SourceBindingProof,
   server authorization, cryptographic storage or a power-loss qualification.
5. Authenticated toy checkpoint suffix discovery, plus reuse of the exact existing
   G22 SHA continuation diagnostic against native hashlib. Neither is a new
   production replication implementation or approved cryptographic backend.

The observations do not test end-to-end replica publication, encrypted storage,
actual inherited member proofs, resource peaks, PostgreSQL plans or all core
semantic prefixes. The old 26 semantic-model tests are unchanged and their prior
results remain historical, not newly executed production evidence.

## 7. Production witness matrix to adopt at the real owners

Vary these axes independently; a large member count alone is insufficient:

| Workload / perturbation | Evidence required |
|---|---|
| Long tiny edit history, N fixed | Cumulative hash/serialization/descriptor/store counts; no growing-prefix writes. |
| Large initial tags, H fixed | Initial construction/cleanup and point edit counts; current native root preserved. |
| Maximum legal UTF-8 values and actual digest-prefix shape | Bytes/validation separate from digest-key depth; no assumed logarithmic text trie. |
| Many no-ops, conflicting/replayed requests, failed stages | Attempt cost and live replay custody, without manufacturing historical events. |
| Repeated add/remove/re-add and description-only updates | All historical values/acts retained; unchanged fields not expanded. |
| Multiple valid anchors in one journal, corrections in imported history | Exact short/long scope, original rejection points, separately costed validation. |
| Shared diamonds, many member associations, distinct byte-identical members | Unique evidence visits plus real association/proof work; no scope collapse. |
| Warm, cold, evicted, interrupted and lost-state runs | Distinguish bounded restart from explicit rebuild; retained failed/undo work. |
| Lagging and new copies; divergent or forged predecessor | Missing suffix versus full closure, actual provider calls/bytes and no stale publication. |
| Shared export versus M detached outputs | Source verification, proof generation and necessary duplicated output charged separately. |
| GC under active attempts and whole collection deletion | Exact live-root protection, bounded candidates/adjacency, provider cleanup through terminal state. |

Use geometric small diagnostic ladders first, then sizes that cross actual
memory/database/provider regimes. Do not turn 128 edits or the #964 member
research envelope into a format cap or new per-commit mandated workload. Actual
production witnesses must bind operation/declaration/source/conditions through
the existing evidence consumer and preserve relevant negative cases. A test count,
passing syntax check or favorable wall time cannot qualify an unmeasured delegate.

## 8. Remaining engineering decisions, not another broad research project

The direction above is sufficient to start. Integration must still select and
review the G22 continuation/validation backend, exact bounded extent/checkpoint
layout, source-owned expressions, SQL/provider operation accounting, and retained
receipt/GC state. These are real implementation/proof obligations, not evidence
that the reference is production complete.

A no-semantic-change diagnostic success must not authorize altering a core digest,
delaying rejection, reducing history, adding a hidden capacity ceiling or trusting
unverified state. Escalate such a specific conflict. Ordinary choices within the
existing owners do not need a new ontology or a second cost-framework project.

## 9. First integration slice and stopping rule

Before freezing persisted bytes, record the actual source and confirm a small
checkpoint can name an append-stable new extent, unchanged typed state roots and
an exact flat prefix without an ever-growing list. Bind proposed metrics and
unestablished delegates to the integrated growth machinery. Run the actual
canonical core profile probe in the locked environment; the earlier dependency
block remains unresolved by these diagnostics.

Then implement the existing P00 establishment/edit/publication/offline-recovery
slice with counters on real hashing, node reads/writes, descriptor bytes and
provider calls. Verify every selected checkpoint in one forward byte pass and
challenge interruption plus a missing historical node. Continue through the
existing P01-P04 ownership map for copying, lifetime, security and composition.
Do not demand that all of #964 close before this scoped work can proceed; do not
claim a dependent path conforming before its actual delegates have proofs.

The growth supplement is complete as reference input when its finite experiments,
analysis, reproduction instructions and remaining proof boundaries are recorded.
Product acceptance remains #969's complete integrated boundary plus its additive
growth clarification. No new release work, collaborative editor, erasure API or
core ontology expansion is introduced.

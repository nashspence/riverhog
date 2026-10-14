# Archive durability growth closure — reference design for #958

Non-authoritative design input under #903. Audited implementation: `main` at
`e0036f80410a83cc5eaa9d63c17650ffae72e2bd`. The owning issue controls scope and
acceptance. This document proposes requirements; it does not assert that the
whole repository already satisfies them.

## 1. Three different commitments

**Representation extent** defines valid persisted encodings: individual bytes,
counts, offsets, ordinal domains, index/descriptor limits, terminals, and how a
logical collection progresses through bounded carriers. Changing these rules
can change which old archive bytes readers must accept.

**Operation growth** defines a supported reader/writer/materializer's work and
resource use over those representations. Preserve practical backward access,
not an exact planner, cache, database schema, implementation language, or SDK.
Do not put SQLite opcodes into the immutable archive format.

**Composition and evidence** bind the operation's actual calls, fanout, phases,
resource lifetimes, and retry scope to those bounds. A bounded helper invoked
once per member over the entire collection is not a bounded-growth operation.
A declaration or generated projection is not proof that its implementation
meets the bound.

Use `scripts/extent_contract.py`, `riverhog-growth-contracts`, source discovery
in `scripts/growth_contract.py`, and existing atlas/qualification machinery.
Do not add a parallel permanent inventory. Critical archive operations need
complete phase accounting: `unestablished` is an explicit unresolved gate, not
successful durability qualification. This does not impose universal complete
growth coverage on unrelated repository code.

## 2. Dimensions and units

Each declaration identifies successful, rejected, interrupted, cold, warm, or
resumed scope, exact start/end, trust assumptions, and independently observable
parameters. Measure both counts and encoded bytes; do not treat variable-size
records as constant-size merely because they fit Python objects.

| Symbol | Independently defined quantity |
|---|---|
| C | Stored ciphertext bytes in the selected required input closure. |
| P | Plaintext payload-volume bytes, including tar headers/index/padding. |
| D | Sum of recovered artifact content bytes (distinct from P). |
| H | Required metadata, journal, history and proof bytes, with unique immutable identities counted once where reuse is allowed. |
| U, V, J | Unique required objects, nonterminal provenance descriptors, and journals. A sequence also has one terminal. |
| E | Required semantic dependency/selection edges; shared histories do not erase distinct authenticated selections. |
| X | Output-expansion bytes required by the selected recovery layout, notably the sum of exact member-sidecar prefix lengths. This is computed from selected inputs, not defined as whatever bytes an implementation happened to read. |
| N, T | Committed physical objects examined and selected physical integrity-ledger entries. Include path/metadata encoded bytes separately. |
| S | Selector count AND total encoded selector bytes. Selective export may still scan N namespace metadata records. |
| A, Z | Active and abandoned checkpoint rows/bytes present at attempt start. |
| L, B, K | Accepted carrier byte limit, application I/O buffer, and configured cache/working-memory budget. |
| R | Attempts, with per-attempt source, destination and staging progress explicitly recorded. |
| W, M | Cryptographic header/work-factor inputs and permitted child-resource policy. |

A counter uses an exact representation. Multiplying counts by maximum encoded
record size has byte units; multiplying two byte totals is not a plausible I/O
bound. CPU comparison/hash/KDF work, logical reads, OS reads, remote requests,
provider copies, network bytes, allocated bytes, peak live bytes and final output
are separate metrics.

## 3. Representation closure and pre-v1 cuts

For each carrier derive the **joint** admissible domain, not independent maxima:
member count x encoded entry lengths x offsets x part count x ordinal width x
canonical escaping/numeric representation. Specify rounding, inclusive versus
exclusive endpoints, empty cases, and the mandatory next terminal ordinal.
Reserve enough room for index/metadata/terminal publication before accepting a
construction step that cannot be sealed. Rollover must make progress; a single
unrepresentable item must reject explicitly rather than loop or truncate.

The existing 1 MiB descriptor budget and proposed 16 MiB pack-index budget must
be writer/reader shared, with tests at their byte boundary and real maximum
member/part shapes. Those budgets do not by themselves prove all combinations
of the supported input domain are sealable. A byte-budget failure must not be
misrepresented as an arbitrary logical collection-size limit.

**New confirmed representation gap:** the outer archive sequence supports a
256-bit ordinal, but pack-index `volume.sequence` is a JSON number. A one-empty-
artifact plan accepts `2^53` and rejects `2^53+1` as inexact binary64 although both
outer ordinals are valid. The probe also checks a near-256-bit endpoint without
allocating a huge archive. This is a discontinuous encoding domain, not simply
an inclusive `2^53` cap.

Recommended pre-v1 disposition: use one exact ordinal encoding through the pack
index, planner, selective reader, independent reader, identity preimage, schemas,
and golden vectors (prefer the already-owned fixed-width archive ordinal).
Audit every embedded large count/offset similarly. Do not silently narrow the
logical domain or add old pre-v1 aliases. This reference **does not implement**
that format cut: it would change canonical index bytes and must be reconciled
with every consuming path and the final fixture baseline. Original captures are
historical input, not a demand to preserve obsolete pre-v1 representations.

## 4. Complete-operation targets and honest cost models

These are design targets to discharge with owner declarations, operation
composition, algorithm arguments and execution witnesses, not claimed measured
whole-operation guarantees.

| Operation | Required accounting / target |
|---|---|
| Finalization and writing | Encoded construction work proportional to emitted payload/metadata plus declared ordering/index work. Never rehash an ever-growing prefix per append. Bound in-flight buffers, counts, parts, and temporary live state. |
| Cold provenance authority | One root open, V+1 descriptor opens, no journal payload opens; verify complete terminal commitment before exposing an index. Index construction has O(V) inserted bounded rows and possibly O(V log V) comparison/page work. |
| All journal extraction under one prepared view | Sum of selected segments/bytes, not J full descriptor scans. Query plans must seek by journal and preserve order without scanning all journals or introducing per-journal global sorts. |
| Whole payload recovery | Required source byte I/O linear in C+P+D up to enumerated constant passes, plus metadata/output costs; bounded per-volume extraction and explicit application memory. Include empty members, tar index/header/padding work and raw segments. |
| History/proof selection | Account unique immutable decode/validation and required edges separately. Cache only exact root/format/identity/prefix/selection/extent, never merely journal ID. No exponential re-expansion of diamonds, recursive stack overflow, or unaccounted member x whole-history traversal. |
| Complete selected-copy recovery | Output-sensitive target O(C+P+H+X) byte work plus necessary record/edge and ordering/index costs. A desired CPU expression may include (U+E) log(U+E); do not describe all SQLite work as linear. Declare source, scratch and output streams separately. |
| Physical materialization | One namespace metadata traversal per continuity validation plus selected metadata/ledger work and selected payload copying. State explicitly whether selector matching is O(N*S), O(N log S), or length-indexed. Do not hide S as a constant. |
| Abandoned checkpoint cleanup | Progress by monotone key/rowid with bounded deletion batches. Surviving active rows must not be rescanned once per abandoned batch. Include A+Z checkpoint state as an input dimension. |

### Output expansion matters

Current independent recovery's `_selected_advice` validates an exact primary
journal prefix and writes it as a sidecar for each member. Shared source journals
can therefore entail large *required output* X. A promise linear only in unique
H is false unless the output contract changes. The payload itself is also
spooled before final materialization. Prefix selection, validation, sidecar
emission and redundant revalidation must be distinguished; output multiplicity
is not permission for unrelated rescans or an excuse to omit memoization.

The supplied patch establishes the descriptor-stage primitive bounds and one
operation-scoped indexed integration. It does not certify the entire history
resolver, materialization planner, tar parser, or full recovery complexity.

## 5. Memory, scratch and rejection

Publish an implementation-specific liveness ledger, not just O(n) scratch:
which files/buffers coexist, how many copies, when released, and peak versus
cumulative writes. A sequential phase composes by maximum peak only when earlier
state is actually released. Nested/index/cache/parallel phases add live resources.
Include per-worker multiplication, database cache/sort/journal files, Python
object expansion, output staging, and both plaintext and ciphertext lengths.

For current recovery, inspect artifact spool D, final payload staging D, current
volume plaintext max(P_i), staged journals, per-member sidecar X, history/record
indexes and output mapping. They need not all peak simultaneously: derive and
verify lifetimes before assigning coefficients. This reference does not present
an unmeasured byte-exact RSS or total-scratch multiplier.

For a bounded plaintext carrier, consume at most L+1 bytes from child stdout,
reject the sentinel, never return truncated success, and do not spool an
unbounded object before checking L. The proposed direct bounded reader writes
zero plaintext scratch; it may briefly hold a bytearray and returned bytes
(approximately 2L plus a bounded chunk at application-buffer level). That is not
a physical-RSS bound and excludes child resources and kernel buffers.

Successful bounded return requires complete authenticated EOF and successful
child exit. Oversize, exceptions and abandoned iterators close/reap the child;
partial payloads never replace the requested destination. Streaming consumers
that stop through generic bounded helpers may have one application chunk of
extra pull; declare that chunk rather than reusing the exact L+1 helper bound.

A small plaintext budget does **not** bound scrypt/header work. Keep an explicit
reader execution/admission policy for KDF parameters, header bytes, finite child
timeouts/cancellation, memory/CPU and diagnostics. A resource refusal must be
distinguishable from invalid archive format; supported high-cost representations
cannot disappear through an undocumented implementation limit. The patch bounds
parent stdout consumption and discards unused child stderr, but **does not** add
KDF, wall-clock, child-RSS or header admission limits.

Invalid tar/PAX/header/member inventories must also be rejected before unlimited
allocation or `getmembers()` accumulation. Valid-carrier limits alone do not
bound malicious pre-validation work. This remains an explicit integration gate.

## 6. Restart and selective-export contracts

Name whether an operation resumes payload work, metadata work, both, or neither.
Repeated arbitrary failures admit no finite total bound independent of R. Use
`sum(attempt_cost[i])`; separately state each attempt's maximum repeated work.
Never drop root/terminal, selected-namespace, immutable-payload or current-revision
checks to manufacture a smaller coefficient.

For filesystem materialization, account each attempt's:
- full namespace metadata continuity scan and selected integrity ledgers;
- source reads for remaining objects, including any previously staged prefix;
- destination rereads for already-published objects and staged-prefix comparison;
- newly written suffix bytes and recovery-owned checkpoint/temp files;
- bounded cleanup over active plus abandoned generations.

`copied_bytes` is not all source bytes read. Current staging resume can reread a
source prefix while writing only its suffix. A `selected` export may inspect
unselected object metadata but must not open unrelated payloads. Provider-side
full-object copies, remote egress and local disk rereads are distinct costs.
Capacity must be explicit and fail safely; no implicit semantic archive cap.

## 7. Permanent evidence and freeze gates

For each critical operation record owner, source declaration, exact scope,
dimensions and units, accepted carrier domain, expression/coefficients, named
callees and actual multiplicity, liveness, failure/retry behavior, proof basis,
witness source identity, historical fixture identity, and remaining limitations.
Use the existing growth representation for expressible bounds; include a separate
algorithm argument for asymptotic bounds that numeric samples cannot prove.

Require adversarial shape families, not only bigger total byte counts:
- many tiny/empty members; few large members; maximum parts with large offsets;
- ordinal/numeric boundaries, escaping and index/descriptor byte thresholds;
- many one-segment journals; few many-segment journals; shared prefixes and
  diamond/deep inherited histories with exact source proofs;
- selected/unselected namespaces, many selectors, superseded revisions,
  segmented committed objects, active/abandoned checkpoint generations;
- corrupt terminals, mutated payloads, oversized finite streams, failing and
  abandoned children, partial destination conflicts, interrupted/changed sources.

Assert deterministic logical opens/bytes, rows examined/query-plan properties,
peak live application buffers/files where measurable, and retry progress.
Geometric scaling and VM counts catch regressions but do not prove big-O alone.
Counterfactual tests must fail for repeated whole-sequence traversal, a stale
source/declaration digest, an extra over-budget pull, or a wrong local query plan.
Keep cryptographic/external-age and installed-provider proofs separate from
synthetic structural or Python-boundary substitutions.

Final acceptance requires closure across writer, ordinary service access,
independent recovery and supplied adapter materialization, on earlier accepted
bytes. Same-version builders are not the historical oracle. Accepting this
reference alone leaves #958 open; unresolved critical rows cannot be labeled
established merely because its focused tests pass.

## Primary technical references

- Python 3.13 subprocess documentation: https://docs.python.org/3.13/library/subprocess.html
  (pipe buffering, capture accumulation, child lifetime and cleanup).
- SQLite query planner: https://www.sqlite.org/queryplanner.html
  (multi-column selectivity, ordering, covering indexes and logarithmic lookup).
- Age format: https://age-encryption.org/v1
  (encryption framing and scrypt work factors, separate from Riverhog carriers).

Repository source, not these external documents, determines the specific
operation call graph and costs measured in this handoff.

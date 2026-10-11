# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: #964; convention: #903.
Reference branch: reference/external/964-large-collection-scaling
Previous reference / direct parent: 830c9ebedc8c37b6a18d4128992f46c4485d550e
Original authoritative branch base: main@8425e672c60285a549ca4d5aaba15ecbd99e5bf3
Original base tree: d18b813a489955b085094765f98282ea2e2276d0
New inventory audit: main@152d095038df0c0d0ed642cf265d47b11c65a754
New inventory source tree: 991e994457b487a7dca016b7fc0409485cef25c6

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: not exposed
Requested by: Riverhog maintainer, through this conversation
Published by: connected GitHub integration; commit metadata is publication metadata.

## Integration agent: start here

Read [v1_inventory/README.md](v1_inventory/README.md), then the
[inventory index](v1_inventory/INVENTORY.md) and
[declaration structure](v1_inventory/STRUCTURE.md). This revision supplies the
repo-wide proposal discussed with the maintainer: 62 proposed public growth-
contract families and two internal engineering-budget families, a mapping of
86 workspace distributions, 182 source anchors, and an illustrative declaration.
Every family includes scope, work and memory/state bounds, invariants and witnesses.

The latest issue comment adopts this inventory as the working scope and structure
for declaration work. It is not approval of every final bound or a claim that the
existing implementation conforms. Family IDs are reference review labels, not
new public contract IDs. Do not copy this hand-maintained inventory into main as
an alternate registry. Owner-authored declarations and existing derived discovery
remain the authority and coverage path.

Start with exact operation applicability, not a blanket linearity statement.
Resolve each relevant discovered operation to its own declaration, an inherited
one plus wrapper overhead, an exact delegated portion, or a justified non-
executable disposition. Distribution coverage is not method/route coverage.
A declaration must state units, measured primitives, scope/completion, worst-case
versus amortized/expected mode, cache and retry conditions, and symbolic delegated
call multiplicities. No linear composite is accepted with incompatible or
unresolved reachable work. Include status, authorization, validation, publication,
discovery readiness, cleanup and replay within their actual lifecycle boundaries.

## Decisions still required before freeze

- G22: arbitrary online graph updates/corrections are not the same as one static
  graph check. Preserve required acceptance/rejection boundaries.
- G02/G28/G40: approve named schema, query and predicate profiles; no variable
  that merely means whatever work the current evaluator performed.
- G26: review shared-history representation, exact query scopes, dataset identity
  and durable-state consequences. Expanded internal rows cannot justify themselves.
- Indexed/ordered terms: document and approve actual costs. Linear visits plus
  logarithmic indexing is not strict linear total machine work. Component tool,
  codec and provider profiles must also be explicit in composed claims.

Linear streaming/traversal remains the default. Any different bound requires
specific justification and approval; general agreement does not silently approve
all exceptions. Preserve materialized-output memory costs rather than promising
constant memory for APIs returning complete lists or byte strings.

Normative declarations belong in the existing Contract Closure. Source traces,
algorithm arguments, witness bindings, measured counters and execution status
belong in the bound Audit Record. Exact shared schema/key placement still needs
owner review. Internal G63/G64 budgets do not become public archive guarantees.

## Evidence and integration sequence

First reconcile the reference against the exact source integrated with #963.
Develop owner declarations and exact operation coverage; establish the outstanding
cost/design decisions; implement fixes and adversarial real-implementation work-
count witnesses; then qualify composition and publish the reviewed closure/evidence.
Use small supported page/cache/segment capacities to expose boundary behavior.
Keep targeted real database/platform/final-image witnesses where those lower layers
matter. Finite samples corroborate algorithmic accounting, not universal proof.
Existing required proof cannot be dropped merely because a smaller witness is fast.

The earlier GROWTH_CONTRACTS.md and SUPERLINEAR_INTEGRATION.md remain useful policy
and candidate-fix material. Historical source probes/results remain pinned to
8425e672 and are unchanged; they are not re-labeled as current-main measurements.
This inventory is pinned to the later 152d0950 source. The reference branch itself
is not rebased: it retains its original runtime tree and all published ancestors.
Run the inventory verifier against a separate checkout of the inventory source,
not against this reference branch's older runtime tree.

#961 owns onboarding; #963 owns functional/growth/timing evidence and approved
internal target applicability; #964 owns owner-specific growth work; #469 owns
release accounting. Coordinated #963/#964 work retains #961's prerequisite and
the shared measurement foundation before dependent acceptance. The #960 explicit
timing-shortfall closeout disposition (6102101195) and maintained report-only
600-second profile are unchanged. No release/provider operation is authorized.

## What is supplied and checked

The factored JSON package restores the entire previously reviewed inventory object
exactly, including source URLs/hashes and per-distribution qualifications. Its
canonical-object SHA-256 is recorded in metadata.json. This is lossless publication
of the proposal, not a new product serialization contract. The structure document,
example and original static verification report are retained verbatim. The shorter
INVENTORY.md is a navigation index; complete family detail is in the JSON parts.

Validation for this publication: source-reference verifier passed on the exact
152d0950 checkout (64 families, 86 distributions, 182 anchors); reconstructed data
compared equal to the original reviewed object; seven package/integrity tests
passed; JSON and Python syntax, local document links and Git whitespace checked.
Published blobs and complete tree are checked against the prepared tree before
recording the final reference SHA in the issue.

Not performed: runtime growth/conformance tests, full dynamic Contract Closure
or public method/route applicability generation, locked full lint/unit/dist/build,
Docker/PostgreSQL/FTP/large-file/recovery qualification, CI/CodeQL, or provider/
release operations. Historical static scan counts are not newly executed dynamic
coverage. No runtime fix or complete v1 growth guarantee is supplied by this branch.

## Publication and authority

Only .reference material changes. The exact reference SHA recorded in #964 is the
handoff identity; the branch name is navigation. Preserve every prior published
reference in history. The issue and subsequent maintainer decisions control;
reconcile and integrate through the normal rail, never merge .reference into main.

The publisher does not expose native Development or dependency mutation. Existing
Markdown links and comments are not claimed to establish those native relationships.
No new native relationship is asserted by this revision.

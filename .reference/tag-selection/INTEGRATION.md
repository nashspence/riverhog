# Focused integration map for #976

The owning issue is the specification. This map names the narrow seams; it is not a
second acceptance registry or an implementation queue.

## Contract and consumer owners

- `packages/riverhog-protocol/src/riverhog_protocol/artifact_discovery.py`:
  factor the three flat tag clauses into one exported `CollectionTagSelector`
  authority in the existing package. Preserve discovery's flat shape, independent
  256-member bounds, order retention, empty-clause defaults and identity behavior.
- `packages/riverhog-protocol/src/riverhog_protocol/collection_tags.py`:
  reuse `CollectionTag` and its exact Unicode/byte validator. Its 100-member request
  batch constant is not the discovery clause maximum.
- `packages/riverhog-client/src/riverhog_client/`:
  own generic exact membership validation and incremental Boolean evaluation,
  without Stove0 imports. Reconcile the overlapping extraction proposed in #955;
  do not depend on its materializer/scheduler or expand its single-tag CLI.
- `some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`:
  require the single selector on admission/departure policies and embedded
  admission intents/readbacks. Remove both old variants, the union and exports.
  `{}` is deliberate match-all; a missing policy selector remains invalid.
- `some-implementations/stove0/application/server/src/stove0_core/`:
  replace `catalog_predicate.py`'s implementation with the shared helper; update
  `admission.py`, `departure.py` and persistence for unfinished evaluations. Keep
  policy/descriptor/source/view/generation fences and cursor/obligation acceptance
  at the application owner. Preserve entry and actual-departure semantics.
- Update current executable config/schema, checked fixtures, state baseline if
  needed, API/client/readbacks and focused qualification at the same owners. No
  aliases, migrations, unrelated recipe field rewrites or new package are intended.

Start with actual protocol model/schema parity and exact-client tests, then wire
complete, resumable owner-local evaluation. Do not ship a partly rewired selector.
Use current repository gates and source-owned extent/growth witnesses. Later main
or issue decisions may change the private seams; the old source pin is not a
requirement to bypass newer infrastructure.

## What the witness does

`reference.py` illustrates the issue's accepted-input rules and an incremental
exact-probe evaluator. One step makes at most one new point-membership contact.
Clause order is internal; serialized array order remains untouched. A shared
within-evaluation evidence map avoids repeated probes across overlapping clauses.
A failed/malformed contact is not negative evidence and does not consume that probe
position. Partial progress is scoped to the caller's exact binding.

`test_reference.py` obtains only the pure tag validator/constants from the audited
source via AST, avoiding a duplicate Unicode policy and avoiding any claim that the
locked application was imported. Its discovery-source check verifies only the
three field defaults and bounds. The truth-table oracle is set algebra, not the
server's SQL implementation. Replace these audit adapters with ordinary product
imports and actual query/service tests during integration.

## What the witness deliberately does not do

It does not define a public checkpoint format, validate/authenticate the supplied
source/view/policy/descriptor identities, implement grants, persist state, own
catalog traversal, deduplicate work, or run admission/departure effects. The test's
deepcopy between steps is not a database crash/replay/atomicity proof; copying a
whole growing evidence map per production step would violate the intended growth
requirement. Persist bounded deltas and a frontier at the actual owner instead.

Production acceptance must still prove small-budget liveness/fairness, fencing
across real restarts/concurrent workers, exact historical reads, real SQL parity,
maximum-size metadata carriers, policy identity, cursor-plus-obligation atomicity,
and retained-state/cleanup costs. Do not treat 768 bounded slots as permission for
768 blocking calls per HTTP control turn, or 100 such evaluations per baseline
page. Do not convert budget exhaustion to false or repeatedly restart a retained
successful prefix. No timeout increase or new logical tag ceiling is a substitute.

No production file should import this reference, and no reference result is a
product qualification claim. Record adopted/reproduced/partial/superseded/rejected
consumption once in #976, with the actual integration SHA and remaining proof.

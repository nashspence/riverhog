# G27/G28 bounded-work revision

Start with [DESIGN.md](DESIGN.md): the selected default policy, retained-view
architecture, explicit contract changes, owner map and acceptance obligations.
[ALGORITHMS.md](ALGORITHMS.md) retains detailed seek/summary/matcher/scope algorithms
subject to that design. [bounded/RESEARCH.md](bounded/RESEARCH.md) explains the new
source audit and primary technical references.

The previous full-query MVCC snapshot and global unchanged-fence retry lifecycle
are superseded. Earlier public-identity/no-pending recommendations were intent
calibration, not a reason to retain an unsuitable default. New snapshot-at-admission,
pending/cancel/expiry and exact segmented support transport are explicitly
**approved as current-only pre-v1 contract changes** by
[#964 decision 6118482544](https://github.com/nashspence/riverhog/issues/964#issuecomment-6118482544),
not claimed deployed or qualified by this reference.
Current archive/custody/authorization and G26/Alembic boundaries remain intact.

## New diagnostic reproduction

Only Python stdlib plus pytest are needed for the new model. No native Riverhog
source modules, copied third-party dependencies or provider access are used:

```sh
cd "$REF/.reference/g27_g28/bounded"
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q -W error -o addopts='' test_model.py
python probe.py > /tmp/g27-g28-bounded-counters.json
```

Recorded: **47 tests passed**, Python 3.13.5 / SQLite 3.46.1 / pytest 9.0.2,
warnings treated as errors. The first 42-test run passed; five further adversarial
controls were added and all 47 passed. [bounded/evidence.json](bounded/evidence.json)
records exact commands/hashes, scope and unexecuted integration obligations.

The model uses real SQLite WAL transactions and pinned copy-on-write directory
roots. It checks bounded checkpoint work, progress during content mutation,
rollback, restart, stale workers, current authority failure, explicit capacity
suspension/resume, and incremental GC. A long-reader negative control blocks WAL
truncation; bounded reads allow it while the old logical query remains active.
It is not the production G26 compiler, authorization service, connection pool,
query protocol, canonical codec, PostgreSQL implementation or resource policy.
Model values are small except separate streamed-literal tests; no production
large-field carrier claim is made by those tests.

The probe at 128/512/2048 consecutive keys uses at most 17 node visits per quantum,
zero admission node scans and zero complete-query restarts despite a content
publication at every quantum. Its dense-key counts are not the general radix
bound; a separate sparse-key test guards the O(w*C) declaration. These are
work-count observations, not wall-clock targets or final-image qualifications.

## Historical material

`reference.py`, `test_reference.py`, `probe.py`, `counters.json`, `evidence.json`
and `RESEARCH.md` at this directory's top level retain the parent handoff's
native helper and old lifecycle diagnostics. Their 53 reference tests and 12
native passes/one exclusion are historical, not rerun or combined with the new
47 passes. In particular `ProjectionStore` models the superseded coarse-fence
lifecycle and is NOT the implementation to integrate. The seek/matcher portions
remain useful at their audited source and with the new streaming qualifications.
G22/G26 executable materials and historical evidence are unchanged.

The inventory digest recorded by the initial retained-view revision is historical;
current recommendation routing and its digest live in ../v1_inventory/metadata.json.
Use [P07](../integration/packets/P07.md) and [P08](../integration/packets/P08.md)
for focused integration. Neither source-integrity checks nor these models establish
dynamic Closure coverage or runtime conformance.

## Provenance and limits

Producer: OpenAI ChatGPT, GPT-6 Astra Pro; effort/configuration not exposed.
Requester: Riverhog maintainer. Publisher: connected GitHub integration.
Only .reference material changes. Existing published ancestors are retained under
#903; the new exact commit recorded in #964 is the handoff identity. Reconcile
against integrated post-#963 source and ordinary owner/contract validation.
No main, release/provider, protected settings or issue-body rewrite is intended.
No native Development/dependency mutation is claimed by Markdown links.

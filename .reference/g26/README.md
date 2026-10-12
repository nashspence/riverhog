# G26 integration reference

Start with [DESIGN.md](DESIGN.md) for the current segmented/resumable build and
[SEMANTICS.md](SEMANTICS.md) for the retained exact source relation and scope rules.
[../g22/DESIGN.md](../g22/DESIGN.md) owns resumable neutral journal validation;
[../g27_g28/DESIGN.md](../g27_g28/DESIGN.md) owns retained-view query progression.

The new ordered Merkle dataset commitment is an **approved** refinement of the
earlier flat compact hash proposal ([#964 decision 6118482544](https://github.com/nashspence/riverhog/issues/964#issuecomment-6118482544)).
It is not the old digest under another computation, and it does not change
archive/member/journal/hash authority. Owner-specific normal-form encoding,
golden vectors and implementation/qualification still require exact review. Retain Alembic and one current
v1_0001 baseline; no compatibility/migration from discarded pre-v1 application state.
Multiple pinned immutable generations in the current schema are normal retention,
not obsolete-schema compatibility.

New diagnostic reproduction and limitations:
[../g22_g26/README.md](../g22_g26/README.md).

Historical code/evidence remain unchanged: factored_index.py, native_compiler.py,
test_factored_index.py, test_native_compiler.py, profile_g26.py, counters.json,
evidence.json and RESEARCH.md. They demonstrate source semantics and earlier model
work at their own source identities, not the new segmented physical implementation
or Merkle codec. In particular, native_compiler.py's complete/per-prefix views are
NOT a production reader; the old model seal/workspace is NOT the revised lifecycle.
The old README's outer-response/old-cursor guidance is superseded by G27/G28's
explicit current-only discovery protocol recommendation.

Use the separate audited main checkout 152d095038df0c0d0ed642cf265d47b11c65a754
for source controls, then reconcile actual integrated post-#963 source. Do not merge
the reference branch or treat it as a new policy/target registry. Full production
parity, large valid journal/profile input, real database/writer/pin/cleanup behavior,
independent recovery and ordinary qualification remain required.

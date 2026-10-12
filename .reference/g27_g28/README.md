# G27/G28 integration reference

Start with [DESIGN.md](DESIGN.md). It specifies the selected algorithms,
consistency contracts, private relational interfaces, invalidation and lifecycle,
summary-delta invariants, operation-scoped bounds, implementation owners and
acceptance sequence. [RESEARCH.md](RESEARCH.md) records source findings and primary
references. [evidence.json](evidence.json) separates executed work from gaps.

`reference.py` contains an owner-explicit non-null seek, a native-dependency
one-pass clause matcher, a disjoint-prefix seek fallback, and a **separate SQLite
lifecycle model**. Its model hash
and exception/API names are not Riverhog contracts. Do not put this entire file
into main. `test_reference.py` compares helper behavior with inspected native
source and exercises model fencing/replay/ready/expiry/reopen behavior.
`probe.py` emits [counters.json](counters.json) and refuses a different source SHA.
The final seek review includes full-tuple/all-ties work controls, removes redundant
null-order modifiers, and preserves a partial-rowid-seek negative control.
These are controlled component observations, not a production query builder,
full PostgreSQL plan, valid-wire media workload, or timing qualification.

## Reproduce

Use a separate checkout of `152d095038df0c0d0ed642cf265d47b11c65a754` as AUDITED,
not this branch's historical runtime tree. REF is this branch checkout. Use the
repository's normal development dependencies; the runs recorded here instead
used an explicitly reported native environment. No dependency source is vendored
in the reference. Prepare PYTHONPATH with AUDITED's workspace `src` directories:

```sh
export PYTHONPATH="$REF/.reference/g27_g28:$(find "$AUDITED" -type d -name src -print | paste -sd: -):${PYTHONPATH:-}"
export PYTEST_DISABLE_PLUGIN_AUTOLOAD=1
cd "$AUDITED"
python -m pytest -q -W error -o addopts='' "$REF/.reference/g27_g28/test_reference.py"
python "$REF/.reference/g27_g28/probe.py" --repo "$AUDITED" > /tmp/g27-g28-counters.json
python -m pytest -q -W error -o addopts='' \
  tests/unit/test_canonical_discovery_extraction.py \
  tests/unit/test_canonical_discovery_search.py \
  tests/unit/test_search_service.py -k 'not http_authentication'
```

Recorded: 53 new tests passed, then 12 unchanged source tests passed with one HTTP
case deliberately deselected. The new parameterized tests include 480 seeded
matcher comparisons; these are not 480 independently reported pytest tests.
Warnings were errors. A prior new-test attempt had four failures because the
random fixture generated ASCII-fold on integers; that generator was corrected,
not the protocol constraint. A preliminary SQL model smoke exposed an INSERT
placeholder-count error, fixed before tests. Both are retained in evidence.

The native environment lacked the installed RFC8785 package. The exact upstream
v0.1.4 implementation (verified Git blob `3137d3326b98938affadb1be711ee411eb2ab86e`)
was supplied outside the repository with an export shim. An initial transcription
differed by one blank line; final runs used verified exact bytes. No substitute
canonicalizer, dependency copy, warning relaxation or package metadata shim is
published. This is not a run of the locked workspace gates.

## Ownership and approval

Produced by OpenAI ChatGPT, model GPT-6 Astra Pro; configuration/effort not exposed.
Requested by the Riverhog maintainer in this conversation. Published through the
connected GitHub integration; commit metadata is publication metadata.

The normal integration agent must reconcile post-#963 source and issue decisions.
The 64-family inventory is a proposal; supplying this detailed design does not
approve its formulas, new private schema or resource conditions. G26's already
approved derived identity/current pre-v1 hard cut remains intact. Preserve the
single current `v1_0001` baseline, Alembic, explicit bootstrap and synthetic future
forward-revision/interruption tests; no old pre-v1 compatibility machinery.

Only `.reference` files change. G22/G26 algorithms, tests and historical evidence
are unchanged. No combined G22/G26/G27/G28 pass is claimed. Exact reference SHA
recorded on #964 is the durable handoff identity under #903. Native Development /
dependency mutation is unavailable to this publisher; a textual branch reference
is not claimed to establish the native relationship. No mainline/release/provider
or protected-setting change is made.

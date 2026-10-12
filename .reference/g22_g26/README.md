# G22/G26 bounded-work revision: reproduction and evidence

Start with [G22 DESIGN](../g22/DESIGN.md) and [G26 DESIGN](../g26/DESIGN.md).
This directory is a small executable design diagnostic, NOT a shared production
scheduler, crypto library, canonicalizer, journal validator or discovery index.

## Reproduce

Use a separate audited main checkout at
152d095038df0c0d0ed642cf265d47b11c65a754. The reference branch's runtime is older.
With Python, pytest and jsonschema available:

```sh
export RIVERHOG_AUDITED=/absolute/path/to/audited-main
export PYTEST_DISABLE_PLUGIN_AUTOLOAD=1
python -m pytest -q -W error -o addopts='' /path/to/reference/.reference/g22_g26/test_work.py
python /path/to/reference/.reference/g22_g26/probe.py
python -S /path/to/reference/.reference/v1_inventory/verify_inventory.py --repo "$RIVERHOG_AUDITED"
python -m unittest discover -s /path/to/reference/.reference/v1_inventory -p test_inventory_package.py -v
```

The producer's 65 diagnostic tests pass. The 10 unchanged inventory tests pass
separately; the verifier checks 64 families, 86 projects and 182 source anchors.
The first two inventory attempts each had 9 passes/1 failure because revised
G26 wording omitted exact protected historical-policy phrases. The text was
corrected to retain that policy while distinguishing the new Merkle refinement;
no test was weakened. Commands/results/hashes are in evidence.json.

sha256_state.py is an independently written RFC SHA-256 diagnostic with a private
state codec, compared to native hashlib across padding/block and random restart
boundaries. It is not vetted production crypto or permission to restore untrusted
states. Its specified-message-domain and fixture codec bounds are not proposed
new Riverhog request limits. Production engine selection and scope/identity binding
are explicit G22 integration requirements.

model.py uses real SQLite files/connections, durable-style transactions, paged Kahn
vertex/edge work, segmented fact staging and checkpointed ordered Merkle carry/fold.
It reopens connections between calls. Exception rollback simulates a pre-commit
failure; it does not prove process-kill or power-loss durability. Small fixed data
hashes execute inside these diagnostic transactions; production loads/computation/
CAS separation and borrowed-pool lifetime need their own instrumentation.

The fact fixtures are arbitrary bytes and integer posting coordinates, NOT full
native canonical source records. Diagnostic fragments are 256 bytes, while the
recommended production normal-form codec is separately specified in G26 DESIGN.
The recursive Merkle oracle is deliberately small/test-only; production fold is
iterative and persisted. No production digest golden vector is asserted here.

The source-anchored negative executes the exact native stage_assertion_page guard
prefix with controlled dependencies and validates 20,001 scalars against the real
generic JSON definition. It is NOT a complete valid profile/journal/custody fixture
or a live endpoint call. The actual production path must have that stronger witness.

Counters at N=128/512/2048 are controlled component observations: Kahn visits each
edge once; both graph and Merkle work use at most 17 logical transitions per tick.
SQL statement counts, lower-layer scans/index work, steady-state RAM, PostgreSQL,
wall-time targets and final-image qualification are not established by those counts.

Historical G22, G26 and G27/G28 native/model evidence remains unchanged at its own
source identity and is not combined with this run. Full exact-prefix corrections,
online undo, streaming native extraction, commitment encoding, retained-generation
GC, authority races, built recovery and ordinary locked/remote gates remain required.
The reference branch and generated evidence do not establish accepted public policy.

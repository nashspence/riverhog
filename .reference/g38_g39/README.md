# G38/G39 researched reference

Owning issue #964; convention #903. Producer: OpenAI ChatGPT / GPT-6 Astra Pro;
configuration/effort not exposed. Requester: Riverhog maintainer. Publication
through the connected GitHub integration does not confer contract authority.

For current integration start with [P10](../integration/packets/P10.md) or
[P11](../integration/packets/P11.md). Open selected sections of DESIGN.md,
INTEGRATION.md or RESEARCH.md only as needed. This revision contributes whole-scope observation
representation, exact incremental acceptance and owner/protocol/provider wiring.
It preserves the G22/G26/G27/G28 files and their historical evidence.

## Reproduce

Use a separate exact main checkout at
`152d095038df0c0d0ed642cf265d47b11c65a754`. REF is this reference checkout, NOT
its historical runtime base as a substitute source. The tests require Python
3.13, pytest and SQLAlchemy for native query-shape probes; they do not import the
whole installed product stack. Paths may be anywhere on the local filesystem.

```sh
export REF=/path/to/reference-checkout
export RIVERHOG_AUDITED=/path/to/exact-audited-main
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q -W error -o addopts='' \
  "$REF/.reference/g38_g39/test_reference.py"
python "$REF/.reference/g38_g39/probe.py" --repo "$RIVERHOG_AUDITED"
python -S "$REF/.reference/v1_inventory/verify_inventory.py" --repo "$RIVERHOG_AUDITED"
python -m unittest discover -s "$REF/.reference/v1_inventory" -p test_inventory_package.py -q
```

50 focused tests passed with warnings as errors. 43-test and 47-test interim
runs preceded the extended 50-test run; do not add these counts. Exact commands,
environment, hashes and final rerun are in [evidence.json](evidence.json).
The existing inventory tests and verifier are separately identified there.

`reference.py` is a small SQLite lifecycle model: exact chunk integrity, private
acceptance, source/answer coverage, taint, CAS and cleanup. Its `encode` is fixture
JSON, NOT JCS. Its SHA helper comes from the earlier diagnostic at
`../g22_g26/sha256_state.py`, not approved production cryptography. Opaque grant
flags and the independent-validation flag model decisions; they are not HTTP
authentication, live Riverhog authorization or actual schema/semantic validators.

`source_probe.py` runs actual source AST query/loop fragments with controlled
fixtures and real SQLAlchemy/SQLite. Boundaries replaced by fixture adapters are
listed in its docstrings and RESEARCH. `probe.py` retains deterministic component
counts in [counters.json](counters.json). Model seeding is test preparation, not
proof of bounded production source selection. Model statements and hashing are
small; no CPU/SQL worst-case profile is established by row-count assertions alone.

Not validated: the integrated protocol/transport/validator/provider design,
valid native whole-scope results and original support parity, PostgreSQL and
actual deployment/concurrency/kill/recovery/security/qualification rails. The scoped
current-only public envelope/ABI/cap direction is approved in #964 comment
6118314165; exact vectors and implementation still require owner qualification. Existing named
extension/schema delegates remain separately qualified; no hidden capability
restriction or automatic conformance follows from a reference passing tests.

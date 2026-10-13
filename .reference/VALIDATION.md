# Reference validation record

Date: 2026-10-13. Scope: the externally produced `.reference/` tree for #969.
Audited base and producer identity are in HANDOFF.md. This is not product
qualification or evidence that the requested feature has been integrated.

## Performed on the final reference code

| Command | Observed result | Wall time |
| --- | --- | --- |
| `python .reference/test_annotations.py` | 26 model tests passed | 0.841 s |
| `python .reference/verify_schema.py` | 9 core-schema frames; 8 profile values passed | 1.711 s |
| `python .reference/verify_schema.py --full-core` | blocked at import: missing `rfc8785`; exit 1 | 1.710 s |
| `python -m py_compile .reference/annotation_reference.py .reference/test_annotations.py .reference/verify_schema.py` | passed | 0.637 s |
| `git diff --check` | passed for tracked source; staged reference checked separately before publication | 0.003 s |

The unittest runner reported 0.205 seconds inside the first wall-time measurement.
Times are single local observations, not targets, benchmarks or growth evidence.

The schema probe loads the unchanged audited journal schema and its actual
referencing resources. It verifies these installed versions against `uv.lock`:
jsonschema 4.26.0, referencing 0.37.0, rpds-py 2026.5.1, attrs 26.1.0 and
jsonschema-specifications 2025.9.1. These match. Schema acceptance does not prove
journal/corpus semantics. The full-core mode calls the real binding/catalog and
journal-set validator, without a shim, but could not import its dependencies.

The source example `packages/riverhog-provenance/examples/never-named-emission.jsonseq`
is reused unchanged. It is one of the nine frames; seven are collection-history
frames and one is the new composition bridge. The narrow profile and model
formats are fixture examples, not new public contracts.

## Required repository gates attempted, not passed

`make lint`, `make unit`, `make dist-smoke`, `make build` and
`make linux-qualification` each exited 2 before running their work because
`mise` was unavailable. Each reported:

```text
Riverhog Makefile targets require mise on PATH, or MISE_BIN=/abs/path/to/mise.
Install mise or pass MISE_BIN explicitly, then rerun make.
```

A complete locked offline environment setup was attempted and blocked by uncached
dependencies. Network/DNS access from the execution container was unavailable.
The container provided Python 3.13.5 and uv 0.10.0 rather than the pinned Python
3.12.3/uv 0.11.24 toolchain. No lock file, version pin, dependency or policy was
changed to bypass this. No stub canonical serializer was installed for runtime
validation. The model's explicitly restricted fixture encoder stays local to the
reference and is not claimed as production canonical serialization.

## Not established

Full canonical runtime/corpus validation; lint/type/product tests; distribution
or image builds; PostgreSQL; real process/storage crash or CAS behavior;
copy-incarnation fences; encryption; actual membership/source-binding proofs;
server history authorization; installed independent recovery/composition;
complete downstream inheritance/retirement; bounded growth and lifetime cost;
provider or release qualification. No numerical performance claim is made.

Publication verifies exact tree/parent/branch identity separately and records
any exact-reference GitHub checks in the owning issue. An absence of checks is
not a green CI result. This branch must not be integrated mechanically.

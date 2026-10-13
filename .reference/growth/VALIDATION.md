# Growth supplement: evidence and limits

Date: 2026-10-13. Non-authoritative reference for #969 under #903; producer,
requester and model attribution are in [HANDOFF](../HANDOFF.md). This record
covers the new growth diagnostics, not a rerun or qualification of the original
annotation model. [GROWTH](../GROWTH.md) owns the explanatory analysis only;
issue decisions and actual source-owned declarations remain authoritative.

## Source and reproduction

Original reference: `a493d050ed486cb8bbb8193bb04e18d550216c75`; authoritative
branch base: `e43df5b5acce5d730c28b48b084e87445fbaac3b`. Read-only audit against
`main@9a485bc174b7b8d703ac05159e3df9be4f36be98` found the studied tag/journal/
recovery owners unchanged and the shared declaration/evidence machinery newly
present. No main-source, ontology, schema, lock or workflow file is altered by
this supplement. The publication record supplies its exact descendant SHA.

The local source came from the earlier full Git clone ZIP, whose SHA-256 was
verified as `e5830c04fc8d275c7561c54eb3021a2f61329749ff52ec679ab6c6c551e09aef`.
A fresh clone connector failed authentication; raw network/DNS access was also
unavailable. Current-source and issue checks used the functioning GitHub reader.
This does not turn the old checkout into the newer main.

From the reference checkout, preferably in the repository's locked environment:

```sh
python .reference/growth/test_growth.py
python .reference/growth/probe.py --output /absolute/diagnostic-results.json
python -m py_compile .reference/growth/probe.py .reference/growth/test_growth.py
```

The harness requires the exact G22 reference Git object
`e35bddf8f1bb72ca3f0046b597d5966cd5a08312` to be available locally. It uses
`git show`, never silently downloads or substitutes its diagnostic engine.
A shallow clone without this object cannot pass the G22 test group. The actual
native tag source is also checked before execution:

| Source | SHA-256 |
|---|---|
| `packages/riverhog-protocol/src/riverhog_protocol/collection_tags.py` | `84fd0abe870f22d88e83a9f58ca2d8ee11ef078baf9dad5165bdd93f3e8b0ec5` |
| G22 `.reference/g22_g26/sha256_state.py` | `e8326f2eff2fb5094e037c04bdf66e499ed86888b7c543f3ebf81b7f013f1380` |

The checked-in [results.json](results.json) additionally identifies the exact
probe and test bytes. Those hashes were rechecked after the final run.

## Execution actually performed

Available interpreter: Python 3.13.5; SQLite 3.46.1. These are not the complete
pinned project toolchain (Python 3.12.3 / uv 0.11.24). No version pin, serializer,
dependency or policy was replaced to make a check pass.

| Final command | Result | Wall seconds |
|---|---|---:|
| `python -u .reference/growth/test_growth.py PrefixTests NativeRadixTests ExistingG22DiagnosticTests` | 17 tests passed | 17.88 |
| `python -u .reference/growth/test_growth.py StructuralWalkTests ReplicationTests` | 15 tests passed | 6.92 |
| `python .reference/growth/probe.py --output .reference/growth/results.json` | completed; `production_conformance=false` | 6.93 |
| `python -m py_compile` on the two new Python files | passed | not separately timed |
| Results/probe/test SHA-256 agreement | passed | not separately timed |

The two final test groups cover all 32 challenges without changing their assertions,
SQLite durability settings or workload. An earlier single suite run also passed
32 tests. A subsequent combined command orchestrator exceeded its 45-second
container budget and was interrupted before producing its planned retained logs.
That attempt is **not a pass**; no remaining process was found. The final groups
above were rerun independently to retain their actual outcomes. No assertion
failure was diagnosed from the interrupted, buffered-output attempt.

Times are local observations, not targets, asymptotic evidence, a performance
qualification, or permission to omit a required check. Primitive counts and their
scope are recorded separately in results.json.

## Concrete findings and boundaries

**Prefix counterexample.** For 128 fixed 64-byte records, the selected input is
8,192 bytes. Rehashing every prefix supplies 528,384 bytes to SHA-256; repeating
that bad recovery through the growing sequence supplies 22,896,640. A single
forward incremental pass supplies 8,192 and preserves every expected prefix
hash. This demonstrates the specified algorithmic pattern, not the complete
old-model invocation graph or a production annotation implementation.

**Native digest-key radix.** The probe executes the hash-pinned binary kernel by
extracting its AST before the Pydantic head wrapper and excluding its unused
Pydantic/canonical-JSON imports and public annotation alias. The original native
validation, SHA-256 keys, node encode/decode and mutation bodies execute unchanged.
This is not whole-module, public API, database or storage-adapter conformance.
No fake canonical serializer is supplied. Node calls/bytes are observed through
the actual memory-store interface; fixture construction and oracle traversal
are separate from measured point operations.

A 65,536-byte Unicode tag is tested independently of tag count. A distinct fixture
finds 128 actual digests sharing their first key byte and selects an operand with
the same digest prefix; fixture search cost is reported separately. It is not a
32-byte SHA collision and does not prove worst-case depth empirically. The fixed
32-byte key path bound is justified from the native algorithm in GROWTH.

**Historical tag verification.** With 128 initial tags and 128 changes, repeated
snapshot validation makes 19,801 node reads. Baseline validation plus exact native
replay makes 153 + 192 reads. Read/put bytes are also recorded; counts alone do not
claim total CPU. Verify-only puts require the exact historical nodes already to
exist, so missing nodes or wrong declared roots fail instead of being repaired
silently. No checkpoint/journal/corpus crypto or complete archive is validated by
this isolated state-transition experiment.

**Shared structure and restart.** The 256-layer diamond has 513 distinct nodes and
1,022 edges. Both uninterrupted traversal and reopening after every seven-unit
step visit each exactly once. The latter makes 292 reopenings and 293 source-binding
checks; SQLite virtual-machine work is reported separately. Tests also cover
2,048-node depth, high-degree adjacency, duplicate roots, cycles, missing children,
selection mismatch and an `os._exit(77)` interruption after committed progress
plus an uncommitted counter change. The retained cursor resumes without accepting
the uncommitted value. This is real SQLite process behavior with trusted scratch,
not power-loss/fsync proof, hostile-state authentication, actual SourceBindingProof,
semantic journal validation or a bounded-memory claim for fixture preparation.

**Suffix and continuation.** A 4,096-checkpoint toy chain needs 0, 1, 16 or 128
checkpoint reads at those respective lags from an already verified ancestor.
The tests reject corrupt and divergent selections. This is suffix discovery, not
provider replication or proof that an untrusted receipt establishes closure.
The existing G22 diagnostic preserves native hash parity across residual/padding
boundaries and 13 serializations/restores; its largest saved state is 108 bytes.
A shape-valid changed state is shown not to authenticate prior source bytes.
This reuses diagnostic input, not a selected or approved production SHA backend.

## Required gates attempted, unavailable

Each gate was invoked afresh on this supplement checkout:

| Command | Exit | Wall seconds |
|---|---:|---:|
| `make lint` | 2 | 0.028 |
| `make unit` | 2 | 0.015 |
| `make dist-smoke` | 2 | 0.029 |
| `make build` | 2 | 0.032 |
| `make linux-qualification` | 2 | 0.022 |

All stopped before their work with:

```text
Riverhog Makefile targets require mise on PATH, or MISE_BIN=/abs/path/to/mise.
Install mise or pass MISE_BIN explicitly, then rerun make.
```

There is no claim that those gates passed. The original full-core schema/runtime
probe remains unperformed here; its previously reported missing `rfc8785` has
not been bypassed. Original model/schema files and their historical validation
record remain byte-identical to the preceding reference.

## Still required at the production boundary

Real canonical runtime/profile/corpus acceptance; reviewed durable hash and
semantic continuation; actual checkpoint/extent format vectors; exact member
proofs; encryption/provider fencing and calls; PostgreSQL and installed recovery;
permission/revocation races; complete copy/retirement/GC and source-release
lifecycles; physical memory/scratch/durability; declared schema/graph/index/backend
costs; current-source growth declarations and their actual qualification consumer.

The toy graph uses integer identities and in-memory fixture preparation. Native
radix tests use an in-memory store. Suffix tests use a toy hash chain. Standalone
output duplication and retained-state formulas are design analysis, not executed
production measurements. The experiments must not be attached to a production
promise as sufficient evidence merely because 32 tests pass.

Publication verifies exact file/tree/parent/branch identity and reference-only
changes separately in #969. Exact-SHA Actions/status observations are recorded
there after pushing; absent checks are not green CI. Native Development linkage,
when unavailable, is explicitly disclosed rather than simulated by a closing PR.

# NON-AUTHORITATIVE TRIAGE REFERENCE

Issue: [#897](https://github.com/nashspence/riverhog/issues/897)

Convention: [#903](https://github.com/nashspence/riverhog/issues/903)

Audited main base: `1b6ee5e8a2f1df39852465050df3efa96d6773e4`

This branch is concrete design/implementation input, not a supported contract,
a release requirement, or authorization to merge. The accepted JCS direction in
#897 does not make this experiment's codecs, admission policy, or placement an
accepted design. The owning issue and subsequent maintainer decisions control.
Reconcile against then-current main; do not integrate this branch mechanically
or open a PR directly from it. The exact reference commit linked from #897 is
the handoff identity; the branch name is navigation only.

## Scope and contents

**ON-01/02 only.** All additions are under `triage/897-on-01-02/`.
Production packages, their dependencies, schemas, migrations, fixtures, generated
contract closure, release configuration, and final freeze are unchanged.
ON-03–20 are not implemented. No digest-field renames, new identity subjects,
array reordering, path normalization, compatibility aliases, or journal rewrites.

`identity-domains.md` is a pinned, evidence-qualified integration search map,
not a new maintained inventory for main. `prototype.py` wraps `rfc8785`, rejects
information loss at raw parsing, implements explicit scalar codecs, and provides
an upload-volume producer/schema/consumer slice. `vectors.json` stores portable
raw lexemes, expected canonical bytes as UTF-8 strings and SHA-256 values,
IEEE-754 bit patterns, and acceptance/rejection cases. `test_reference.py` and
`node_oracle.mjs` execute them without importing any product or reference-app
implementation. The oracle is test-only, not a second supported canonicalizer.

## Run

From a checkout of the exact reference commit, with Python 3.13 and Node 22:

```sh
cd triage/897-on-01-02
python -m venv /tmp/riverhog-897-reference
/tmp/riverhog-897-reference/bin/python -m pip install -r requirements.txt
REFERENCE_NODE=1 /tmp/riverhog-897-reference/bin/python test_reference.py
```

Without `REFERENCE_NODE=1`, the Python vectors run and the Node comparison is
explicitly skipped. With it, missing Node or an oracle failure fails the run.
There is no network access or write to archive data in the tests.

## Prototype numeric policy, not a new definition of JCS

There is **one JCS serializer**. Semantic input domains are a separate concern.
The serializer takes native floats as already-declared binary64 values. It cannot
recover a number lexeme rounded by an earlier parser, or duplicate object names
already collapsed into a dictionary. Strict admission must precede those steps.

The raw parser retains every number lexeme and rejects duplicate decoded names
at every depth. Its default is an exact numeric domain; only caller-supplied,
schema-selected JSON Pointer paths opt into ordinary binary64 rounding. The
allowlist is a stand-in for explicit field/schema declarations, not an untrusted
request option, automatic schema interpreter, or invitation to coerce opaque data.
Thus `0.1` is rejected by the default exact policy, but accepted at a declared
binary64 path. **RFC 8785 does not prohibit `0.1`.** Exact decimals not representable
under this policy need an explicitly owned string encoding; a universal decimal
codec is not specified by this experiment.

The exact domain checks both binary64 representability and the emitted JCS
number's decimal value. `2**53` and `2**53 + 2` pass; `2**53 + 1` does not. Even an
exact binary64 integer such as `2**63` may have a JCS decimal spelling that differs
from that integer. Similarly, the exact binary64 fraction `1424953923781206.25`
is serialized as `1424953923781206.2`. These values are valid in a declared
binary64 domain, but not safe interchangeable exact-decimal identities. These
additional rejection rules belong to the prototype admission policy, not JCS.

Named exact fields use one representation across their **entire** domain:

| Prototype semantic domain | Wire representation | Capacity |
| --- | --- | --- |
| Provenance sequence | Canonical unsigned decimal string | 0 through 2**63 - 1 |
| Archive/upload volume ordinal | Exactly 64 lowercase hexadecimal characters | 0 through 2**256 - 1 |
| Nonnegative exact count/size/offset | Canonical unsigned decimal string | No new semantic ceiling |

Internal values remain integers. There is no small-number/large-string switch
and no automatic stringify-all-integers traversal. The ordinal codec matches the
existing archive representation and the upload slice verifies the existing
`kind`/`sequence`/`volume_id` binding. It is not a replacement for the entire upload
protocol. The provenance example is a field projection, not a full journal.

String schemas are generated from the same domain definitions. The 63-bit range
is expressed as a bounded decimal-language pattern, not an imprecise huge JSON
`maximum`. Patterns use an absolute end assertion compatible with both Python and
ECMAScript; a final newline must not pass. Do **not** replace an arbitrary JSON
Schema `maximum` with a string: that changes or invalidates the schema's meaning.
The test rejects such an unconverted schema and verifies the original is intact.

I-JSON character checks reject unpaired surrogates and noncharacters. JCS retains
Unicode code points, uses UTF-16 property ordering, and leaves arrays in order.
The 1 MiB default input budget and 64-level nesting budget are prototype carrier
limits, not newly proposed limits on repository logical totals. Owners must map
existing carrier budgets during integration rather than adopting these defaults
blindly. Unbounded decimal codecs avoid Python's decimal digit-conversion ceiling
without changing interpreter-global settings; callers still bound each message.

## Integrity boundary

`recorded_json_sha256` accepts an **already extracted** journal entry's recorded
JSON-text bytes. It neither parses nor canonicalizes them. The caller retains
ownership of RFC 7464 framing, schema, chain, and predecessor checks. It is a
hash-domain guard, not a production journal verifier. The tests show equivalent
JSON values with different recorded hashes, distinguish framing from entry bytes,
and leave a large-sequence inherited byte string unchanged.

New canonical emission does not authorize rewriting an inherited prefix or
changing an existing archive, encrypted root, signed object, or byte commitment.
Likewise, JCS does not choose self-digest exclusion, omitted nulls, defaults,
length prefixes, array/set ordering, or the object a digest identifies. The
ordered custody-object test retains the existing eight-byte big-endian length
prefix rather than turning the commitment into a hash of a JSON array.

## Integration input, not a completed production hard cut

A focused reusable library could own strict raw admission, JCS, and scalar codec
primitives. Riverhog must not import Stove0 implementation modules, and recovery
must not gain server/client/database dependencies. Concrete package ownership
and exact field coverage remain for the owning issue and normal integration rail.
Each integration slice must change the producer, raw parser, semantic model,
canonicality check, digest consumer, executable schema, and fixture together.
For HTTP, parsing must happen before default JSON/Pydantic conversion loses
information; encoding an already-parsed model is not a duplicate-key defense.

The map identifies closure-generation surfaces for later coordination, but this
reference neither regenerates them nor performs the final boundary rebaseline,
#469 qualification, release/v1 advancement, publication, or any ON-03–20 work.
Private data cutovers and historical-byte verification require explicit ownership;
this directory contains no migration utility.

## Validation evidence and limits

Local execution passed **11 test methods**, covering **118 checked-in fixtures**
(54 raw JSON, all 26 RFC 8785 Appendix B bit-pattern cases, 38 scalar cases),
additional domain/binding/integrity/budget tests, and a Node differential over
**10,000 seeded random bit patterns** plus the 26 fixed cases. Accepted raw JSON
fixtures and all scalar regex vectors were also compared with Node. This is
sampled differential evidence, not an exhaustive proof of binary64 formatting.

Execution used Python 3.13, Node v22.16.0, jsonschema 4.26.0, and rfc8785 0.1.4.
Package installation and a full repository clone were unavailable in the execution
environment. For local tests only, the rfc8785 source was fetched through GitHub
and reconstructed outside this directory, with exact Git blob identity verified:
`src/rfc8785/_impl.py` = `3137d3326b98938affadb1be711ee411eb2ab86e`;
`src/rfc8785/__init__.py` = `5a1f9d919643fa3bcaa0999ea66d9c535568c42a`.
No dependency source is vendored here. A clean dependency installation remains a
separate reproducibility check. Full repository `make lint`, `make unit`,
`make dist-smoke`, and `make build` were **not run**. Pushed-commit CI observations
belong in the owning issue's handoff comment; local success is not release evidence.

Standards and implementation references: [RFC 8785](https://www.rfc-editor.org/rfc/rfc8785.html),
[RFC 7493](https://www.rfc-editor.org/rfc/rfc7493.html), and
[rfc8785.py v0.1.4](https://github.com/trailofbits/rfc8785.py/tree/v0.1.4).

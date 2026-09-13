# stove0_protocol.ArtifactSelection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactselection:f06645b268 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9819eb3e2f"></a>
| Field | Shape |
|---|---|
| <a id="s-8c007fd171"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-550d2794ad"></a>`distribution` | "stove0-protocol" |
| <a id="s-12d29f7299"></a>`module` | "stove0_protocol" |
| <a id="s-57b5fb5e53"></a>`name` | "ArtifactSelection" |
| <a id="s-a53c8acf0d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.ArtifactSelection.canonical_artifacts](stove0-protocol-artifactselection-canonical-artifacts.md)
- [stove0_protocol.ArtifactSelection.canonical_bytes](stove0-protocol-artifactselection-canonical-bytes.md)
- [stove0_protocol.ArtifactSelection.ref](stove0-protocol-artifactselection-ref.md)
- [stove0_protocol.ArtifactSelection.roots](stove0-protocol-artifactselection-roots.md)
- [stove0_protocol.ArtifactSelection.seal](stove0-protocol-artifactselection-seal.md)
- [stove0_protocol.ArtifactSelection.verify_summary_and_digest](stove0-protocol-artifactselection-verify-summary-and-digest.md)

## Governing policies

- <a id="pa-3f4d54c4f3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSelection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22c976a611b86556ad195e185b1eefe5e3d4494f6157adf49d1f492988752644 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "ddd13318263dfef1cbb2e7911190fc38edd7b4dec41df377a17edcb68cbc328d",
    "signature": "\"(*, format: Literal['stove0-artifact-selection/v1'] = 'stove0-artifact-selection/v1', artifacts: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[int, Ge(ge=0)], selection_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ArtifactSelection",
  "unit": "export"
}
```

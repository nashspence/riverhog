# stove0_protocol.ArtifactSelectionRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactselectionref:5c6b257eba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-49de63b382"></a>
| Field | Shape |
|---|---|
| <a id="s-75236df783"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-253aae5a09"></a>`distribution` | "stove0-protocol" |
| <a id="s-62f6917187"></a>`module` | "stove0_protocol" |
| <a id="s-e22a721a43"></a>`name` | "ArtifactSelectionRef" |
| <a id="s-b8d88cd133"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.ArtifactSelectionRef.from_selection](stove0-protocol-artifactselectionref-from-selection.md)

## Governing policies

- <a id="pa-ef659a0767"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSelectionRef`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c7358ba78bfbd1d4c30184ca8845e18c11863babde79c35af7cac8085ab320b4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "e8cd0eea202ee0c8f3d19552216aa1b0d50521d431df3a185e911bfe1c3aa4c7",
    "signature": "\"(*, selection_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[int, Ge(ge=0)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ArtifactSelectionRef",
  "unit": "export"
}
```

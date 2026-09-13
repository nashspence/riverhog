# riverhog_protocol.CollectionUploadUnitAssignmentDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadunitass-1298f1453d:0e02024475 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6c34514b88"></a>
| Field | Shape |
|---|---|
| <a id="s-b8eeb4a6a2"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-479d0f76ec"></a>`distribution` | "riverhog-protocol" |
| <a id="s-979066d2d0"></a>`module` | "riverhog_protocol" |
| <a id="s-2b5d1d766e"></a>`name` | "CollectionUploadUnitAssignmentDocument" |
| <a id="s-d2930ada45"></a>`unit` | "export" |

## Governing policies

- <a id="pa-83e124d136"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadUnitAssignmentDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a8cd23279fe058b3757ae40f82f5c4ff0d19feb5927b1849bbb9bd5238fa80a7 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "ec544995264910195920c2079dde8ea57cf4fc881161b190480f7b4f779caa00",
    "signature": "\"(*, volume: riverhog_protocol.collection_upload_transport.CollectionUploadVolumeSummaryDocument, plan_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], unit: riverhog_protocol.collection_upload_transport.CollectionUploadUnitWorkDocument) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadUnitAssignmentDocument",
  "unit": "export"
}
```

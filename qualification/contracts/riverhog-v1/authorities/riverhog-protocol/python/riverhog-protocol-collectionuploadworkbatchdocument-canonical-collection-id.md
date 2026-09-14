# riverhog_protocol.CollectionUploadWorkBatchDocument.canonical_collection_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadworkbat-ae6fb3bbf2:ca659ac6df -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8978cf4334"></a>
- <a id="s-f102051e3a"></a>`distribution`: `riverhog-protocol`
- <a id="s-04340be555"></a>`module`: `riverhog_protocol`
- <a id="s-b4787f172f"></a>`name`: `canonical_collection_id`
- <a id="s-c57abd6bb6"></a>`owner`: `riverhog_protocol.CollectionUploadWorkBatchDocument`
- <a id="s-50864a3511"></a>`unit`: `member`

### Declared structure

- <a id="s-d5f984d1bd"></a>`kind`: `"classmethod"`
- <a id="s-74c333ab6f"></a>`signature`: `"\"(cls, value: 'int') -> 'int'\""`

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadWorkBatchDocument](riverhog-protocol-collectionuploadworkbatchdocument.md)

## Governing policies

- <a id="pa-fcdb3514cf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadWorkBatchDocument.canonical_collection_id`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b4ce4d1658bd1047269aa37635af27eafc65d1f544aa3625c3896774f157b4ea -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'int') -> 'int'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "canonical_collection_id",
  "owner": "riverhog_protocol.CollectionUploadWorkBatchDocument",
  "unit": "member"
}
```

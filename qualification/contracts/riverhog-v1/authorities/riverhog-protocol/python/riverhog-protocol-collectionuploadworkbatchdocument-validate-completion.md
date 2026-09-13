# riverhog_protocol.CollectionUploadWorkBatchDocument.validate_completion

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadworkbat-79a4e0437b:9b77eb28d1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0e648f9134"></a>
| Field | Shape |
|---|---|
| <a id="s-273bed867e"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-6cb4187a24"></a>`distribution` | "riverhog-protocol" |
| <a id="s-f74bcdcc26"></a>`module` | "riverhog_protocol" |
| <a id="s-38a857bc63"></a>`name` | "validate_completion" |
| <a id="s-7572f5b8fc"></a>`owner` | "riverhog_protocol.CollectionUploadWorkBatchDocument" |
| <a id="s-331b0fba61"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadWorkBatchDocument](riverhog-protocol-collectionuploadworkbatchdocument.md)

## Governing policies

- <a id="pa-f53a323a3d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadWorkBatchDocument.validate_completion`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 911dc82f0cb26a98b575ffda313b579170744d1d40ae476f40eef3e9382f0c03 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_completion",
  "owner": "riverhog_protocol.CollectionUploadWorkBatchDocument",
  "unit": "member"
}
```

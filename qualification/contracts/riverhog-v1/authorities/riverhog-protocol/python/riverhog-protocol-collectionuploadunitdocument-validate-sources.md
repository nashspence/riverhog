# riverhog_protocol.CollectionUploadUnitDocument.validate_sources

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadunitdoc-3418295d46:1af2fd8ecd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-be50524c8a"></a>
| Field | Shape |
|---|---|
| <a id="s-2986a7d18e"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-7576dac2d2"></a>`distribution` | "riverhog-protocol" |
| <a id="s-b40e59c08a"></a>`module` | "riverhog_protocol" |
| <a id="s-59336c568a"></a>`name` | "validate_sources" |
| <a id="s-0e6f3665cb"></a>`owner` | "riverhog_protocol.CollectionUploadUnitDocument" |
| <a id="s-de50b788a7"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadUnitDocument](riverhog-protocol-collectionuploadunitdocument.md)

## Governing policies

- <a id="pa-c7dca1cc14"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadUnitDocument.validate_sources`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: be53480f28b29f2c2c5f6adcdbe8a742cd4f987c46f550cc76c676f97632d9e2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_sources",
  "owner": "riverhog_protocol.CollectionUploadUnitDocument",
  "unit": "member"
}
```

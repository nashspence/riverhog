# riverhog_storage_adapter_protocol.ObjectMetadataReceipt.canonical_metadata

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectm-9dea699128:b96854788f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ae12a120ab"></a>
- <a id="s-051e88f664"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-e44cc4300d"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-d6ece44e2d"></a>`name`: `canonical_metadata`
- <a id="s-cf0093f62a"></a>`owner`: `riverhog_storage_adapter_protocol.ObjectMetadataReceipt`
- <a id="s-4e75866518"></a>`unit`: `member`

### Declared structure

- <a id="s-907745acfd"></a>`kind`: `"classmethod"`
- <a id="s-d302211747"></a>`signature`: `"\"(cls, value: 'dict[str, str]') -> 'dict[str, str]'\""`

## Maintained corroboration

### Related interface records

- [ObjectMetadataReceipt](riverhog-storage-adapter-protocol-objectmetadatareceipt.md)

## Governing policies

- <a id="pa-9a51802360"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectMetadataReceipt.canonical_metadata`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ebaef261c5ee9a2b9f80d475d35eb3b3af24edf04c829598b1ad5033e1ceeb3 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'dict[str, str]') -> 'dict[str, str]'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_metadata",
  "owner": "riverhog_storage_adapter_protocol.ObjectMetadataReceipt",
  "unit": "member"
}
```

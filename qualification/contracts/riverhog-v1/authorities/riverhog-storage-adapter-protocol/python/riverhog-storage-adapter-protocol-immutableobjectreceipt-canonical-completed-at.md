# riverhog_storage_adapter_protocol.ImmutableObjectReceipt.canonical_completed_at

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-immutab-9115f74857:1c65b1bc67 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-056a6010b2"></a>
| Field | Shape |
|---|---|
| <a id="s-37e975d772"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-5cdc3921e7"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-0861682819"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-d7dcfcce3b"></a>`name` | "canonical_completed_at" |
| <a id="s-35129f5db4"></a>`owner` | "riverhog_storage_adapter_protocol.ImmutableObjectReceipt" |
| <a id="s-6ef76f611d"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ImmutableObjectReceipt](riverhog-storage-adapter-protocol-immutableobjectreceipt.md)

## Governing policies

- <a id="pa-d6441bcc4d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ImmutableObjectReceipt.canonical_completed_at`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2e6fbc9912a233ec447d907490a1007f1e323503c4d14eb246f8c527b454f2fc -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_completed_at",
  "owner": "riverhog_storage_adapter_protocol.ImmutableObjectReceipt",
  "unit": "member"
}
```

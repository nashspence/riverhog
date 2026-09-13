# riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.put_small_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-22f3b00b86:4a2c48d56b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2e8bf132f0"></a>
| Field | Shape |
|---|---|
| <a id="s-3ef3738ed8"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-99f4c49da3"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-9da3609b45"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-5129dbdee8"></a>`name` | "put_small_object" |
| <a id="s-22f06b1dcc"></a>`owner` | "riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort" |
| <a id="s-4996f99366"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort](riverhog-storage-adapter-protocol-validatedstorageadapterport.md)

## Governing policies

- <a id="pa-4f0b07c9a4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.put_small_object`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c3356c444d841c10f0564efdb31264dc277458d57502de1af6b3164dc02f2352 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'SmallObjectWriteRequest', content: 'BinaryContent') -> 'ImmutableObjectReceipt'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "put_small_object",
  "owner": "riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort",
  "unit": "member"
}
```

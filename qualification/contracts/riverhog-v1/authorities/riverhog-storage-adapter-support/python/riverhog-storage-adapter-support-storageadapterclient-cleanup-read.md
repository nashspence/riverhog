# riverhog_storage_adapter_support.StorageAdapterClient.cleanup_read

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-d5e6e01cb8:b6f9ab0976 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8fc49bfc2c"></a>
| Field | Shape |
|---|---|
| <a id="s-edf436c13f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-47ad9f11f2"></a>`distribution` | "riverhog-storage-adapter-support" |
| <a id="s-5bdaf3e8d0"></a>`module` | "riverhog_storage_adapter_support" |
| <a id="s-0a861fa88a"></a>`name` | "cleanup_read" |
| <a id="s-0cb7ced726"></a>`owner` | "riverhog_storage_adapter_support.StorageAdapterClient" |
| <a id="s-158713292e"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_support.StorageAdapterClient](riverhog-storage-adapter-support-storageadapterclient.md)

## Governing policies

- <a id="pa-45bb212166"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterClient.cleanup_read`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9c150a90aeaa72e7121592461814c515d9514a461a37743a863282bc37a8b84e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ReadPreparationRequest') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "cleanup_read",
  "owner": "riverhog_storage_adapter_support.StorageAdapterClient",
  "unit": "member"
}
```

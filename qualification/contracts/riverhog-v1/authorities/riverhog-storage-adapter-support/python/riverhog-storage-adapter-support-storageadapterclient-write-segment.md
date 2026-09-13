# riverhog_storage_adapter_support.StorageAdapterClient.write_segment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-23ece0d2e8:1afda6cad1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4c1e6ffc34"></a>
| Field | Shape |
|---|---|
| <a id="s-550ff1d468"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-18535cbe82"></a>`distribution` | "riverhog-storage-adapter-support" |
| <a id="s-e49019122a"></a>`module` | "riverhog_storage_adapter_support" |
| <a id="s-5ff55d6c36"></a>`name` | "write_segment" |
| <a id="s-4a31767612"></a>`owner` | "riverhog_storage_adapter_support.StorageAdapterClient" |
| <a id="s-ca71b7e1aa"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_support.StorageAdapterClient](riverhog-storage-adapter-support-storageadapterclient.md)

## Governing policies

- <a id="pa-c32d173c2f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterClient.write_segment`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 169242ba7096ad650fb493edbcf7824ffc293deb2f34c24cf5d5339d74dc4843 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, session: 'WriteSession', number: 'int', stored_bytes: 'int', content: 'BinaryContent') -> 'WriteSegmentReceipt'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "write_segment",
  "owner": "riverhog_storage_adapter_support.StorageAdapterClient",
  "unit": "member"
}
```

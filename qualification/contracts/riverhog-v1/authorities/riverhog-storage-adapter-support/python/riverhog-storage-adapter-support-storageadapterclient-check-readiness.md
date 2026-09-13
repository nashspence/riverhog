# riverhog_storage_adapter_support.StorageAdapterClient.check_readiness

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-a516b5dc19:67fba56144 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7df75e928d"></a>
| Field | Shape |
|---|---|
| <a id="s-22096e564a"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-c2f218397c"></a>`distribution` | "riverhog-storage-adapter-support" |
| <a id="s-67d04fa60f"></a>`module` | "riverhog_storage_adapter_support" |
| <a id="s-63d7096099"></a>`name` | "check_readiness" |
| <a id="s-86cc0dd8a2"></a>`owner` | "riverhog_storage_adapter_support.StorageAdapterClient" |
| <a id="s-b7c19f7bb8"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_support.StorageAdapterClient](riverhog-storage-adapter-support-storageadapterclient.md)

## Governing policies

- <a id="pa-dbc3d40e2c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterClient.check_readiness`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a04dfb511ab18ada5ca54824edd2a934a55aac281e3a24fbb1bd980f71d0f482 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "check_readiness",
  "owner": "riverhog_storage_adapter_support.StorageAdapterClient",
  "unit": "member"
}
```

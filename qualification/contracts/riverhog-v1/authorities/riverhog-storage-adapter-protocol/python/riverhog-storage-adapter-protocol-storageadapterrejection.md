# riverhog_storage_adapter_protocol.StorageAdapterRejection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storage-94541be79e:d7541a735c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-da060e9b5a"></a>
| Field | Shape |
|---|---|
| <a id="s-c917347c81"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-9f291ef63e"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-7e3f16d693"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-106c96fb70"></a>`name` | "StorageAdapterRejection" |
| <a id="s-9b58bd22dd"></a>`unit` | "export" |

## Governing policies

- <a id="pa-d9783e7ab3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterRejection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8dbe2454518733d2c15c09a14ec7c732110705cfd43c050cc06f6d36c3555e69 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(code: 'StorageAdapterErrorCode', message: 'str') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "StorageAdapterRejection",
  "unit": "export"
}
```

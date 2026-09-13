# riverhog_storage_adapter_protocol.MaintenanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-maintenanceresult:07072a2098 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ed8d421fa7"></a>
| Field | Shape |
|---|---|
| <a id="s-1a793910d3"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-939249e3ca"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-26af95f59e"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-5e932f383e"></a>`name` | "MaintenanceResult" |
| <a id="s-f628dee5c7"></a>`unit` | "export" |

## Governing policies

- <a id="pa-fad0f8f5cc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.MaintenanceResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 791c2072a052da8aaf0dfe25cf27efea76c988e5b11563b0f093b3832e6b71b9 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "1b8ad376bb41a2eb81c64450d4ccb99a09150f8f1c72f6203ecdad2b933a565c",
    "signature": "'(*, affected: Annotated[int, Ge(ge=0)]) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "MaintenanceResult",
  "unit": "export"
}
```

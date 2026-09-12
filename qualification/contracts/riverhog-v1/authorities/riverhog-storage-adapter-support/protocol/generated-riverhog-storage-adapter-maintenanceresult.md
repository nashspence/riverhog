# generated:riverhog-storage-adapter: MaintenanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-maintenanceresult:0f44d30e3c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-720226b55488"></a>
- <a id="s-9838cbaddba9"></a>`title`: MaintenanceResult
- <a id="s-c0dba656c0c1"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-11316610fc0f"></a>`affected` | yes | type="integer"; minimum=0 |  |

## Governing policies

- <a id="pa-875318e10505"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471a9) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/MaintenanceResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1b8ad376bb41a2eb81c64450d4ccb99a09150f8f1c72f6203ecdad2b933a565c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "affected": {
      "minimum": 0,
      "title": "Affected",
      "type": "integer"
    }
  },
  "required": [
    "affected"
  ],
  "title": "MaintenanceResult",
  "type": "object"
}
```

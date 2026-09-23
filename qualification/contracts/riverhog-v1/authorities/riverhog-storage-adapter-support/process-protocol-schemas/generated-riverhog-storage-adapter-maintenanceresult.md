# generated:riverhog-storage-adapter: MaintenanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-maintenanceresult:7138d08d6b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-720226b554"></a>

- <a id="s-c0dba656c0"></a>`type`: `"object"`
- <a id="s-c39891a1cb"></a>`additionalProperties`: `false`
- <a id="s-cec86f880b"></a>`required`: `["affected"]`
- <a id="s-9838cbaddb"></a>`title`: `"MaintenanceResult"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-11316610fc"></a>`affected` | yes | [NonnegativeDecimal](#s-bf651de82e) |  |

### Definitions

- [NonnegativeDecimal](#s-bf651de82e)

### <a id="s-bf651de82e"></a>definition `NonnegativeDecimal`

- <a id="s-6948288d68"></a>`type`: `"string"`
- <a id="s-d1abdfab54"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-5c150aefbd"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/MaintenanceResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9ada78c00de844b2231a1e4f8424b57ffde21759484b2ad78c7c4f431fa8b83d -->

```json
{
  "$defs": {
    "NonnegativeDecimal": {
      "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
      "type": "string"
    }
  },
  "additionalProperties": false,
  "properties": {
    "affected": {
      "$ref": "#/$defs/NonnegativeDecimal"
    }
  },
  "required": [
    "affected"
  ],
  "title": "MaintenanceResult",
  "type": "object"
}
```

</details>

# generated:riverhog-storage-adapter: DeletePrefixRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-delete-ac2392c700:24fdf1c683 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: DeletePrefixRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `mode` | no | type="string"; const="all_versions" |  |
| `object_prefix` | yes | type="string"; minLength=1; maxLength=4096 |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |

## Governing policies

- `compatibility/components/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/DeletePrefixRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 26f1798a7e22e1a72e0c6932bfbe66dca3e80273a417c3e5c18fdc0e4bd59a3f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "mode": {
      "const": "all_versions",
      "default": "all_versions",
      "title": "Mode",
      "type": "string"
    },
    "object_prefix": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Object Prefix",
      "type": "string"
    }
  },
  "required": [
    "object_prefix"
  ],
  "title": "DeletePrefixRequest",
  "type": "object"
}
```

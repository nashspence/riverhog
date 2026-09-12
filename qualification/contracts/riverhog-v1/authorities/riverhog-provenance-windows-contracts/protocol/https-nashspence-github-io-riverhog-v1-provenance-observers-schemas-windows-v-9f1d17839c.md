# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-9f1d17839c:c6eda73b5b -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-volume-context.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-volume-context.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `bytes_per_sector` | yes | integer |  |
| `drive_type` | yes | integer |  |
| `filesystem_flags` | yes | integer |  |
| `filesystem_name` | yes | string |  |
| `final_path` | yes | string |  |
| `maximum_component_length` | yes | integer |  |
| `mount_path` | yes | string |  |
| `sectors_per_cluster` | yes | integer |  |
| `volume_guid_path` | yes | string |  |
| `volume_label` | yes | string |  |
| `volume_serial_number` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74c63c0471ae132d4e4c3c695e5cb9250813ff3c243d70d7636d8973ffb05481 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "bytes_per_sector": {
      "minimum": 0,
      "type": "integer"
    },
    "drive_type": {
      "minimum": 0,
      "type": "integer"
    },
    "filesystem_flags": {
      "minimum": 0,
      "type": "integer"
    },
    "filesystem_name": {
      "minLength": 1,
      "type": "string"
    },
    "final_path": {
      "type": "string"
    },
    "maximum_component_length": {
      "minimum": 0,
      "type": "integer"
    },
    "mount_path": {
      "type": "string"
    },
    "sectors_per_cluster": {
      "minimum": 0,
      "type": "integer"
    },
    "volume_guid_path": {
      "type": "string"
    },
    "volume_label": {
      "type": "string"
    },
    "volume_serial_number": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "filesystem_name",
    "volume_label",
    "volume_serial_number",
    "maximum_component_length",
    "filesystem_flags",
    "mount_path",
    "volume_guid_path",
    "drive_type",
    "sectors_per_cluster",
    "bytes_per_sector",
    "final_path"
  ],
  "type": "object"
}
```

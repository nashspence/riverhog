# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-448544fd06:693d7e9b24 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 11 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-file-stat.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-file-stat.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `allocation_size` | yes | integer |  |
| `change_time_ticks` | yes | integer |  |
| `creation_time_ticks` | yes | integer |  |
| `delete_pending` | yes | boolean |  |
| `end_of_file` | yes | integer |  |
| `file_attributes` | yes | integer |  |
| `file_id_bits` | yes | integer |  |
| `file_id_hex` | yes | string |  |
| `file_id_scheme` | yes | object (1 fields) |  |
| `file_index_64` | yes | integer |  |
| `last_access_time_ticks` | yes | integer |  |
| `last_write_time_ticks` | yes | integer |  |
| `number_of_links` | yes | integer |  |
| `reparse_tag` | yes | integer |  |
| `storage` | yes | object |  |
| `volume_serial_number` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7e1f4e64443ea706e814ef0b3eb7e5767f43c246072c6e8a14b3787b0d5aa7a -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "allocation_size": {
      "minimum": 0,
      "type": "integer"
    },
    "change_time_ticks": {
      "type": "integer"
    },
    "creation_time_ticks": {
      "type": "integer"
    },
    "delete_pending": {
      "type": "boolean"
    },
    "end_of_file": {
      "minimum": 0,
      "type": "integer"
    },
    "file_attributes": {
      "minimum": 0,
      "type": "integer"
    },
    "file_id_bits": {
      "enum": [
        64,
        128
      ],
      "type": "integer"
    },
    "file_id_hex": {
      "pattern": "^[0-9a-f]+$",
      "type": "string"
    },
    "file_id_scheme": {
      "enum": [
        "windows-file-id-128",
        "windows-file-index-64"
      ]
    },
    "file_index_64": {
      "minimum": 0,
      "type": "integer"
    },
    "last_access_time_ticks": {
      "type": "integer"
    },
    "last_write_time_ticks": {
      "type": "integer"
    },
    "number_of_links": {
      "minimum": 0,
      "type": "integer"
    },
    "reparse_tag": {
      "minimum": 0,
      "type": "integer"
    },
    "storage": {
      "additionalProperties": false,
      "properties": {
        "byte_offset_for_partition_alignment": {
          "minimum": 0,
          "type": "integer"
        },
        "byte_offset_for_sector_alignment": {
          "minimum": 0,
          "type": "integer"
        },
        "filesystem_effective_physical_bytes_per_sector_for_atomicity": {
          "minimum": 0,
          "type": "integer"
        },
        "flags": {
          "minimum": 0,
          "type": "integer"
        },
        "logical_bytes_per_sector": {
          "minimum": 0,
          "type": "integer"
        },
        "physical_bytes_per_sector_for_atomicity": {
          "minimum": 0,
          "type": "integer"
        },
        "physical_bytes_per_sector_for_performance": {
          "minimum": 0,
          "type": "integer"
        }
      },
      "type": "object"
    },
    "volume_serial_number": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "volume_serial_number",
    "file_id_hex",
    "file_id_bits",
    "file_id_scheme",
    "file_index_64",
    "creation_time_ticks",
    "last_access_time_ticks",
    "last_write_time_ticks",
    "change_time_ticks",
    "file_attributes",
    "reparse_tag",
    "allocation_size",
    "end_of_file",
    "number_of_links",
    "delete_pending",
    "storage"
  ],
  "type": "object"
}
```

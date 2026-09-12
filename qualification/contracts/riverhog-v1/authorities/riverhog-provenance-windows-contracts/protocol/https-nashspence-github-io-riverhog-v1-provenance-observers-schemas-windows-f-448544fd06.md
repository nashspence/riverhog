# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-448544fd06:693d7e9b24 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 11 |

## External contract

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `allocation_size` | yes | type="integer"; minimum=0 |  |
| `change_time_ticks` | yes | type="integer" |  |
| `creation_time_ticks` | yes | type="integer" |  |
| `delete_pending` | yes | type="boolean" |  |
| `end_of_file` | yes | type="integer"; minimum=0 |  |
| `file_attributes` | yes | type="integer"; minimum=0 |  |
| `file_id_bits` | yes | type="integer"; enum=[64,128] |  |
| `file_id_hex` | yes | type="string"; pattern="^[0-9a-f]+$" |  |
| `file_id_scheme` | yes | enum=["windows-file-id-128","windows-file-index-64"] |  |
| `file_index_64` | yes | type="integer"; minimum=0 |  |
| `last_access_time_ticks` | yes | type="integer" |  |
| `last_write_time_ticks` | yes | type="integer" |  |
| `number_of_links` | yes | type="integer"; minimum=0 |  |
| `reparse_tag` | yes | type="integer"; minimum=0 |  |
| `storage` | yes | type="object"; fields=`byte_offset_for_partition_alignment`, `byte_offset_for_sector_alignment`, `filesystem_effective_physical_bytes_per_sector_for_atomicity`, `flags`, `logical_bytes_per_sector`, `physical_bytes_per_sector_for_atomicity`, `physical_bytes_per_sector_for_performance`; additional keys=`additionalProperties` |  |
| `volume_serial_number` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
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

## Governing policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-file-stat.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-file-stat.json`

### Exact owned JSON

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

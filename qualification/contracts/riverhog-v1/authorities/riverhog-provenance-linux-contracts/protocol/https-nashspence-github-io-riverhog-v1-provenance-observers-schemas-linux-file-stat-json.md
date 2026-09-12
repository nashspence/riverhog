# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-linux-contracts:https-nashspence-github-io-riverhog-v1-pr-453aae6637:501bf68468 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-linux-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 6 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1linux-file-stat.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json` — `reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/schemas/linux-file-stat.schema.json`
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

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `atomic_write_segments_max` | no | integer |  |
| `atomic_write_unit_max` | no | integer |  |
| `atomic_write_unit_max_opt` | no | integer |  |
| `atomic_write_unit_min` | no | integer |  |
| `blocks_512_bytes` | no | integer |  |
| `dev_major` | no | integer |  |
| `dev_minor` | no | integer |  |
| `device` | yes | integer |  |
| `dio_mem_align` | no | integer |  |
| `dio_offset_align` | no | integer |  |
| `dio_read_offset_align` | no | integer |  |
| `gid` | yes | integer |  |
| `inode` | yes | integer |  |
| `mode` | yes | string |  |
| `nlink` | yes | integer |  |
| `preferred_io_block_size` | no | integer |  |
| `rdev` | no | integer |  |
| `size` | yes | integer |  |
| `statx_attributes` | no | integer |  |
| `statx_attributes_mask` | no | integer |  |
| `statx_available` | no | boolean |  |
| `statx_mask` | no | integer |  |
| `subvolume_id` | no | integer |  |
| `uid` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61b29c01570ddab284846ec05646a585a9c760696ebafc145fcb9987de606321 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "atomic_write_segments_max": {
      "minimum": 0,
      "type": "integer"
    },
    "atomic_write_unit_max": {
      "minimum": 0,
      "type": "integer"
    },
    "atomic_write_unit_max_opt": {
      "minimum": 0,
      "type": "integer"
    },
    "atomic_write_unit_min": {
      "minimum": 0,
      "type": "integer"
    },
    "blocks_512_bytes": {
      "minimum": 0,
      "type": "integer"
    },
    "dev_major": {
      "minimum": 0,
      "type": "integer"
    },
    "dev_minor": {
      "minimum": 0,
      "type": "integer"
    },
    "device": {
      "minimum": 0,
      "type": "integer"
    },
    "dio_mem_align": {
      "minimum": 0,
      "type": "integer"
    },
    "dio_offset_align": {
      "minimum": 0,
      "type": "integer"
    },
    "dio_read_offset_align": {
      "minimum": 0,
      "type": "integer"
    },
    "gid": {
      "minimum": 0,
      "type": "integer"
    },
    "inode": {
      "minimum": 0,
      "type": "integer"
    },
    "mode": {
      "pattern": "^[0-7]+$",
      "type": "string"
    },
    "nlink": {
      "minimum": 0,
      "type": "integer"
    },
    "preferred_io_block_size": {
      "minimum": 0,
      "type": "integer"
    },
    "rdev": {
      "minimum": 0,
      "type": "integer"
    },
    "size": {
      "minimum": 0,
      "type": "integer"
    },
    "statx_attributes": {
      "minimum": 0,
      "type": "integer"
    },
    "statx_attributes_mask": {
      "minimum": 0,
      "type": "integer"
    },
    "statx_available": {
      "type": "boolean"
    },
    "statx_mask": {
      "minimum": 0,
      "type": "integer"
    },
    "subvolume_id": {
      "minimum": 0,
      "type": "integer"
    },
    "uid": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "device",
    "inode",
    "mode",
    "nlink",
    "uid",
    "gid",
    "size"
  ],
  "type": "object"
}
```

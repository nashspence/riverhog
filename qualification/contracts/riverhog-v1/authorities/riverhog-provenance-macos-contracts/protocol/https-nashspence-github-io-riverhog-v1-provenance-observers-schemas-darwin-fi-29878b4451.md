# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-macos-contracts:https-nashspence-github-io-riverhog-v1-pr-29878b4451:2f76ebda03 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-macos-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1darwin-file-stat.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json` — `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/schemas/darwin-file-stat.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `blocks_512_bytes` | no | integer |  |
| `device` | yes | integer |  |
| `flags` | no | integer |  |
| `generation` | no | integer |  |
| `gid` | yes | integer |  |
| `inode` | yes | integer |  |
| `mode` | yes | string |  |
| `nlink` | yes | integer |  |
| `preferred_io_block_size` | no | integer |  |
| `rdev` | no | integer |  |
| `size` | yes | integer |  |
| `uid` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a9731ecced70d231000fe7771ba7028735379cf2b56a77864b38177c51409de9 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "blocks_512_bytes": {
      "minimum": 0,
      "type": "integer"
    },
    "device": {
      "minimum": 0,
      "type": "integer"
    },
    "flags": {
      "minimum": 0,
      "type": "integer"
    },
    "generation": {
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

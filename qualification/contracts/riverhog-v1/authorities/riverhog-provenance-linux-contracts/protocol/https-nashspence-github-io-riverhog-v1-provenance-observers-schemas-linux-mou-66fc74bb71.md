# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-linux-contracts:https-nashspence-github-io-riverhog-v1-pr-66fc74bb71:2727c47cb7 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-linux-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1linux-mount-context.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json` — `reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/schemas/linux-mount-context.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| cardinality | items | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| cardinality | items | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `device` | yes | string |  |
| `filesystem_type` | yes | string |  |
| `mount_id` | yes | integer |  |
| `mount_options` | yes | array |  |
| `mount_point` | yes | string |  |
| `optional_fields` | yes | array |  |
| `parent_id` | yes | integer |  |
| `root` | yes | string |  |
| `source` | yes | string |  |
| `super_options` | yes | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0443a380bfbe8e785650c1ee81ec9e13f188682a1e33f80dc7e4df05a087bc0 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "device": {
      "type": "string"
    },
    "filesystem_type": {
      "minLength": 1,
      "type": "string"
    },
    "mount_id": {
      "minimum": 0,
      "type": "integer"
    },
    "mount_options": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "mount_point": {
      "type": "string"
    },
    "optional_fields": {
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "parent_id": {
      "minimum": 0,
      "type": "integer"
    },
    "root": {
      "type": "string"
    },
    "source": {
      "type": "string"
    },
    "super_options": {
      "items": {
        "type": "string"
      },
      "type": "array"
    }
  },
  "required": [
    "mount_id",
    "parent_id",
    "device",
    "root",
    "mount_point",
    "mount_options",
    "optional_fields",
    "filesystem_type",
    "source",
    "super_options"
  ],
  "type": "object"
}
```

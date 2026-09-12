# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-linux-contracts:https-nashspence-github-io-riverhog-v1-pr-ce7cee8b8b:24f0a59fd8 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-linux-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1linux-fsxattr.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json` — `reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/schemas/linux-fsxattr.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| cardinality | items | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `cow_extent_size` | yes | integer |  |
| `extent_size` | yes | integer |  |
| `nextents` | yes | integer |  |
| `project_id` | yes | integer |  |
| `xflag_names` | yes | array |  |
| `xflags` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fa34df9dec8d9b0062148907b6cd8645a40bd941ffa212387bcbd09f922747b1 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "cow_extent_size": {
      "minimum": 0,
      "type": "integer"
    },
    "extent_size": {
      "minimum": 0,
      "type": "integer"
    },
    "nextents": {
      "minimum": 0,
      "type": "integer"
    },
    "project_id": {
      "minimum": 0,
      "type": "integer"
    },
    "xflag_names": {
      "items": {
        "type": "string"
      },
      "type": "array",
      "uniqueItems": true
    },
    "xflags": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "xflags",
    "xflag_names",
    "extent_size",
    "nextents",
    "project_id",
    "cow_extent_size"
  ],
  "type": "object"
}
```

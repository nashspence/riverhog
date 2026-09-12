# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-linux-contracts:https-nashspence-github-io-riverhog-v1-pr-8be8c78b69:0fe7178446 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-linux-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1linux-statx-attributes.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json` — `reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/schemas/linux-statx-attributes.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| cardinality | items | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `attributes` | yes | integer |  |
| `attributes_mask` | yes | integer |  |
| `set_names` | yes | array |  |
| `supported_names` | yes | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 56fc562c3d49ff461fc7c95a24a26dc1c57814bd8fce8bbaec9ceb2eff4f9351 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "attributes": {
      "minimum": 0,
      "type": "integer"
    },
    "attributes_mask": {
      "minimum": 0,
      "type": "integer"
    },
    "set_names": {
      "items": {
        "type": "string"
      },
      "type": "array",
      "uniqueItems": true
    },
    "supported_names": {
      "items": {
        "type": "string"
      },
      "type": "array",
      "uniqueItems": true
    }
  },
  "required": [
    "attributes",
    "attributes_mask",
    "set_names",
    "supported_names"
  ],
  "type": "object"
}
```

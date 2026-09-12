# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-macos-contracts:https-nashspence-github-io-riverhog-v1-pr-cda5d58e04:a24d1095dd -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-macos-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 8 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1darwin-file-attributes.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json` — `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/schemas/darwin-file-attributes.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | entries | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `allocation_size` | no | integer |  |
| `data_allocation_size` | no | integer |  |
| `data_length` | no | integer |  |
| `document_id` | no | integer |  |
| `generation` | no | integer |  |
| `io_block_size` | no | integer |  |
| `resource_fork_allocation_size` | no | integer |  |
| `resource_fork_length` | no | integer |  |
| `total_size` | no | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d93ccd741dbce58c24079da050ac8396123467f74d0e749607ce56e379106d5c -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "minProperties": 1,
  "properties": {
    "allocation_size": {
      "minimum": 0,
      "type": "integer"
    },
    "data_allocation_size": {
      "minimum": 0,
      "type": "integer"
    },
    "data_length": {
      "minimum": 0,
      "type": "integer"
    },
    "document_id": {
      "minimum": 0,
      "type": "integer"
    },
    "generation": {
      "minimum": 0,
      "type": "integer"
    },
    "io_block_size": {
      "minimum": 0,
      "type": "integer"
    },
    "resource_fork_allocation_size": {
      "minimum": 0,
      "type": "integer"
    },
    "resource_fork_length": {
      "minimum": 0,
      "type": "integer"
    },
    "total_size": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "type": "object"
}
```

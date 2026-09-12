# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-macos-contracts:https-nashspence-github-io-riverhog-v1-pr-cda5d58e04:a24d1095dd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-macos-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 8 |

## External contract

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `allocation_size` | no | type="integer"; minimum=0 |  |
| `data_allocation_size` | no | type="integer"; minimum=0 |  |
| `data_length` | no | type="integer"; minimum=0 |  |
| `document_id` | no | type="integer"; minimum=0 |  |
| `generation` | no | type="integer"; minimum=0 |  |
| `io_block_size` | no | type="integer"; minimum=0 |  |
| `resource_fork_allocation_size` | no | type="integer"; minimum=0 |  |
| `resource_fork_length` | no | type="integer"; minimum=0 |  |
| `total_size` | no | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | entries | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
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
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json` — `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/schemas/darwin-file-attributes.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1darwin-file-attributes.json`

### Exact owned JSON

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

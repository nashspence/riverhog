# Riverhog v1 authenticated provenance-volume terminator

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance:riverhog-v1-authenticated-provenance-volu-6f487d528f:e5302da766 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-provenance-terminal-v1.schema.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json` — `packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-terminal-v1.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json
- `title`: Riverhog v1 authenticated provenance-volume terminator
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_generation` | yes | #/$defs/sha256 |  |
| `archive_tree_sha256` | yes | #/$defs/sha256 |  |
| `kind` | yes | object (1 fields) |  |
| `schema` | yes | object (1 fields) |  |
| `sequence` | yes | string |  |

### Definitions

| Definition | Shape |
|---|---|
| `sha256` | string |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0adc252e32d2acdca092eeb7a30601d8ce840fa83722504eb594598f501ee97d -->

```json
{
  "$defs": {
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    }
  },
  "$id": "https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "archive_generation": {
      "$ref": "#/$defs/sha256"
    },
    "archive_tree_sha256": {
      "$ref": "#/$defs/sha256"
    },
    "kind": {
      "const": "terminal"
    },
    "schema": {
      "const": "riverhog-provenance-terminal/v1"
    },
    "sequence": {
      "not": {
        "const": "0000000000000000000000000000000000000000000000000000000000000000"
      },
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    }
  },
  "required": [
    "schema",
    "archive_generation",
    "archive_tree_sha256",
    "sequence",
    "kind"
  ],
  "title": "Riverhog v1 authenticated provenance-volume terminator",
  "type": "object"
}
```

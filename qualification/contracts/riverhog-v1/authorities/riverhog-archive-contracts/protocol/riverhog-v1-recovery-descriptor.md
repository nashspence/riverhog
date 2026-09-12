# Riverhog v1 recovery descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-archive-contracts:riverhog-v1-recovery-descriptor:29e07b6fb5 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-archive-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-recovery-descriptor-v1.schema.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json` — `packages/riverhog-archive-contracts/schemas/riverhog-recovery-descriptor-v1.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json
- `title`: Riverhog v1 recovery descriptor
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `encryption` | yes | object |  |
| `root` | yes | object |  |
| `schema` | yes | object (1 fields) |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 70e187d12431366bc3f4ef919e9afca355e3ec4ca9ba8a7cb1a98a393ed034fb -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "encryption": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "age-v1-scrypt"
        },
        "passphrase_id": {
          "pattern": "^[A-Za-z0-9_-]{16,128}$",
          "type": "string"
        }
      },
      "required": [
        "format",
        "passphrase_id"
      ],
      "type": "object"
    },
    "root": {
      "additionalProperties": false,
      "properties": {
        "path": {
          "const": "manifest.json.age"
        },
        "stored_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "stored_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "path",
        "stored_bytes",
        "stored_sha256"
      ],
      "type": "object"
    },
    "schema": {
      "const": "riverhog-recovery-descriptor/v1"
    }
  },
  "required": [
    "schema",
    "encryption",
    "root"
  ],
  "title": "Riverhog v1 recovery descriptor",
  "type": "object"
}
```

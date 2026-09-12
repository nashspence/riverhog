# Riverhog collection description document v1

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-protocol:riverhog-collection-description-document-v1:6266f2d39a -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-protocol` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-collection-description-v1.schema.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json` — `packages/riverhog-protocol/schemas/riverhog-collection-description-v1.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| encoded-size | bytes | `contract_max` | maximum=32768, reason=bounded-human-authored-catalog-description |
| length | characters | `contract_max` | maximum=32768, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=9007199254740991, minimum=1, reason=schema-maximum |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json
- `title`: Riverhog collection description document v1
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_root_sha256` | yes | string |  |
| `description` | yes | object (1 fields) |  |
| `description_identity` | yes | string |  |
| `format` | yes | object (1 fields) |  |
| `revision` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc0c384f58432075d0c9df58afd0af1c50307749fa4873380bb8bb093c2ad9c8 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "archive_root_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    },
    "description": {
      "oneOf": [
        {
          "maxLength": 32768,
          "minLength": 1,
          "type": "string",
          "x-riverhog-encoded-bytes-max": 32768,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-human-authored-catalog-description"
          },
          "x-unicode-normalization": "NFC"
        },
        {
          "type": "null"
        }
      ]
    },
    "description_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    },
    "format": {
      "const": "riverhog-collection-description/v1"
    },
    "revision": {
      "maximum": 9007199254740991,
      "minimum": 1,
      "type": "integer",
      "x-riverhog-extent": {
        "policy": "fixed",
        "reason": "exact-json-safe-monotonic-description-revision"
      }
    }
  },
  "required": [
    "archive_root_sha256",
    "description",
    "description_identity",
    "format",
    "revision"
  ],
  "title": "Riverhog collection description document v1",
  "type": "object"
}
```

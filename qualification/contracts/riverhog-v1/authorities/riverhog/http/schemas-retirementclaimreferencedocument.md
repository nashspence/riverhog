# schemas: RetirementClaimReferenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retirementclaimreferencedocument:83af135e41 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetirementClaimReferenceDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: RetirementClaimReferenceDocument
- `description`: Exact claim evidence authorizing one retirement deletion plan.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `claim_id` | yes | string |  |
| `execution_id` | no | object (2 fields) |  |
| `fence` | yes | integer |  |
| `outcomes` | no | object (1 fields) |  |
| `output_collection_id` | no | object (1 fields) |  |
| `work_id` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7bf2593b59150209bcbb33413d688188b7cde9cae8da595c8bbd627946da79b -->

```json
{
  "additionalProperties": false,
  "description": "Exact claim evidence authorizing one retirement deletion plan.",
  "oneOf": [
    {
      "properties": {
        "execution_id": {
          "type": "string"
        },
        "outcomes": {
          "type": "null"
        },
        "output_collection_id": {
          "type": "integer"
        }
      },
      "required": [
        "execution_id",
        "output_collection_id"
      ]
    },
    {
      "properties": {
        "execution_id": {
          "type": "null"
        },
        "outcomes": {
          "type": "object"
        },
        "output_collection_id": {
          "type": "null"
        }
      },
      "required": [
        "outcomes"
      ]
    }
  ],
  "properties": {
    "claim_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Claim Id",
      "type": "string"
    },
    "execution_id": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Execution Id"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "outcomes": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ExactSetAuthorityDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "output_collection_id": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionId"
        },
        {
          "type": "null"
        }
      ]
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    }
  },
  "required": [
    "claim_id",
    "fence",
    "work_id"
  ],
  "title": "RetirementClaimReferenceDocument",
  "type": "object"
}
```
